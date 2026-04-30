"""
Databricks Focused Discovery for Backstage POC
================================================
Discovers tables from 4 specific schemas only:
  - bronze_sap_mul       (Raw SAP data from MUL system)
  - silver_sap_cleansed  (Cleansed and merged masters)
  - silver               (Business entities)
  - gold_procurement     (Curated procurement tables)

Authentication: Databricks-native SPN (OAuth2 M2M)
Connection: SQL Statement Execution REST API (direct, no proxy)
"""

import os
import sys
import time
import yaml
import requests
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Remove proxy (direct connection)
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(key, None)


# ============================================================
# CONFIGURATION
# ============================================================

DATABRICKS_HOST = "https://adb-2478587080594690.10.azuredatabricks.net"
CLIENT_ID = "0c30a662-4213-4184-b4e0-55cd298a1c4d"
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "dosea8a9acbae130f2ab7ae6b35bf20db634")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "9a99a601e597a874")
CATALOG = "adani_edl_governance_catalog_dev"

SCHEMAS_TO_DISCOVER = [
    "bronze_sap_mul",
    "silver_sap_cleansed",
    "silver",
    "gold_procurement",
]

OUTPUT_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTO_DIR = os.path.join(OUTPUT_BASE, "auto-discovered")
TABLES_DIR = os.path.join(AUTO_DIR, "tables")
CATALOG_FILE = os.path.join(AUTO_DIR, "catalog-info.yaml")

OWNER = "edl-governance"
SYSTEM = "enterprise-data-lake"

TOKEN = None


# ============================================================
# AUTHENTICATION
# ============================================================

def get_token():
    global TOKEN
    r = requests.post(
        f"{DATABRICKS_HOST}/oidc/v1/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "all-apis",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=30,
    )
    if r.status_code == 200:
        TOKEN = r.json()["access_token"]
        return True
    print(f"  AUTH FAILED: {r.status_code} - {r.text}")
    return False


# ============================================================
# SQL EXECUTION
# ============================================================

def run_sql(sql):
    global TOKEN

    r = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        json={
            "warehouse_id": WAREHOUSE_ID,
            "statement": sql,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json",
        },
        verify=False,
        timeout=60,
    )

    if r.status_code != 200:
        print(f"    SQL error ({r.status_code}): {r.text[:200]}")
        return None

    result = r.json()
    status = result.get("status", {}).get("state", "")

    if status in ("PENDING", "RUNNING"):
        sid = result.get("statement_id", "")
        for _ in range(50):
            time.sleep(1)
            pr = requests.get(
                f"{DATABRICKS_HOST}/api/2.0/sql/statements/{sid}",
                headers={"Authorization": f"Bearer {TOKEN}"},
                verify=False,
                timeout=30,
            )
            if pr.status_code == 200:
                result = pr.json()
                status = result.get("status", {}).get("state", "")
                if status == "SUCCEEDED":
                    break
                elif status == "FAILED":
                    err = result.get("status", {}).get("error", {})
                    print(f"    SQL FAILED: {err.get('message', 'unknown')}")
                    return None

    if status == "SUCCEEDED":
        manifest = result.get("manifest", {})
        cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        rows = result.get("result", {}).get("data_array", [])
        return {"columns": cols, "rows": rows}
    elif status == "FAILED":
        err = result.get("status", {}).get("error", {})
        print(f"    SQL FAILED: {err.get('message', 'unknown')}")
        return None
    return None


# ============================================================
# HELPERS
# ============================================================

def sanitize(name):
    r = name.lower().replace("_", "-").replace(".", "-").replace(" ", "-")
    r = "".join(c for c in r if c.isalnum() or c == "-")
    while "--" in r:
        r = r.replace("--", "-")
    return r.strip("-")[:63]


def detect_layer(schema):
    s = schema.lower()
    if "bronze" in s:
        return "bronze"
    elif "silver_sap_cleansed" in s or "silver_cleansed" in s:
        return "silver-cleansed"
    elif "silver" in s:
        return "silver"
    elif "gold" in s:
        return "gold"
    return "other"


def get_tags(schema):
    layer = detect_layer(schema)
    tags = ["databricks", "auto-discovered", "unity-catalog"]
    if layer != "other":
        tags.append(layer)
    s = schema.lower()
    for kw in ["sap", "mul", "procurement"]:
        if kw in s:
            tags.append(kw)
    return tags


def save(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    rel = os.path.relpath(filepath, OUTPUT_BASE).replace("\\", "/")
    print(f"    + {rel}")


# ============================================================
# DISCOVER TABLES
# ============================================================

def discover_tables(schemas):
    print("\n" + "=" * 60)
    print("DISCOVERING TABLES")
    print("=" * 60)

    all_files = []
    total = 0

    for sname in schemas:
        print(f"\n  Schema: {CATALOG}.{sname}")

        result = run_sql(f"SHOW TABLES IN `{CATALOG}`.`{sname}`")
        if not result:
            print(f"    FAILED or empty")
            continue

        tables = []
        for row in result["rows"]:
            if len(row) >= 2:
                tables.append(row[1])
            elif len(row) >= 1:
                tables.append(row[0])

        print(f"    Found {len(tables)} tables")

        for tname in tables:
            full_name = f"{CATALOG}.{sname}.{tname}"
            ename = sanitize(f"{sname}-{tname}")
            layer = detect_layer(sname)
            tags = get_tags(sname)

            # Get table comment
            comment = None
            try:
                dr = run_sql(f"DESCRIBE TABLE EXTENDED `{CATALOG}`.`{sname}`.`{tname}`")
                if dr:
                    for row in dr["rows"]:
                        if len(row) >= 2:
                            f_name = str(row[0]).strip()
                            f_val = str(row[1]).strip()
                            if f_name.lower() == "comment" and f_val and f_val.lower() not in ("", "none", "null"):
                                comment = f_val
                                break
            except:
                pass

            # Get columns
            columns = []
            try:
                cr = run_sql(f"DESCRIBE TABLE `{CATALOG}`.`{sname}`.`{tname}`")
                if cr:
                    for row in cr["rows"]:
                        if len(row) >= 2:
                            cn = str(row[0]).strip()
                            ct = str(row[1]).strip()
                            cc = str(row[2]).strip() if len(row) > 2 and row[2] else None

                            if cn.startswith("#") or cn == "":
                                break
                            if ct == "":
                                continue
                            if cc and cc.lower() in ("", "none", "null"):
                                cc = None

                            columns.append({"name": cn, "type": ct, "comment": cc})
            except:
                pass

            # Build description
            desc = [
                f"Auto-discovered from Databricks Unity Catalog.",
                f"Full name: {full_name}",
                f"Layer: {layer}",
            ]
            if comment:
                desc.append(f"Description: {comment}")
            if columns:
                desc.append(f"Columns ({len(columns)}):")
                for col in columns[:20]:
                    line = f"  - {col['name']} ({col['type']})"
                    if col.get("comment"):
                        line += f" -- {col['comment']}"
                    desc.append(line)
                if len(columns) > 20:
                    desc.append(f"  ... +{len(columns)-20} more")
            desc.append(f"Discovered: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            subfolder = {
                "bronze": "bronze",
                "silver-cleansed": "silver_cleansed",
                "silver": "silver",
                "gold": "gold",
            }.get(layer, "other")

            fpath = os.path.join(TABLES_DIR, subfolder, f"{ename}.yaml")

            entity = {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "Component",
                "metadata": {
                    "name": ename,
                    "title": f"{sname}.{tname}",
                    "description": "\n".join(desc),
                    "tags": tags,
                    "annotations": {
                        "databricks.com/full-table-name": full_name,
                        "databricks.com/catalog": CATALOG,
                        "databricks.com/schema": sname,
                        "databricks.com/table": tname,
                    },
                    "links": [{
                        "url": f"{DATABRICKS_HOST}/explore/data/{CATALOG}/{sname}/{tname}",
                        "title": "View in Databricks",
                        "icon": "dashboard",
                    }],
                },
                "spec": {
                    "type": "data-table",
                    "lifecycle": "production",
                    "owner": OWNER,
                    "system": SYSTEM,
                },
            }

            save(fpath, entity)
            all_files.append(f"./{os.path.relpath(fpath, OUTPUT_BASE).replace(chr(92), '/')}")
            total += 1

    print(f"\n  Total tables discovered: {total}")
    return all_files


# ============================================================
# APPLY LINEAGE
# ============================================================

def apply_lineage():
    print("\n" + "=" * 60)
    print("APPLYING LINEAGE")
    print("=" * 60)

    # Load all entities
    mapping = {}
    for root, dirs, files_list in os.walk(TABLES_DIR):
        for f in files_list:
            if not f.endswith(".yaml"):
                continue
            fpath = os.path.join(root, f)
            with open(fpath, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            if not data or "metadata" not in data:
                continue
            ann = data["metadata"].get("annotations", {})
            full_name = ann.get("databricks.com/full-table-name", "")
            schema = ann.get("databricks.com/schema", "")
            table = ann.get("databricks.com/table", "")
            ename = data["metadata"]["name"]
            if full_name:
                mapping[full_name] = {
                    "entity_name": ename,
                    "filepath": fpath,
                    "schema": schema,
                    "table": table,
                    "layer": detect_layer(schema),
                }

    # Group by layer
    by_layer = {}
    for full_name, info in mapping.items():
        layer = info["layer"]
        if layer not in by_layer:
            by_layer[layer] = {}
        by_layer[layer][full_name] = info

    print(f"  Tables by layer:")
    for layer in ["bronze", "silver-cleansed", "silver", "gold"]:
        count = len(by_layer.get(layer, {}))
        print(f"    {layer}: {count}")

    count = 0

    for full_name, info in mapping.items():
        layer = info["layer"]
        table = info["table"].lower()
        upstreams = []

        if layer == "gold":
            # Gold depends on silver tables
            for s_full, s_info in by_layer.get("silver", {}).items():
                s_table = s_info["table"].lower()
                t_words = set(table.replace("_", " ").split()) - {"slt", "mstr", "master", "raw", "overview"}
                s_words = set(s_table.replace("_", " ").split()) - {"slt", "mstr", "master", "raw"}
                common = t_words & s_words
                if len(common) >= 1:
                    upstreams.append(f"component:{s_info['entity_name']}")

        elif layer == "silver":
            # Silver depends on silver-cleansed
            for sc_full, sc_info in by_layer.get("silver-cleansed", {}).items():
                sc_table = sc_info["table"].lower()
                t_base = table.replace("_mstr", "").replace("_master", "")
                sc_base = sc_table.replace("_mstr", "").replace("_master", "")
                if t_base == sc_base or t_base in sc_table or sc_base in table:
                    upstreams.append(f"component:{sc_info['entity_name']}")

        elif layer == "silver-cleansed":
            # Silver cleansed depends on bronze tables
            for b_full, b_info in by_layer.get("bronze", {}).items():
                b_table = b_info["table"].lower()
                t_base = table.replace("_mstr", "").replace("_master", "").replace("_cleansed", "")
                b_base = b_table.replace("_slt", "").replace("_raw", "").replace("_delta", "")
                if t_base == b_base:
                    upstreams.append(f"component:{b_info['entity_name']}")

        if upstreams:
            fpath = info["filepath"]
            with open(fpath, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            data["spec"]["dependsOn"] = list(set(upstreams))
            save(fpath, data)
            count += 1
            print(f"    {info['entity_name']} <- {list(set(upstreams))[:3]}{'...' if len(upstreams) > 3 else ''}")

    print(f"\n  Lineage relationships added: {count}")


# ============================================================
# GENERATE CATALOG
# ============================================================

def generate_catalog(all_files):
    print("\n" + "=" * 60)
    print("GENERATING CATALOG-INFO.YAML")
    print("=" * 60)

    bronze = sorted([f for f in all_files if "/bronze/" in f])
    silver_c = sorted([f for f in all_files if "/silver_cleansed/" in f])
    silver = sorted([f for f in all_files if "/silver/" in f and "/silver_cleansed/" not in f])
    gold = sorted([f for f in all_files if "/gold/" in f])

    catalog = {
        "apiVersion": "backstage.io/v1alpha1",
        "kind": "Location",
        "metadata": {
            "name": "edl-auto-discovered",
            "description": (
                f"Auto-discovered from Databricks Unity Catalog.\n"
                f"Catalog: {CATALOG}\n"
                f"Schemas: {', '.join(SCHEMAS_TO_DISCOVER)}\n"
                f"Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"Bronze: {len(bronze)}, Silver Cleansed: {len(silver_c)}, "
                f"Silver: {len(silver)}, Gold: {len(gold)}"
            ),
        },
        "spec": {
            "targets": bronze + silver_c + silver + gold,
        },
    }

    save(CATALOG_FILE, catalog)

    print(f"  Bronze:          {len(bronze)}")
    print(f"  Silver Cleansed: {len(silver_c)}")
    print(f"  Silver:          {len(silver)}")
    print(f"  Gold:            {len(gold)}")
    print(f"  TOTAL:           {len(all_files)}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("DATABRICKS FOCUSED DISCOVERY FOR BACKSTAGE POC")
    print("=" * 60)
    print(f"  Workspace:  {DATABRICKS_HOST}")
    print(f"  Catalog:    {CATALOG}")
    print(f"  Warehouse:  {WAREHOUSE_ID}")
    print(f"  Schemas:")
    for s in SCHEMAS_TO_DISCOVER:
        print(f"    - {s}")
    print(f"  Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: Authenticate
    print("\n1. Authenticating SPN...")
    if not get_token():
        print("\nFATAL: Authentication failed!")
        sys.exit(1)
    print("   OK!")

    # Step 2: Test SQL
    print("\n2. Testing SQL execution...")
    result = run_sql("SELECT 1 AS test")
    if not result:
        print("\nFATAL: SQL execution failed!")
        print("  Check: Is SQL Warehouse running?")
        sys.exit(1)
    print("   OK!")

    # Step 3: Verify schemas exist
    print(f"\n3. Verifying schemas...")
    result = run_sql(f"SHOW SCHEMAS IN `{CATALOG}`")
    if not result:
        print(f"\nFATAL: Cannot list schemas in {CATALOG}")
        sys.exit(1)

    available = [row[0] for row in result["rows"]]
    valid_schemas = []
    for s in SCHEMAS_TO_DISCOVER:
        if s in available:
            print(f"   OK      {s}")
            valid_schemas.append(s)
        else:
            print(f"   MISSING {s} (skipping)")

    if not valid_schemas:
        print("\nFATAL: No target schemas found!")
        sys.exit(1)

    # Step 4: Create directories
    os.makedirs(TABLES_DIR, exist_ok=True)

    # Step 5: Discover tables
    print("\n4. Discovering tables...")
    all_files = discover_tables(valid_schemas)
    if not all_files:
        print("\nNo tables found!")
        sys.exit(1)

    # Step 6: Apply lineage
    print("\n5. Applying lineage...")
    apply_lineage()

    # Step 7: Generate catalog
    print("\n6. Generating catalog...")
    generate_catalog(all_files)

    # Summary
    print("\n" + "=" * 60)
    print("DISCOVERY COMPLETE!")
    print("=" * 60)
    print(f"  Catalog:    {CATALOG}")
    print(f"  Schemas:    {len(valid_schemas)}")
    print(f"  Tables:     {len(all_files)}")
    print(f"  Output:     {AUTO_DIR}")
    print(f"\nNext steps:")
    print(f"  1. Review:")
    print(f"     dir auto-discovered\\tables\\bronze")
    print(f"     dir auto-discovered\\tables\\silver_cleansed")
    print(f"     dir auto-discovered\\tables\\silver")
    print(f"     dir auto-discovered\\tables\\gold")
    print(f"  2. Push to git:")
    print(f"     git add .")
    print(f'     git commit -m "Auto-discovery: 4 schemas from Databricks"')
    print(f"     git push origin main")
    print(f"  3. Register in Backstage:")
    print(f"     Create > Register Existing Component")
    print(f"     URL: https://github.com/YOUR_USER/YOUR_REPO/blob/main/auto-discovered/catalog-info.yaml")


if __name__ == "__main__":
    main()