"""
Databricks Auto-Discovery for Backstage
=========================================
Uses SQL Statement Execution REST API.
Authentication: Databricks-native SPN (OAuth2 M2M).
Connection: DIRECT (no proxy).
"""

import os
import sys
import time
import yaml
import requests
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Remove any proxy env vars to ensure direct connection
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(key, None)


# ============================================================
# CONFIGURATION
# ============================================================

DATABRICKS_HOST = "https://adb-2478587080594690.10.azuredatabricks.net"
CLIENT_ID = "0c30a662-4213-4184-b4e0-55cd298a1c4d"
CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "dosea8a9acbae130f2ab7ae6b35bf20db634")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "9a99a601e597a874")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "adani_edl_governance_catalog_dev")

SCHEMAS_TO_SKIP = ["information_schema", "default", "__databricks_internal"]

OUTPUT_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTO_DIR = os.path.join(OUTPUT_BASE, "auto-discovered")
TABLES_DIR = os.path.join(AUTO_DIR, "tables")
CATALOG_FILE = os.path.join(AUTO_DIR, "catalog-info.yaml")

OWNER = "edl-governance"
SYSTEM = "enterprise-data-lake"


# ============================================================
# AUTHENTICATION
# ============================================================

TOKEN = None

def get_token():
    """Get OAuth token from Databricks OIDC."""
    global TOKEN

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "all-apis",
    }

    r = requests.post(
        f"{DATABRICKS_HOST}/oidc/v1/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
        timeout=30,
    )

    if r.status_code == 200:
        TOKEN = r.json()["access_token"]
        return True
    else:
        print(f"  AUTH FAILED: {r.status_code} - {r.text}")
        return False


# ============================================================
# SQL EXECUTION VIA REST API
# ============================================================

def run_sql(sql, timeout_seconds=50):
    """Execute SQL using Databricks SQL Statement Execution API."""
    global TOKEN

    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"

    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }

    r = requests.post(url, json=payload, headers=headers, verify=False, timeout=60)
    
    if r.status_code != 200:
        print(f"    SQL API error ({r.status_code}): {r.text[:200]}")
        return None

    result = r.json()
    status = result.get("status", {}).get("state", "")

    # Handle async execution
    if status == "PENDING" or status == "RUNNING":
        statement_id = result.get("statement_id", "")
        for _ in range(timeout_seconds):
            time.sleep(1)
            poll_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
            pr = requests.get(poll_url, headers=headers, verify=False, timeout=30)
            if pr.status_code == 200:
                result = pr.json()
                status = result.get("status", {}).get("state", "")
                if status == "SUCCEEDED":
                    break
                elif status == "FAILED":
                    error = result.get("status", {}).get("error", {})
                    print(f"    SQL FAILED: {error.get('message', 'unknown')}")
                    return None
            else:
                break

    if status == "SUCCEEDED":
        manifest = result.get("manifest", {})
        columns = [col["name"] for col in manifest.get("schema", {}).get("columns", [])]
        data_array = result.get("result", {}).get("data_array", [])
        return {"columns": columns, "rows": data_array}

    elif status == "FAILED":
        error = result.get("status", {}).get("error", {})
        print(f"    SQL FAILED: {error.get('message', 'unknown error')}")
        return None
    else:
        print(f"    Unexpected status: {status}")
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
    if "bronze" in s: return "bronze"
    elif "silver_sap_cleansed" in s or "silver_cleansed" in s: return "silver-cleansed"
    elif "silver" in s: return "silver"
    elif "gold" in s: return "gold"
    return "other"


def get_tags(schema):
    layer = detect_layer(schema)
    tags = ["databricks", "auto-discovered", "unity-catalog"]
    if layer != "other": tags.append(layer)
    s = schema.lower()
    for kw in ["sap", "aem", "p11", "mul", "procurement"]:
        if kw in s: tags.append(kw)
    return tags


def save(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
    rel = os.path.relpath(filepath, OUTPUT_BASE).replace("\\", "/")
    print(f"    + {rel}")


# ============================================================
# DISCOVERY
# ============================================================

def discover_schemas():
    """Get all schemas in the catalog."""
    print(f"\n  Running: SHOW SCHEMAS IN `{CATALOG}`")
    result = run_sql(f"SHOW SCHEMAS IN `{CATALOG}`")

    if not result:
        return []

    schemas = []
    for row in result["rows"]:
        sname = row[0] if row else ""
        if sname and sname not in SCHEMAS_TO_SKIP:
            schemas.append(sname)

    print(f"  Found {len(schemas)} schemas:")
    for s in schemas:
        print(f"    - {s}")

    return schemas


def discover_tables(schemas):
    """Discover all tables in all schemas."""
    print("\n" + "=" * 60)
    print("DISCOVERING TABLES")
    print("=" * 60)

    all_files = []
    total = 0

    for sname in schemas:
        print(f"\n  Schema: {CATALOG}.{sname}")

        result = run_sql(f"SHOW TABLES IN `{CATALOG}`.`{sname}`")
        if not result:
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
                desc_result = run_sql(f"DESCRIBE TABLE EXTENDED `{CATALOG}`.`{sname}`.`{tname}`")
                if desc_result:
                    for row in desc_result["rows"]:
                        if len(row) >= 2:
                            field = str(row[0]).strip()
                            value = str(row[1]).strip()
                            if field.lower() == "comment" and value and value.lower() not in ("", "none", "null"):
                                comment = value
                                break
            except:
                pass

            # Get columns
            columns = []
            try:
                col_result = run_sql(f"DESCRIBE TABLE `{CATALOG}`.`{sname}`.`{tname}`")
                if col_result:
                    for row in col_result["rows"]:
                        if len(row) >= 2:
                            col_name = str(row[0]).strip()
                            col_type = str(row[1]).strip()
                            col_comment = str(row[2]).strip() if len(row) > 2 and row[2] else None

                            if col_name.startswith("#") or col_name == "":
                                break
                            if col_type == "":
                                continue
                            if col_comment and col_comment.lower() in ("", "none", "null"):
                                col_comment = None

                            columns.append({"name": col_name, "type": col_type, "comment": col_comment})
            except:
                pass

            # Build description
            desc_parts = [f"Auto-discovered from Databricks Unity Catalog.", f"Full name: {full_name}"]
            if comment:
                desc_parts.append(f"Description: {comment}")
            if columns:
                desc_parts.append(f"Columns ({len(columns)}):")
                for col in columns[:15]:
                    line = f"  - {col['name']} ({col['type']})"
                    if col.get("comment"):
                        line += f" -- {col['comment']}"
                    desc_parts.append(line)
                if len(columns) > 15:
                    desc_parts.append(f"  ... +{len(columns)-15} more")
            desc_parts.append(f"Discovered: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            subfolder = {"bronze": "bronze", "silver-cleansed": "silver_cleansed",
                         "silver": "silver", "gold": "gold"}.get(layer, "other")

            fpath = os.path.join(TABLES_DIR, subfolder, f"{ename}.yaml")

            entity = {
                "apiVersion": "backstage.io/v1alpha1",
                "kind": "Component",
                "metadata": {
                    "name": ename,
                    "title": f"{sname}.{tname}",
                    "description": "\n".join(desc_parts),
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

    print(f"\n  Total tables: {total}")
    return all_files


def apply_lineage(all_files):
    """Apply lineage based on schema naming patterns."""
    print("\n" + "=" * 60)
    print("APPLYING LINEAGE")
    print("=" * 60)

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

    by_layer = {}
    for full_name, info in mapping.items():
        layer = info["layer"]
        if layer not in by_layer:
            by_layer[layer] = {}
        by_layer[layer][full_name] = info

    print(f"  Tables by layer:")
    for layer, tables in by_layer.items():
        print(f"    {layer}: {len(tables)}")

    count = 0

    for full_name, info in mapping.items():
        layer = info["layer"]
        table = info["table"].lower()
        upstreams = []

        if layer == "gold":
            for s_full, s_info in by_layer.get("silver", {}).items():
                s_table = s_info["table"].lower()
                t_words = set(table.replace("_", " ").split())
                s_words = set(s_table.replace("_", " ").split())
                if t_words & s_words:
                    upstreams.append(f"component:{s_info['entity_name']}")

        elif layer == "silver":
            for sc_full, sc_info in by_layer.get("silver-cleansed", {}).items():
                sc_table = sc_info["table"].lower()
                t_base = table.replace("_", "")
                sc_base = sc_table.replace("_mstr", "").replace("_master", "").replace("_", "")
                if t_base == sc_base or table in sc_table or sc_table.replace("_mstr", "") in table:
                    upstreams.append(f"component:{sc_info['entity_name']}")

        elif layer == "silver-cleansed":
            for b_full, b_info in by_layer.get("bronze", {}).items():
                b_table = b_info["table"].lower()
                t_base = table.replace("_mstr", "").replace("_master", "")
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

    print(f"\n  Lineage relationships added: {count}")


def generate_catalog(all_files):
    """Generate master catalog-info.yaml."""
    print("\n" + "=" * 60)
    print("GENERATING CATALOG-INFO.YAML")
    print("=" * 60)

    bronze = sorted([f for f in all_files if "/bronze/" in f])
    silver_c = sorted([f for f in all_files if "/silver_cleansed/" in f])
    silver = sorted([f for f in all_files if "/silver/" in f and "/silver_cleansed/" not in f])
    gold = sorted([f for f in all_files if "/gold/" in f])
    other = sorted([f for f in all_files if "/other/" in f])

    catalog = {
        "apiVersion": "backstage.io/v1alpha1",
        "kind": "Location",
        "metadata": {
            "name": "edl-auto-discovered",
            "description": (
                f"Auto-discovered from Databricks Unity Catalog.\n"
                f"Catalog: {CATALOG}\n"
                f"Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"Bronze: {len(bronze)}, Silver Cleansed: {len(silver_c)}, "
                f"Silver: {len(silver)}, Gold: {len(gold)}, Other: {len(other)}"
            ),
        },
        "spec": {
            "targets": bronze + silver_c + silver + gold + other,
        },
    }

    save(CATALOG_FILE, catalog)

    print(f"  Bronze:          {len(bronze)}")
    print(f"  Silver Cleansed: {len(silver_c)}")
    print(f"  Silver:          {len(silver)}")
    print(f"  Gold:            {len(gold)}")
    print(f"  Other:           {len(other)}")
    print(f"  TOTAL:           {len(all_files)}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("DATABRICKS AUTO-DISCOVERY FOR BACKSTAGE")
    print(f"Workspace:  {DATABRICKS_HOST}")
    print(f"Catalog:    {CATALOG}")
    print(f"Warehouse:  {WAREHOUSE_ID}")
    print(f"Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Authenticate
    print("\n1. Authenticating SPN...")
    if not get_token():
        print("\nFATAL: Authentication failed!")
        sys.exit(1)
    print("   OK!")

    # Test SQL execution
    print("\n2. Testing SQL execution...")
    result = run_sql("SELECT 1 AS test")
    if not result:
        print("\nFATAL: SQL execution failed!")
        print("\nTroubleshooting:")
        print(f"  Warehouse ID: {WAREHOUSE_ID}")
        print("  1. Is the SQL Warehouse RUNNING? (not stopped/paused)")
        print("  2. Does the SPN have access to this warehouse?")
        print("     Go to SQL Warehouse > Permissions > Add SPN")
        print("  3. Is your IP whitelisted in workspace IP Access Lists?")
        sys.exit(1)
    print("   SQL execution works!")

    # Test catalog access
    print(f"\n3. Testing access to catalog: {CATALOG}...")
    result = run_sql(f"SHOW SCHEMAS IN `{CATALOG}`")
    if not result:
        print(f"\nFATAL: Cannot access catalog {CATALOG}!")
        print("  SPN may need USE CATALOG / USE SCHEMA permissions")
        sys.exit(1)
    print("   Catalog access works!")

    # Create directories
    os.makedirs(TABLES_DIR, exist_ok=True)

    # Discover schemas
    print("\n4. Discovering schemas...")
    schemas = discover_schemas()
    if not schemas:
        print("\nNo schemas found!")
        sys.exit(1)

    # Discover tables
    print("\n5. Discovering tables...")
    all_files = discover_tables(schemas)
    if not all_files:
        print("\nNo tables found!")
        sys.exit(1)

    # Apply lineage
    print("\n6. Applying lineage...")
    apply_lineage(all_files)

    # Generate catalog
    print("\n7. Generating catalog...")
    generate_catalog(all_files)

    # Summary
    print("\n" + "=" * 60)
    print("DISCOVERY COMPLETE!")
    print("=" * 60)
    print(f"  Catalog:  {CATALOG}")
    print(f"  Tables:   {len(all_files)}")
    print(f"  Output:   {AUTO_DIR}")
    print(f"\nNext steps:")
    print(f"  1. Review: dir auto-discovered\\tables")
    print(f"  2. Push to git:")
    print(f"     git add .")
    print(f'     git commit -m "Auto-discovery sync from Databricks"')
    print(f"     git push origin main")
    print(f"  3. Register in Backstage:")
    print(f"     Create > Register Existing Component")
    print(f"     URL: https://github.com/YOUR_USER/YOUR_REPO/blob/main/auto-discovered/catalog-info.yaml")


if __name__ == "__main__":
    main()