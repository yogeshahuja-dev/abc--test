"""
Test SPN access to specific catalog: adani_edl_governance_catalog_dev
"""

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

HOST = "https://adb-2478587080594690.10.azuredatabricks.net"
CLIENT_ID = "0c30a662-4213-4184-b4e0-55cd298a1c4d"
CLIENT_SECRET = "dosea8a9acbae130f2ab7ae6b35bf20db634"
PROXY = "http://cloudproxy.adani.com:8080"
PROXIES = {"http": PROXY, "https": PROXY}
CATALOG = "adani_edl_governance_catalog_dev"

print("=" * 60)
print(f"TESTING SPN ACCESS TO: {CATALOG}")
print("=" * 60)

# Step 1: Authenticate
print("\n1. Authenticating SPN...")
TOKEN = None

for scope in ["all-apis", None]:
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    if scope:
        payload["scope"] = scope

    r = requests.post(
        f"{HOST}/oidc/v1/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        proxies=PROXIES, verify=False, timeout=30
    )

    if r.status_code == 200:
        TOKEN = r.json()["access_token"]
        print(f"   AUTH SUCCESS (scope: {scope or 'none'})")
        break
    else:
        if scope:
            continue
        print(f"   AUTH FAILED: {r.status_code} - {r.text}")
        exit(1)

headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Step 2: Try listing ALL catalogs (may fail — that's OK)
print("\n2. Testing list ALL catalogs...")
r = requests.get(
    f"{HOST}/api/2.1/unity-catalog/catalogs",
    headers=headers, proxies=PROXIES, verify=False, timeout=15
)
if r.status_code == 200:
    cats = [c["name"] for c in r.json().get("catalogs", [])]
    print(f"   OK - Can list catalogs: {cats}")
elif r.status_code == 403:
    print(f"   FORBIDDEN - Cannot list ALL catalogs (this is expected)")
    print(f"   Will try accessing specific catalog directly...")
else:
    print(f"   Status: {r.status_code}")

# Step 3: Try accessing specific catalog directly
print(f"\n3. Testing direct access to catalog: {CATALOG}...")
r = requests.get(
    f"{HOST}/api/2.1/unity-catalog/catalogs/{CATALOG}",
    headers=headers, proxies=PROXIES, verify=False, timeout=15
)
if r.status_code == 200:
    data = r.json()
    print(f"   OK - Catalog found: {data.get('name', 'unknown')}")
    print(f"   Comment: {data.get('comment', 'none')}")
elif r.status_code == 403:
    print(f"   FORBIDDEN - Cannot access catalog directly")
else:
    print(f"   Status: {r.status_code} - {r.text[:200]}")

# Step 4: List schemas in the specific catalog
print(f"\n4. Listing schemas in {CATALOG}...")
r = requests.get(
    f"{HOST}/api/2.1/unity-catalog/schemas?catalog_name={CATALOG}",
    headers=headers, proxies=PROXIES, verify=False, timeout=15
)
if r.status_code == 200:
    schemas = r.json().get("schemas", [])
    schema_names = [s["name"] for s in schemas]
    print(f"   FOUND {len(schema_names)} schemas:")
    for s in schema_names:
        print(f"     - {CATALOG}.{s}")
elif r.status_code == 403:
    print(f"   FORBIDDEN - Cannot list schemas")
    print(f"   SPN may need USE SCHEMA permission")
else:
    print(f"   Status: {r.status_code} - {r.text[:200]}")

# Step 5: List tables in each schema
if r.status_code == 200 and schemas:
    print(f"\n5. Listing tables in each schema...")
    total_tables = 0
    schema_table_count = {}

    for schema in schemas:
        sname = schema["name"]
        if sname in ["information_schema", "default", "__databricks_internal"]:
            continue

        r2 = requests.get(
            f"{HOST}/api/2.1/unity-catalog/tables?catalog_name={CATALOG}&schema_name={sname}",
            headers=headers, proxies=PROXIES, verify=False, timeout=15
        )
        if r2.status_code == 200:
            tables = r2.json().get("tables", [])
            total_tables += len(tables)
            schema_table_count[sname] = len(tables)
            sample = [t["name"] for t in tables[:5]]
            more = f" ... +{len(tables)-5} more" if len(tables) > 5 else ""
            print(f"   {sname}: {len(tables)} tables  {sample}{more}")
        elif r2.status_code == 403:
            print(f"   {sname}: FORBIDDEN")
            schema_table_count[sname] = "FORBIDDEN"
        else:
            print(f"   {sname}: FAILED ({r2.status_code})")
            schema_table_count[sname] = f"FAILED ({r2.status_code})"

    # Step 6: Test lineage
    print(f"\n6. Testing lineage API...")
    # Pick first table from first accessible schema
    test_table = None
    for schema in schemas:
        sname = schema["name"]
        if sname in ["information_schema", "default", "__databricks_internal"]:
            continue
        r3 = requests.get(
            f"{HOST}/api/2.1/unity-catalog/tables?catalog_name={CATALOG}&schema_name={sname}",
            headers=headers, proxies=PROXIES, verify=False, timeout=15
        )
        if r3.status_code == 200:
            tbls = r3.json().get("tables", [])
            if tbls:
                test_table = f"{CATALOG}.{sname}.{tbls[0]['name']}"
                break

    if test_table:
        print(f"   Testing lineage for: {test_table}")
        r4 = requests.post(
            f"{HOST}/api/2.1/unity-catalog/lineage/table-lineage",
            headers=headers, proxies=PROXIES, verify=False, timeout=15,
            json={"table_name": test_table, "include_entity_lineage": True}
        )
        if r4.status_code == 200:
            lineage = r4.json()
            ups = lineage.get("upstreams", [])
            downs = lineage.get("downstreams", [])
            print(f"   Lineage OK! Upstreams: {len(ups)}, Downstreams: {len(downs)}")
            if ups:
                for u in ups[:3]:
                    tinfo = u.get("tableInfo", {})
                    print(f"     Upstream: {tinfo.get('name', 'unknown')}")
        elif r4.status_code == 403:
            print(f"   Lineage: FORBIDDEN")
        else:
            print(f"   Lineage: {r4.status_code} - {r4.text[:200]}")
    else:
        print(f"   No tables found to test lineage")

    # Summary
    print(f"\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Catalog:  {CATALOG}")
    print(f"  Schemas:  {len([s for s in schema_names if s not in ['information_schema', 'default', '__databricks_internal']])}")
    print(f"  Tables:   {total_tables}")
    print(f"\n  Schema breakdown:")
    for sname, count in schema_table_count.items():
        print(f"    {sname}: {count}")
    print(f"\n  These can be auto-discovered into Backstage!")

else:
    print(f"\n" + "=" * 60)
    print("CANNOT PROCEED")
    print("=" * 60)
    print(f"  SPN cannot list schemas in {CATALOG}")
    print(f"  Ask admin to grant: GRANT USE SCHEMA ON CATALOG `{CATALOG}` TO `{CLIENT_ID}`")