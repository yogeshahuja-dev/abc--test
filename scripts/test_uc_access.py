"""
Test: Can our SPN access Unity Catalog?
"""

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Config
HOST = "https://adb-2478587080594690.10.azuredatabricks.net"
CLIENT_ID = "0c30a662-4213-4184-b4e0-55cd298a1c4d"
CLIENT_SECRET = "dosea8a9acbae130f2ab7ae6b35bf20db634"
PROXY = "http://cloudproxy.adani.com:8080"
PROXIES = {"http": PROXY, "https": PROXY}

print("=" * 60)
print("TESTING SPN ACCESS TO DATABRICKS")
print("=" * 60)

# Step 1: Get token
print("\n1. Authenticating...")
token_url = f"{HOST}/oidc/v1/token"

for scope in ["all-apis", None]:
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    if scope:
        payload["scope"] = scope

    r = requests.post(token_url, data=payload,
                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                      proxies=PROXIES, verify=False, timeout=30)

    if r.status_code == 200:
        TOKEN = r.json()["access_token"]
        print(f"   AUTH SUCCESS (scope: {scope or 'none'})")
        break
    else:
        if scope:
            print(f"   Failed with scope '{scope}', trying without...")
            continue
        print(f"   AUTH FAILED: {r.status_code} - {r.text}")
        exit(1)

headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

# Step 2: Test each API endpoint
print("\n2. Testing API endpoints...\n")

tests = [
    ("Unity Catalog - List Catalogs",    "GET",  "/api/2.1/unity-catalog/catalogs"),
    ("Workspace - List Clusters",        "GET",  "/api/2.0/clusters/list"),
    ("Workspace - List Jobs",            "GET",  "/api/2.0/jobs/list?limit=5"),
    ("Workspace - SQL Dashboards",       "GET",  "/api/2.0/sql/dashboards"),
    ("Workspace - ML Endpoints",         "GET",  "/api/2.0/serving-endpoints"),
    ("Workspace - Current User",         "GET",  "/api/2.0/preview/scim/v2/Me"),
]

results = {}

for name, method, endpoint in tests:
    url = f"{HOST}{endpoint}"
    try:
        r = requests.get(url, headers=headers, proxies=PROXIES, verify=False, timeout=15)
        status = r.status_code

        if status == 200:
            data = r.json()
            # Count items
            if "catalogs" in data:
                count = len(data["catalogs"])
                detail = f"Found {count} catalogs"
            elif "jobs" in data:
                count = len(data["jobs"])
                detail = f"Found {count} jobs"
            elif "results" in data:
                count = len(data["results"])
                detail = f"Found {count} items"
            elif "endpoints" in data:
                count = len(data["endpoints"])
                detail = f"Found {count} endpoints"
            elif "clusters" in data:
                count = len(data["clusters"])
                detail = f"Found {count} clusters"
            else:
                detail = "OK"
            print(f"   OK  {name}")
            print(f"       {detail}")
            results[name] = "OK"
        elif status == 403:
            print(f"   FORBIDDEN  {name}")
            print(f"       SPN does not have permission")
            results[name] = "FORBIDDEN"
        elif status == 401:
            print(f"   UNAUTHORIZED  {name}")
            results[name] = "UNAUTHORIZED"
        else:
            print(f"   FAILED ({status})  {name}")
            print(f"       {r.text[:100]}")
            results[name] = f"FAILED ({status})"
    except Exception as e:
        print(f"   ERROR  {name}")
        print(f"       {str(e)[:100]}")
        results[name] = "ERROR"

# Step 3: If catalogs work, test schemas and tables
if results.get("Unity Catalog - List Catalogs") == "OK":
    print("\n3. Testing Unity Catalog deeper...\n")

    # Get catalog names
    r = requests.get(f"{HOST}/api/2.1/unity-catalog/catalogs",
                     headers=headers, proxies=PROXIES, verify=False, timeout=15)
    catalogs = r.json().get("catalogs", [])
    cat_names = [c["name"] for c in catalogs]
    print(f"   Catalogs: {cat_names}")

    # Test first catalog's schemas
    for cat in cat_names[:3]:  # Test first 3 catalogs
        r = requests.get(f"{HOST}/api/2.1/unity-catalog/schemas?catalog_name={cat}",
                         headers=headers, proxies=PROXIES, verify=False, timeout=15)
        if r.status_code == 200:
            schemas = r.json().get("schemas", [])
            schema_names = [s["name"] for s in schemas]
            print(f"   {cat} schemas: {schema_names[:10]}{'...' if len(schema_names) > 10 else ''}")

            # Test first schema's tables
            for schema in schemas[:2]:  # Test first 2 schemas
                sname = schema["name"]
                if sname in ["information_schema", "default"]:
                    continue
                r2 = requests.get(
                    f"{HOST}/api/2.1/unity-catalog/tables?catalog_name={cat}&schema_name={sname}",
                    headers=headers, proxies=PROXIES, verify=False, timeout=15)
                if r2.status_code == 200:
                    tables = r2.json().get("tables", [])
                    table_names = [t["name"] for t in tables[:5]]
                    print(f"   {cat}.{sname} tables ({len(tables)} total): {table_names}{'...' if len(tables) > 5 else ''}")
                elif r2.status_code == 403:
                    print(f"   {cat}.{sname} tables: FORBIDDEN")
                else:
                    print(f"   {cat}.{sname} tables: FAILED ({r2.status_code})")
                break  # Only test first valid schema
        elif r.status_code == 403:
            print(f"   {cat} schemas: FORBIDDEN")
        else:
            print(f"   {cat} schemas: FAILED ({r.status_code})")

    # Test lineage API
    print("\n4. Testing Lineage API...\n")
    r = requests.get(f"{HOST}/api/2.1/unity-catalog/lineage/table-lineage",
                     headers=headers, proxies=PROXIES, verify=False, timeout=15)
    if r.status_code == 405 or r.status_code == 400:
        print("   Lineage API exists (needs POST with table name)")
    elif r.status_code == 403:
        print("   Lineage API: FORBIDDEN")
    elif r.status_code == 200:
        print("   Lineage API: OK")
    else:
        print(f"   Lineage API: {r.status_code}")

# Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
for name, result in results.items():
    icon = "OK" if result == "OK" else "X "
    print(f"  [{icon}] {name}: {result}")

print("\n" + "=" * 60)
print("RECOMMENDATION")
print("=" * 60)

uc_ok = results.get("Unity Catalog - List Catalogs") == "OK"
ws_ok = results.get("Workspace - List Jobs") == "OK"

if uc_ok and ws_ok:
    print("  FULL ACCESS - SPN can discover everything!")
    print("  Run: python scripts/databricks_discovery.py")
elif uc_ok and not ws_ok:
    print("  PARTIAL ACCESS - SPN can access Unity Catalog but NOT workspace APIs")
    print("  Can discover: Tables, Schemas, Catalogs, Lineage")
    print("  Cannot discover: Jobs, Dashboards, ML Endpoints")
    print("  RECOMMENDATION: Use SPN for tables + PAT for jobs/dashboards")
elif not uc_ok and not ws_ok:
    print("  NO ACCESS - SPN cannot access anything")
    print("  RECOMMENDATION: Use your Personal Access Token instead")
    print("  Generate PAT: Databricks > Profile > Settings > Developer > Access tokens")