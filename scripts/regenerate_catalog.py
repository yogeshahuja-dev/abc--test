"""Regenerate catalog-info.yaml from existing auto-discovered files."""

import os
import yaml
from datetime import datetime

OUTPUT_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AUTO_DIR = os.path.join(OUTPUT_BASE, "auto-discovered")
TABLES_DIR = os.path.join(AUTO_DIR, "tables")
CATALOG_FILE = os.path.join(AUTO_DIR, "catalog-info.yaml")

print("Scanning auto-discovered files...")

all_files = []
for root, dirs, files in os.walk(AUTO_DIR):
    for f in files:
        if f.endswith(".yaml") and f != "catalog-info.yaml":
            fpath = os.path.join(root, f)
            rel = "./" + os.path.relpath(fpath, OUTPUT_BASE).replace("\\", "/")
            all_files.append(rel)

# Sort by layer
bronze = sorted([f for f in all_files if "/bronze/" in f])
silver_c = sorted([f for f in all_files if "/silver_cleansed/" in f])
silver = sorted([f for f in all_files if "/silver/" in f and "/silver_cleansed/" not in f])
gold = sorted([f for f in all_files if "/gold/" in f])
other = sorted([f for f in all_files if "/other/" in f])

print(f"  Bronze:          {len(bronze)}")
print(f"  Silver Cleansed: {len(silver_c)}")
print(f"  Silver:          {len(silver)}")
print(f"  Gold:            {len(gold)}")
print(f"  Other:           {len(other)}")
print(f"  TOTAL:           {len(all_files)}")

catalog = {
    "apiVersion": "backstage.io/v1alpha1",
    "kind": "Location",
    "metadata": {
        "name": "edl-auto-discovered",
        "description": (
            f"Auto-discovered from Databricks Unity Catalog.\n"
            f"Catalog: adani_edl_governance_catalog_dev\n"
            f"Sync: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            f"Bronze: {len(bronze)}, Silver Cleansed: {len(silver_c)}, "
            f"Silver: {len(silver)}, Gold: {len(gold)}, Other: {len(other)}"
        ),
    },
    "spec": {
        "targets": bronze + silver_c + silver + gold + other,
    },
}

with open(CATALOG_FILE, "w", encoding="utf-8") as f:
    yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

print(f"\nGenerated: {CATALOG_FILE}")
print("Done!")