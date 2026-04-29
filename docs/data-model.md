# Data Model

## Bronze Tables

| Schema.Table | Source System | Description | Refresh |
|-------------|-------------|-------------|---------|
| bronze_sap_aem.t024_slt | SAP AEM | Purchasing Groups (raw) | Real-time SLT |
| bronze_sap_p11.t024_slt | SAP P11 | Purchasing Groups (raw) | Real-time SLT |
| bronze_sap_mul.t024_slt | SAP MUL | Purchasing Groups (raw) | Real-time SLT |
| bronze_sap_aem.lfa1_slt | SAP AEM | Vendor Master (raw) | Real-time SLT |
| bronze_sap_p11.lfa1_slt | SAP P11 | Vendor Master (raw) | Real-time SLT |
| bronze_sap_mul.lfa1_slt | SAP MUL | Vendor Master (raw) | Real-time SLT |

## Silver Cleansed Tables

| Schema.Table | Source Tables | Transformations |
|-------------|--------------|-----------------|
| silver_sap_cleansed.t024_mstr | AEM + P11 + MUL t024_slt | Merge, dedup, null handling, type cast |
| silver_sap_cleansed.lfa1_mstr | AEM + P11 + MUL lfa1_slt | Merge, dedup, null handling, type cast |

## Silver Tables (Business Entities)

| Schema.Table | Source Table | Description |
|-------------|-------------|-------------|
| silver.purchasing_group | silver_sap_cleansed.t024_mstr | Standardized purchasing group codes and descriptions |
| silver.vendor | silver_sap_cleansed.lfa1_mstr | Unified vendor records across SAP systems |
| silver.material | Cleansed material tables | Unified material catalog |

## Gold Tables (Curated)

| Schema.Table | Source Tables | Consumers | Description |
|-------------|-------------|-----------|-------------|
| gold_procurement.po_overview | silver.purchasing_group | Dashboard, Data Product, ML | Purchase Order aggregations and KPIs |
| gold_procurement.pr_overview | silver.vendor, silver.material | Dashboard, Data Product, ML | Purchase Requisition aggregations and KPIs |

## Key Columns

### gold_procurement.po_overview

| Column | Type | Description |
|--------|------|-------------|
| po_number | STRING | Purchase Order number |
| purchasing_group | STRING | Purchasing group code |
| purchasing_group_desc | STRING | Purchasing group description |
| vendor_id | STRING | Vendor identifier |
| po_amount | DECIMAL | Total PO amount |
| po_date | DATE | PO creation date |
| delivery_date | DATE | Expected delivery date |
| status | STRING | PO status (Open/Closed/Partial) |
| source_system | STRING | Originating SAP system |

### gold_procurement.pr_overview

| Column | Type | Description |
|--------|------|-------------|
| pr_number | STRING | Purchase Requisition number |
| vendor_id | STRING | Vendor identifier |
| vendor_name | STRING | Vendor name |
| material_id | STRING | Material identifier |
| material_desc | STRING | Material description |
| pr_amount | DECIMAL | Total PR amount |
| pr_date | DATE | PR creation date |
| status | STRING | PR status (Open/Approved/Rejected) |
| source_system | STRING | Originating SAP system |