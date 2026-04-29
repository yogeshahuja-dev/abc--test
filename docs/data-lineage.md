# Data Lineage

## Full Lineage: Bronze to Dashboard

This page shows the complete data lineage from raw SAP source tables all the way to the Procurement Cockpit Dashboard.

## Procurement Cockpit Dashboard Lineage

The **Procurement Cockpit Dashboard** is built from two Gold tables:

- `gold_procurement.po_overview` (Purchase Order Overview)
- `gold_procurement.pr_overview` (Purchase Requisition Overview)

### PO Overview Lineage (Purchase Orders)

| Layer | Table | Transformation | Job |
|-------|-------|---------------|-----|
| Bronze | bronze_sap_aem.t024_slt | Raw SLT replication from SAP AEM | SLT |
| Bronze | bronze_sap_p11.t024_slt | Raw SLT replication from SAP P11 | SLT |
| Bronze | bronze_sap_mul.t024_slt | Raw SLT replication from SAP MUL | SLT |
| Silver Cleansed | silver_sap_cleansed.t024_mstr | Merge 3 sources, dedup, null handling | SilverCleansedLoad_BSIK_BSAK |
| Silver | silver.purchasing_group | Business logic, entity resolution | SilverLoad_Procurement |
| Gold | gold_procurement.po_overview | Aggregation, KPI calculation | GoldLoad_Procurement |

### PR Overview Lineage (Purchase Requisitions)
| Layer | Table | Transformation | Job |
|-------|-------|---------------|-----|
| Bronze | bronze_sap_aem.lfa1_slt | Raw SLT replication from SAP AEM | SLT |
| Bronze | bronze_sap_p11.lfa1_slt | Raw SLT replication from SAP P11 | SLT |
| Bronze | bronze_sap_mul.lfa1_slt | Raw SLT replication from SAP MUL | SLT |
| Silver Cleansed | silver_sap_cleansed.lfa1_mstr | Merge 3 sources, dedup, null handling | SilverCleansedLoad_BSIK_BSAK |
| Silver | silver.vendor | Business logic, entity resolution | SilverLoad_Procurement |
| Silver | silver.material | Material master processing | SilverLoad_Procurement |
| Gold | gold_procurement.pr_overview | Aggregation, KPI calculation | GoldLoad_Procurement |

### End-to-End: Dashboard Lineage
## Data Product Lineage: Early Payment Trends

| Source | Table | Path to Data Product |
|--------|-------|---------------------|
| SAP T024 | Purchasing Groups | Bronze → Silver Cleansed → Silver → Gold PO Overview → Data Product |
| SAP LFA1 | Vendor Master | Bronze → Silver Cleansed → Silver → Gold PR Overview → Data Product |

## Impact Analysis

### What happens if bronze_sap_aem.t024_slt fails?

| Impacted Table | Layer | Impact |
|---------------|-------|--------|
| silver_sap_cleansed.t024_mstr | Silver Cleansed | Partial data (P11 + MUL only) |
| silver.purchasing_group | Silver | Missing AEM purchasing groups |
| gold_procurement.po_overview | Gold | Incomplete PO data |
| Procurement Cockpit Dashboard | Consumer | Incomplete dashboard |
| Early Payment Trends | Data Product | Incomplete analysis |

### What happens if Job: GoldLoad_Procurement fails?

| Impacted | Impact |
|----------|--------|
| gold_procurement.po_overview | Stale data |
| gold_procurement.pr_overview | Stale data |
| Procurement Cockpit Dashboard | Shows yesterday's data |
| Early Payment Trends | Stale analysis |
| Procurement Agent | Answers based on old data |
| Payment Prediction | Predictions based on old data |