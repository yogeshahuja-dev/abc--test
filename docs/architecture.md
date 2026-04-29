# EDL Architecture

## Medallion Architecture

Our platform follows a strict layered approach where data flows through well-defined stages.

### Bronze Layer (Raw)
- Raw data ingestion from SAP systems via SLT replication
- No transformations applied — data stored as-is in Delta format
- Source systems: **SAP AEM**, **SAP P11**, **SAP MUL**
- Example tables: `bronze_sap_aem.t024_slt`, `bronze_sap_p11.lfa1_slt`

### Silver Cleansed Layer (Cleaned)
- Merges data from all 3 SAP systems (AEM + P11 + MUL) into master tables
- Transformations: Deduplication, null handling, type casting, source system tagging
- Populated by: **Job: SilverCleansedLoad_BSIK_BSAK**
- Example tables: `silver_sap_cleansed.t024_mstr`, `silver_sap_cleansed.lfa1_mstr`

### Silver Layer (Business Entities)
- Business entities with logic and entity resolution applied
- Populated by: **Job: SilverLoad_Procurement**
- Example tables: `silver.purchasing_group`, `silver.vendor`, `silver.material`

### Gold Layer (Curated)
- Business-ready curated data for consumption
- Aggregations, KPIs, reporting tables
- Populated by: **Job: GoldLoad_Procurement**
- Example tables: `gold_procurement.po_overview`, `gold_procurement.pr_overview`

### Consumers
- **Dashboards**: Procurement Cockpit
- **Data Products**: Early Payment Trends
- **ML Endpoints**: Procurement Agent (Genie), Payment Prediction Model
- **APIs**: Data Product REST API, ML Serving API

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Processing Engine | Azure Databricks |
| Storage | Azure Data Lake Gen2 |
| Table Format | Delta Lake |
| Catalog | Unity Catalog |
| Orchestration | Databricks Workflows |
| CI/CD | GitLab CI/CD |
| Dashboards | Databricks SQL Dashboards / Power BI |
| ML Serving | Databricks Model Serving |
| AI Agent | Databricks Genie |
| Developer Portal