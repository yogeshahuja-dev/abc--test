# EDL Architecture

## Medallion Architecture

### Bronze Layer
- Raw data ingestion from source systems
- No transformations applied
- Data stored as-is in Delta format
- Sources: SAP, Oracle, APIs, Flat Files

### Silver Cleansed Layer
- Data quality checks
- Deduplication
- Standardization of formats
- Schema enforcement
- Null handling and type casting

### Silver ER (Entity Resolution) Layer
- Master data management
- Entity relationships
- Business logic applied
- Cross-source entity matching

### Gold Layer
- Business-ready curated data
- Aggregations and KPIs
- Reporting tables
- Data products for consumption

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Processing | Azure Databricks |
| Storage | Azure Data Lake Gen2 |
| Format | Delta Lake |
| Orchestration | Databricks Workflows |
| CI/CD | GitLab CI/CD |
| Dashboards | Power BI |
| Catalog | Backstage |
| Service Management | ServiceNow |

## Architecture Diagram

| Layer | Input | Output |
|-------|-------|--------|
| Source Systems | SAP, Oracle, APIs, Files | Raw Data |
| Bronze | Raw Data | Delta Tables (as-is) |
| Silver Cleansed | Bronze Tables | Cleaned Delta Tables |
| Silver ER | Cleansed Tables | Resolved Delta Tables |
| Gold | ER Tables | Curated Delta Tables |
| Consumers | Gold Tables | Dashboards, APIs, Data Products |

## Environments

| Environment | Purpose |
|-------------|---------|
| Dev | Development and testing |
| Staging | Pre-production validation |
| Production | Live data processing |