# Data Model

## Bronze Tables

| Table | Source | Refresh Frequency | Format |
|-------|--------|-------------------|--------|
| raw_customers | SAP | Daily 6 AM | Delta |
| raw_transactions | Oracle | Hourly | Delta |
| raw_products | REST API | Real-time | Delta |
| raw_invoices | Flat File | Daily 8 AM | Delta |
| raw_employees | SAP HR | Daily 7 AM | Delta |

## Silver Cleansed Tables

| Table | Source Table | Transformations |
|-------|-------------|-----------------|
| cleansed_customers | raw_customers | Dedup, null handling, type cast |
| cleansed_transactions | raw_transactions | Dedup, date standardization |
| cleansed_products | raw_products | Schema enforcement |
| cleansed_invoices | raw_invoices | Format standardization |
| cleansed_employees | raw_employees | PII masking, dedup |

## Silver ER Tables

| Table | Description |
|-------|-------------|
| master_customer | Unified customer entity across sources |
| master_product | Unified product catalog |
| master_employee | Unified employee records |
| customer_product_mapping | Relationship mapping |

## Gold Tables

| Table | Description | Owner | Consumers |
|-------|-------------|-------|-----------|
| dim_customer | Customer dimension | EDL Governance | Power BI, API |
| dim_product | Product dimension | EDL Governance | Power BI, API |
| dim_employee | Employee dimension | EDL Governance | Power BI |
| fact_sales | Sales transactions | EDL Governance | Power BI |
| fact_invoices | Invoice details | EDL Governance | Power BI |
| agg_daily_revenue | Daily revenue KPI | EDL Governance | Dashboard |
| agg_monthly_summary | Monthly business summary | EDL Governance | Dashboard |

## Data Quality Rules

| Rule | Applied At | Description |
|------|-----------|-------------|
| Not Null Check | Silver Cleansed | Key columns cannot be null |
| Deduplication | Silver Cleansed | Remove duplicate records |
| Type Validation | Silver Cleansed | Ensure correct data types |
| Referential Integrity | Silver ER | Foreign key validation |
| Business Rules | Gold | KPI calculation validation |