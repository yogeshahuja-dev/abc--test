# Enterprise Data Lake (EDL)

Welcome to the EDL documentation portal — your single pane of glass for the entire data platform.

## Overview

The Enterprise Data Lake is our centralized data platform built on **Azure Databricks** with **Unity Catalog**. It follows the **Medallion Architecture**: Bronze → Silver Cleansed → Silver → Gold → Consumers.

## What This Portal Covers

| Area | Description |
|------|-------------|
| **Tables** | Every table from Bronze to Gold, with full lineage |
| **Jobs** | Databricks Workflow jobs that move data between layers |
| **Dashboards** | Business dashboards consuming Gold data |
| **Data Products** | Curated data products served to consumers |
| **ML Endpoints** | Genie agents and ML model serving endpoints |
| **APIs** | REST APIs for data products and ML serving |

## Data Flow Summary

| Layer | Example Tables | Populated By |
|-------|---------------|--------------|
| Bronze | bronze_sap_aem.t024_slt, bronze_sap_p11.lfa1_slt | SLT Replication |
| Silver Cleansed | silver_sap_cleansed.t024_mstr, lfa1_mstr | Job: SilverCleansedLoad |
| Silver | silver.purchasing_group, silver.vendor | Job: SilverLoad_Procurement |
| Gold | gold_procurement.po_overview, pr_overview | Job: GoldLoad_Procurement |
| Dashboard | Procurement Cockpit | Built on Gold |
| Data Product | Early Payment Trends | Built on Gold |
| ML | Procurement Agent, Payment Prediction | Trained on Gold |

## Quick Links

- [Architecture](architecture.md)
- [Data Lineage](data-lineage.md)
- [Data Model](data-model.md)
- [Jobs & Orchestration](jobs.md)
- [Data Products](data-products.md)
- [ML Endpoints](ml-endpoints.md)
- [Onboarding Guide](onboarding.md)

## External Links

- [Databricks Workspace](https://adb-2478587080594690.10.azuredatabricks.net/)
- [EDL Core Portal](https://edlcore.adani.com/)
- [EDL Data Model](https://az01lappedlapid02.azurewebsites.net/docs)
- [EDL Data Products](https://az01lappedlapid04.azurewebsites.net/docs)
- [EDL Data Sharing](https://az01lappedlapid03.azurewebsites.net/docs)

## Team

All components are managed by the **EDL Governance** team.