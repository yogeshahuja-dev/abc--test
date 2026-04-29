# Jobs & Orchestration

## Overview

Data movement between layers is orchestrated by **Databricks Workflow Jobs**. Jobs run in sequence — each job depends on the previous one completing successfully.

## Job Chain

| Order | Job Name | From Layer | To Layer | Schedule | Duration |
|-------|----------|-----------|----------|----------|----------|
| 1 | SilverCleansedLoad_BSIK_BSAK | Bronze | Silver Cleansed | Daily 6:00 AM IST | ~45 min |
| 2 | SilverLoad_Procurement | Silver Cleansed | Silver | Daily 7:00 AM IST | ~30 min |
| 3 | GoldLoad_Procurement | Silver | Gold | Daily 8:00 AM IST | ~20 min |

## Job Details

### Job 1: SilverCleansedLoad_BSIK_BSAK

**Purpose:** Moves raw data from Bronze to Silver Cleansed layer.

| Property | Value |
|----------|-------|
| Schedule | Daily 6:00 AM IST |
| Input Tables | bronze_sap_aem.t024_slt, bronze_sap_p11.t024_slt, bronze_sap_mul.t024_slt, bronze_sap_aem.lfa1_slt, bronze_sap_p11.lfa1_slt, bronze_sap_mul.lfa1_slt |
| Output Tables | silver_sap_cleansed.t024_mstr, silver_sap_cleansed.lfa1_mstr |
| Transformations | Merge 3 SAP sources, deduplication, null handling, type casting, source tagging |
| Cluster | EDL Production Cluster |
| Retry Policy | 2 retries with 5 min delay |

### Job 2: SilverLoad_Procurement

**Purpose:** Moves cleansed data from Silver Cleansed to Silver layer, applying business logic.

| Property | Value |
|----------|-------|
| Schedule | Daily 7:00 AM IST |
| Input Tables | silver_sap_cleansed.t024_mstr, silver_sap_cleansed.lfa1_mstr |
| Output Tables | silver.purchasing_group, silver.vendor, silver.material |
| Transformations | Business logic, entity resolution, standardization |
| Depends On | SilverCleansedLoad_BSIK_BSAK must complete |
| Cluster | EDL Production Cluster |

### Job 3: GoldLoad_Procurement

**Purpose:** Moves business entities from Silver to Gold layer, creating curated tables.

| Property | Value |
|----------|-------|
| Schedule | Daily 8:00 AM IST |
| Input Tables | silver.purchasing_group, silver.vendor, silver.material |
| Output Tables | gold_procurement.po_overview, gold_procurement.pr_overview |
| Transformations | Aggregation, KPI calculation, enrichment |
| Depends On | SilverLoad_Procurement must complete |
| Cluster | EDL Production Cluster |

## Monitoring

| What to Monitor | Where | Alert |
|----------------|-------|-------|
| Job failures | Databricks Workflows UI | Email + Slack |
| Job duration spike | Databricks Workflows UI | If > 2x normal duration |
| Data quality | Silver Cleansed layer | Row count drops > 10% |
| Stale Gold data | Gold layer | If not refreshed by 9:00 AM |

## Failure Handling

| Scenario | Impact | Action |
|----------|--------|--------|
| SilverCleansedLoad fails | Silver, Gold, Dashboard all stale | Fix and re-run. Downstream jobs wait. |
| SilverLoad fails | Gold, Dashboard stale | Fix and re-run. Gold job waits. |
| GoldLoad fails | Dashboard shows old data | Fix and re-run. Dashboard auto-refreshes. |
| One Bronze source missing | Partial data in Silver Cleansed | Job still runs with available sources. Alert raised. |