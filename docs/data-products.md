# Data Products

## Overview

Data Products are curated, well-documented, and governed datasets built on the **Gold layer**. They are designed for specific business use cases and served to consumers via APIs, dashboards, or direct table access.

## Available Data Products

### Early Payment Trends

| Property | Value |
|----------|-------|
| Name | Early Payment Trends |
| Domain | Procurement / Finance |
| Owner | EDL Governance |
| Status | Active |
| Source Tables | gold_procurement.po_overview, gold_procurement.pr_overview |
| Consumers | Finance team, Treasury, Procurement leadership |
| Refresh | Daily (after GoldLoad_Procurement completes) |
| SLA | Data available by 9:00 AM IST |
| Access | Via Data Product REST API or direct table access |

**What it provides:**

- Early payment discount utilization rates
- Payment timeline analysis by vendor
- Cash flow optimization opportunities
- Vendor payment behavior patterns
- Month-over-month payment trend comparison

**Business Value:**

- Identifies vendors eligible for early payment discounts
- Estimates potential savings from early payments
- Helps Treasury team optimize cash flow
- Provides procurement leadership with spend visibility

## Data Product Lifecycle

| Stage | Description |
|-------|-------------|
| Draft | Data product defined, tables identified |
| Development | Transformations built, API configured |
| Review | Data quality validated, documentation reviewed |
| Active | Live and serving consumers |
| Deprecated | Replaced by newer version, consumers migrated |

## How to Access Data Products

### Option 1: Direct Table Access (Databricks)
```sql
SELECT * FROM gold_procurement.po_overview LIMIT 100;