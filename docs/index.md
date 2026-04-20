# Enterprise Data Lake (EDL)

Welcome to the EDL documentation portal.

## Overview

The Enterprise Data Lake is our centralized data platform built on **Azure Databricks**.
It follows the medallion architecture: Bronze → Silver Cleansed → Silver ER → Gold.

## Data Flow

| Stage | Description |
|-------|-------------|
| Bronze | Raw data from source systems |
| Silver Cleansed | Quality checked and deduplicated |
| Silver ER | Entity resolved and business logic applied |
| Gold | Business-ready curated data |
| Consumers | Dashboards, APIs, Data Products |

## Quick Links

- [Architecture](architecture.md)
- [Data Model](data-model.md)
- [Onboarding Guide](onboarding.md)

## Team

All components are managed by the **EDL Governance** team.