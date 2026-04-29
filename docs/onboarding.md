# Onboarding Guide

## For New Team Members

### Day 1: Access Setup

| Step | Action | How |
|------|--------|-----|
| 1 | Get Databricks workspace access | ServiceNow ticket → EDL Governance approves |
| 2 | Get GitLab repository access | Team lead adds you to the group |
| 3 | Login to Backstage | Use your GitHub account at http://localhost:3000 |
| 4 | Read this documentation | Start with Architecture → Data Lineage → Data Model |

### Week 1: Understanding the Platform

| Day | Focus Area | What to Do |
|-----|-----------|-----------|
| Mon | Architecture | Read architecture docs, understand medallion layers |
| Tue | Data Lineage | Trace data from Bronze to Dashboard in Backstage catalog |
| Wed | Data Model | Review all table schemas in Bronze, Silver, Gold |
| Thu | Jobs | Understand the 3 jobs and their orchestration chain |
| Fri | Hands-on | Run existing pipelines in dev environment |

### Week 2: First Contribution

| Day | Focus Area | What to Do |
|-----|-----------|-----------|
| Mon | Clone repo | Clone GitLab repo, set up local dev environment |
| Tue | Small task | Pick a small Bronze layer task from backlog |
| Wed | Code | Write your first transformation |
| Thu | Test | Write unit tests, run in dev |
| Fri | Review | Submit merge request for code review |

## Access Matrix

| System | How to Request | Approver | Turnaround |
|--------|---------------|----------|------------|
| Databricks Workspace | ServiceNow ticket | EDL Governance | 1-2 days |
| GitLab Repository | Team lead request | EDL Governance | Same day |
| Power BI Dashboards | Self-service portal | Auto-approved | Instant |
| Backstage Portal | GitHub login | Auto-approved | Instant |
| Unity Catalog (read) | Databricks access | Auto with workspace | With workspace |
| Unity Catalog (write) | ServiceNow ticket | EDL Governance | 1-2 days |

## Key Links

| Resource | URL |
|----------|-----|
| Databricks Workspace | [Open](https://adb-2478587080594690.10.azuredatabricks.net/) |
| EDL Core Portal | [Open](https://edlcore.adani.com/) |
| EDL Data Model Docs | [Open](https://az01lappedlapid02.azurewebsites.net/docs) |
| EDL Data Products Docs | [Open](https://az01lappedlapid04.azurewebsites.net/docs) |
| EDL Data Sharing Docs | [Open](https://az01lappedlapid03.azurewebsites.net/docs) |
| Data Governance Docs | [Open](https://az01lappedlapid03.azurewebsites.net/docs) |
| Backstage Portal | [Open](http://localhost:3000) |

## Coding Standards

| Rule | Details |
|------|---------|
| Language | Python and PySpark |
| Style | PEP 8 |
| Testing | Unit tests required for all transformations |
| Review | Code review required before merge to main |
| Format | Delta Lake for all tables |
| Naming | snake_case for tables and columns |
| Documentation | Every table must have a Backstage catalog entry |

## CI/CD Pipeline

| Step | Action | Tool |
|------|--------|------|
| 1 | Developer pushes to feature branch | GitLab |
| 2 | Automated tests run | GitLab CI |
| 3 | Code review and approval | GitLab MR |
| 4 | Merge to main branch | GitLab |
| 5 | Deploy to staging | GitLab CD |
| 6 | Validation in staging | Manual + automated checks |
| 7 | Deploy to production | GitLab CD |

## Who to Contact

| Question | Contact |
|----------|---------|
| Access issues | EDL Governance team |
| Data quality questions | EDL Governance team |
| Pipeline failures | On-call engineer (PagerDuty) |
| New data source request | EDL Governance team |
| Dashboard requests | EDL Governance team |