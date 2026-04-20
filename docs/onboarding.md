# Onboarding Guide

## For New Team Members

### Day 1
1. Get access to Azure Databricks workspace
2. Clone the GitLab repository
3. Set up local development environment
4. Review this documentation

### Week 1
1. Review the data model documentation
2. Understand Bronze to Silver to Gold flow
3. Run existing pipelines in dev environment
4. Start with a small Bronze layer task

### Week 2
1. Write your first Silver layer transformation
2. Submit merge request for code review
3. Deploy to staging via CI/CD

## Access Requests

| System | How to Request | Approver |
|--------|---------------|----------|
| Databricks | ServiceNow ticket | EDL Governance |
| GitLab | Team lead approval | EDL Governance |
| Power BI | Self-service portal | Auto-approved |
| Backstage | GitHub login | Auto-approved |

## Coding Standards

- All code in Python/PySpark
- Follow PEP 8 style guide
- Unit tests required for all transformations
- Code review required before merge
- Use Delta Lake format for all tables

## CI/CD Pipeline Flow

| Step | Action | Tool |
|------|--------|------|
| 1 | Developer pushes to feature branch | GitLab |
| 2 | Automated tests run | GitLab CI |
| 3 | Code review and approval | GitLab MR |
| 4 | Merge to main branch | GitLab |
| 5 | Deploy to staging | GitLab CD |
| 6 | Validation in staging | Manual |
| 7 | Deploy to production | GitLab CD |

## Contact

For any questions, reach out to the **EDL Governance** team.