# ${{ values.pipelineName }}

${{ values.description }}

## Data Layer

This pipeline belongs to the **${{ values.layer }}** layer.

## Owner

EDL Governance Team

## System

Enterprise Data Lake

## Getting Started

1. Clone this repository
2. Set up your Databricks workspace connection
3. Update the pipeline configuration
4. Run the pipeline in dev environment
5. Submit merge request for review

## Folder Structure

- catalog-info.yaml - Backstage catalog entry
- README.md - This file
- src/pipeline.py - Main pipeline code
- tests/test_pipeline.py - Unit tests
- config/config.yaml - Pipeline configuration

## CI/CD

This pipeline is deployed via GitLab CI/CD to Databricks.