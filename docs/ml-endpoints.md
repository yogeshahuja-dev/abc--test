# ML Endpoints

## Overview

ML Endpoints are served via **Databricks Model Serving**. They provide real-time predictions and natural language query capabilities on top of Gold layer data.

## Available Endpoints

### 1. Procurement Agent (Genie)

| Property | Value |
|----------|-------|
| Name | Procurement Agent |
| Type | Databricks Genie / AI Agent |
| Owner | EDL Governance |
| Status | Active |
| Source Data | gold_procurement.po_overview, gold_procurement.pr_overview |
| Serving | Databricks Model Serving |

**What it does:**

Users can ask natural language questions about procurement data:

| Example Query | What It Returns |
|--------------|----------------|
| "Show top 10 vendors by spend" | Ranked vendor list with amounts |
| "PO status for last month" | Summary of open/closed/partial POs |
| "Which vendors have pending PRs?" | List of vendors with open requisitions |
| "Compare spend Q1 vs Q2" | Quarter-over-quarter spend analysis |
| "Show purchasing group wise PO distribution" | PO count and amount by purchasing group |

**How to use:**
POST /serving-endpoints/procurement-agent/invocations { "query": "Show top 10 vendors by spend in last quarter", "context": { "domain": "procurement", "max_results": 10 } }
### 2. Payment Prediction Model

| Property | Value |
|----------|-------|
| Name | Payment Prediction |
| Type | ML Model (Classification + Regression) |
| Owner | EDL Governance |
| Status | Active |
| Trained On | Historical PO and PR data from Gold layer |
| Serving | Databricks Model Serving |
| Retrained | Weekly (every Sunday) |

**What it predicts:**

| Prediction | Description |
|-----------|-------------|
| Predicted Payment Date | When the payment is likely to be made |
| Early Payment Probability | Likelihood of early payment (0-1) |
| Discount Opportunity | Potential discount if paid early |

**How to use:**
POST /serving-endpoints/payment-prediction/invocations { "vendor_id": "V001234", "po_number": "PO4500012345", "invoice_amount": 150000.00, "invoice_date": "2026-04-15" }

**Response:**

```json
{
  "predicted_payment_date": "2026-04-25",
  "early_payment_probability": 0.78,
  "discount_opportunity": "2% if paid within 10 days",
  "confidence": 0.89
}


