# retail-lakehouse-databricks-project

# Retail Lakehouse Data Platform using Databricks

## Project Overview

This project implements an end-to-end Retail Lakehouse Data Platform using Databricks, PySpark, Delta Lake, Unity Catalog, and Databricks Workflows following the Medallion Architecture (Bronze, Silver, Gold).

## Architecture

Raw CSV Files
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
Data Quality Validation
↓
Databricks Workflow
↓
Email Notification

## Technology Stack

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Databricks Jobs
- GitHub

## Project Features

- Bronze Layer Data Ingestion
- Silver Layer Data Cleaning
- Gold Layer KPI Generation
- Delta Lake Tables
- Time Travel
- Incremental Load using MERGE INTO
- Data Quality Validation
- Databricks Workflow Automation
- Scheduled Job Execution
- Email Notifications

## Data Model

### Customers
- customer_id
- customer_name
- city
- signup_date

### Products
- product_id
- product_name
- category
- price

### Orders
- order_id
- customer_id
- product_id
- quantity
- order_date

## Business KPIs

- Daily Revenue
- Revenue by Category
- Top Customers
- Order Analytics

## Workflow

bronze_ingestion
↓
silver_cleaning
↓
gold_kpi_generation
↓
data_quality_validation

## Project Outcome

Successfully built an automated Databricks Lakehouse pipeline using Medallion Architecture with Delta Lake, Incremental Processing, Data Quality Checks, and Job Orchestration.
