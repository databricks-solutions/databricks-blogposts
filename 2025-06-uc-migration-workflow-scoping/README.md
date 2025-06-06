# How to scope a UC Workflow Migration using System Tables

Migrating workflows from Hive Metastore (HMS) to Unity Catalog (UC) in Databricks is a critical step for enterprises looking to enhance data governance, security, and scalability. This blog post provides a structured approach to scope workflows for migration based on the analysis provided below. By leveraging UCX Assessments, system tables, assessing job metadata, and incorporating stakeholder insights, organizations can effectively prioritize and streamline their migration efforts.

## Pre-Requisites

Before diving into the workflow scoping process, it is essential to:

1. Install **UCX** and run the UCX Assessment in each workspace.  
2. Ensure **Unity Catalog** is enabled in the workspace and a metastore is attached.  
3. Enable and grant access to the necessary Databricks system tables.  

> **Note:** In the following analysis, we will be using the terms _jobs_ and _workflows_ interchangeably, as they both refer to Databricks Workflows.

## Required System Tables

The following system tables must be enabled to gather the necessary data for scoping workflows:

- `system.lakeflow.jobs`  
  Contains metadata about all jobs created in the account, including job IDs, names, and workspace associations.

- `system.billing.list_prices`  
  Provides historical pricing information for various SKUs, enabling cost analysis of workflows.

- `system.lakeflow.job_run_timeline`  
  Tracks the start and end times of job runs, allowing for an analysis of job execution frequency and success rates.

- `system.lakeflow.job_task_run_timeline`  
  Captures task-level execution details, including compute resource usage.

- `system.access.audit`  
  Logs cluster-related actions, such as creation or edits, to identify workflows already running on Unity Catalog–compliant clusters.

- `system.billing.usage`  
  Aggregates billable usage data across jobs to calculate workflow costs.

Follow the instructions [here](https://docs.databricks.com/data-governance/unity-catalog/system-tables.html) to enable these system schemas.
