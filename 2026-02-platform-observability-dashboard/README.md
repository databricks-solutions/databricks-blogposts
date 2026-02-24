# Databricks Platform Observability Dashboard

A comprehensive solution for monitoring, tracking, and optimizing Databricks platform costs, usage, and operational health. This repository provides ready-to-use dashboards and automation scripts to gain complete visibility into your Databricks environment.

## 🎯 Overview

This project delivers a production-ready **Platform Observability and Cost Tracking Dashboard** for Databricks. It leverages Databricks System Tables to provide:

- **Real-time cost tracking** across all compute types (All-Purpose, Job Clusters, SQL Warehouses, Serverless)
- **Usage analytics** with detailed breakdowns by workspace, cluster, job, and user
- **Reliability metrics** including job success/failure rates and cluster performance
- **Platform hygiene monitoring** to identify misconfigurations and optimization opportunities
- **Automated data pipeline** that materializes dashboard tables in parallel for optimal performance

## ✨ Features

### Cost & Usage Tracking
- 📊 **Comprehensive cost analysis** across all Databricks workloads (3-year history)
- 💰 **Currency conversion** and discount support for accurate financial reporting
- 📈 **Trend analysis** with daily/monthly granularity
- 🏷️ **Tag-based cost allocation** for chargebacks and showbacks
- 💡 **Potential savings identification** based on resource utilization

### Operational Insights
- ✅ **Job reliability tracking** with success/failure rates
- ⚡ **Cluster performance metrics** including CPU and memory utilization
- 🔍 **Resource optimization recommendations**
- 📍 **Idle resource detection** (warehouses, clusters)
- ⏰ **Uptime monitoring** for SQL Warehouses

### Platform Hygiene
- 🚨 **Outdated DBR version detection** (versions 5.x-9.x)
- ⚙️ **Missing autoscaling configuration** alerts
- 🔴 **Auto-termination compliance** monitoring
- 💸 **Spot instance adoption** tracking
- 🔬 **Preview channel usage** identification

### Technical Highlights
- 🚀 **Parallel query execution** for fast dashboard materialization
- 📦 **Delta Lake optimized** tables with automatic OPTIMIZE and VACUUM
- 🔄 **Incremental processing** with configurable time windows
- 📝 **Execution logging** for monitoring and troubleshooting
- 🎨 **Lakeview Dashboard** with rich visualizations

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│              Databricks System Tables                   │
│  (billing, compute, access, lakeflow)                   │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│     Parallel Materialization Notebook                   │
│  (materialize_dashboard_queries_run_parallely.py)       │
│                                                          │
│  • Executes 20+ complex SQL queries in parallel         │
│  • Creates curated Delta tables                         │
│  • Applies OPTIMIZE and VACUUM operations               │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│         Materialized Dashboard Tables                   │
│  (main.default)               │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│         Lakeview Dashboard (AI/BI)                      │
│  (Databricks Cost Tracking.lvdash.json)                 │
│                                                          │
│  • Executive Summary                                     │
│  • Cost Analysis by Workload Type                       │
│  • Optimization Recommendations                         │
│  • Compliance & Hygiene Reports                         │
└─────────────────────────────────────────────────────────┘
```

## 📋 Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **System Tables** enabled in your account (required for billing and usage data)
- **Databricks Runtime** 13.3 LTS or higher
- **Permissions**:
  - `USE CATALOG` on system catalog
  - `USE SCHEMA` on billing, compute, access, and lakeflow schemas
  - `SELECT` permissions on system tables
  - `CREATE TABLE` permissions on destination catalog/schema
  - `CAN USE` permission granted on the SQL Warehouse resource that executes the dashboard's queries

## 🚀 Getting Started

### Step 1: Download Dashboard Assets

Clone this repository or download the following files:
- `materialize_dashboard_queries_run_parallely.py` - Data materialization notebook
- `Databricks Cost Tracking.lvdash.json` - Lakeview dashboard configuration
- `README.md` - Detailed documentation

### Step 2: Import Assets into Your Workspace

1. **Import the Notebook:**
   ```
   Workspace → Import → Select materialize_dashboard_queries_run_parallely.py
   ```

2. **Import the Dashboard:**
   ```
   Dashboards → Import → Select "Databricks Cost Tracking.lvdash.json"
   ```

### Step 3: Configure Parameters

Edit the notebook parameters:

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| `discount` | Discount percentage from list price | `33` | No |
| `currency_conversion` | Conversion rate from USD (0 = no conversion) | `0` | No |
| `destination_catalog` | Target catalog for tables | `main` | Yes |
| `destination_schema` | Target schema for tables | `default` | Yes |

### Step 4: Run the Materialization Notebook

Execute the notebook to create all dashboard tables:
```python
# The notebook will:
# 1. Create ~20 materialized tables in parallel
# 2. OPTIMIZE each table for query performance
# 3. VACUUM old files to reduce storage costs
# 4. Log execution status to a log table
```

**⏱️ Execution Time:** 10-20 minutes depending on data volume and cluster size

**💡 Tip:** Schedule this notebook to run daily using Databricks Jobs for up-to-date insights in the dashboard.

### Step 5: Configure Dashboard Datasets

1. **Open the dashboard:** Navigate to and open the imported dashboard in your workspace
2. **Access data settings:** Switch to the **Data** tab (this tab is only visible when the dashboard is in *draft* mode)
3. **Update data sources:** For each dataset query, update the `catalog` and `schema` references to point to your `destination_catalog` and `destination_schema`
4. **Validate changes:** Save your modifications and refresh the dashboard to ensure all data connections are working correctly

## 📊 Dashboard Components

## 1. Executive Summary (Page 1)

- **Total Cost:** Aggregate and YTD costs
- **Savings:** All-Purpose (job migration) and Job cluster (CPU-based) opportunities
- **Distribution:** Pie charts by cluster type and job status
- **Trends:** Daily/monthly cost patterns
- **Workspace Comparison:** Cost and savings by workspace

## 2. All-Purpose Cluster Cost Analysis (Page 2)

- **Metrics:** Total cost, DBUs, job runs with cluster filtering
- **Savings:** Jobs on All-Purpose clusters (45.4% potential)
- **SKU Breakdown:** Cost by cluster type
- **Trends:** Daily/monthly patterns
- **Focus:** Scheduled job migration opportunities

## 3. Job Cluster Cost Analysis (Page 3)

- **Metrics:** Cost, DBUs, run counts by job/pipeline and type
- **Status:** Distribution by run type and result
- **Savings:** CPU/memory utilization-based recommendations
- **SKU Analysis:** Highest-cost cluster types
- **Trends:** Daily/monthly costs and run summaries

## 4. Serverless Cost Analysis (Page 4)

- **Filtering:** By product type (Interactive, Jobs, SQL, DLT) and user
- **Metrics:** Total costs and DBUs
- **Distribution:** By SKU and billing origin
- **Trends:** Daily/monthly by workload type
- **Attribution:** User-level cost tracking

## 5. SQL Warehouse Analysis (Page 5)

- **Metrics:** Cost and DBUs across Classic, Pro, Serverless
- **Distribution:** Workspace-level and type comparison
- **SKU Analysis:** Highest-cost warehouses
- **Uptime:** Top workspaces by warehouse uptime
- **Trends:** Daily/monthly patterns

## 6. Unfollowed Best Practices (Page 6)

- **Autoscaling:** Fixed worker clusters
- **Auto-Termination:** Missing or excessive (>30 min)
- **Runtimes:** Outdated DBR versions
- **Spot Instances:** Non-production clusters not using spot
- **Warehouse Config:** Auto-stop issues, idle (30+ days), non-current channels, untagged resources

## 7. Executive Summary Details (Page 7)

- **Table:** All cost data with region, workspace, SKU, cluster type, usage, date
- **Pivot:** Cost by workspace with monthly columns
- **Filtering:** By cluster type, SKU, workspace
- **Export:** Table format for analysis
- **Time-Series:** Granular cost evolution

## 8. All-Purpose Cost Analysis Details (Page 8)

- **Configuration:** Complete cluster specs and settings
- **Savings:** Specific clusters with 45.4% migration potential
- **Filtering:** By SKU, cluster name, cluster ID
- **Attribution:** Costs linked to configurations
- **Analysis:** Workspace-level optimization potential

## 9. Job Cluster Cost Analysis Details (Page 9)

- **Run-Level:** Individual costs with CPU/memory metrics and savings
- **Job-Wise:** Aggregated costs by job with run type and SKU
- **Failures:** Failed run costs and success/failure ratios
- **Performance:** CPU/memory utilization for right-sizing
- **Filtering:** By SKU, run type, job/pipeline, job ID

## 10. Serverless Analysis Details (Page 10)

- **Overall:** All resources with workspace, user, resource ID/name, DBUs, costs
- **User-Level:** Top users by DBU consumption
- **Job-Specific:** Serverless job consumption with resource tracking
- **Notebook-Specific:** Notebook usage with paths
- **Filtering:** By user, job name, notebook name

## 11. SQL Warehouse Details (Page 11)

- **Configuration:** Complete warehouse specs and settings
- **Uptime:** Detailed records with start/end times and hours
- **Filtering:** By SKU, warehouse ID, name, type
- **Attribution:** Costs linked to configurations
- **Compliance:** Tags for resource ownership

## 12. Unfollowed Best Practices Details (Page 12)

- **Fixed Workers:** Clusters without autoscaling
- **Auto-Termination:** Missing/excessive settings (HTML highlighted)
- **Outdated DBR:** Versions with upgrade recommendations (color-coded)
- **On-Demand:** Clusters not using spot instances
- **Warehouse Issues:** Idle (30+ days), auto-stop issues, non-current channels, untagged
- **Filtering:** By cluster name, warehouse ID, warehouse name

## 💡 Cost Optimization Insights

The dashboard automatically identifies optimization opportunities:

1. **Job Runs on All-Purpose Clusters**
   - Detects jobs running on interactive clusters
   - Calculates potential savings by migrating to job clusters
   - Typical savings: **45-50%** of all-purpose cluster costs

2. **Underutilized Resources**
   - Identifies clusters/warehouses with low CPU/memory utilization
   - Recommends rightsizing or autoscaling configuration
   - Potential savings based on actual utilization patterns

3. **Auto-Termination Gaps**
   - Flags clusters/warehouses with missing or excessive auto-termination
   - Prevents runaway costs from forgotten resources

4. **Spot Instance Opportunities**
   - Identifies clusters using only on-demand instances
   - Recommends spot instance adoption for fault-tolerant workloads
   - Potential savings: **60-90%** on compute costs

5. **Failed Jobs Cost Tracking**
   - Quantifies wasted spend on failed job runs
   - Helps prioritize reliability improvements

## 🗂️ System Tables Used

This solution queries the following Databricks System Tables:

| Schema | Table | Purpose |
|--------|-------|---------|
| `system.billing` | `usage` | DBU consumption data |
| `system.billing` | `list_prices` | Pricing information |
| `system.compute` | `clusters` | Cluster configurations |
| `system.compute` | `warehouses` | SQL Warehouse configurations |
| `system.compute` | `warehouse_events` | Warehouse lifecycle events |
| `system.compute` | `node_timeline` | Node-level performance metrics |
| `system.access` | `workspaces_latest` | Workspace metadata |
| `system.lakeflow` | `jobs` | Job definitions |
| `system.lakeflow` | `job_run_timeline` | Job execution history |

## ⚙️ Configuration

### Customizing Materialized Tables

The notebook creates the following tables (all prefixed with `{destination_catalog}.{destination_schema}`):

| Table Name | Description |
|------------|-------------|
| `all_total_cost_and_quantity_workload` | Overall cost by workload type |
| `total_cost_and_quantity_interactive_cluster` | All-purpose cluster costs |
| `all_purpose_cost_by_tags` | Tag-based cost allocation |
| `total_cost_and_quantity_job` | Job cluster costs & metrics |
| `total_job_run_and_result_status` | Job reliability metrics |
| `jobs_no_scaling` | Jobs missing autoscaling |
| `jobs_using_outdated_dbr_version` | Legacy runtime detection |
| `active_clusters_wo_auto_termination` | Auto-term compliance |
| `cluster_with_no_spot_instances` | Spot adoption tracking |
| `ap_cluster_by_job_runs` | AP cluster job analysis |
| `all_total_cost_and_quantity_warehouse` | SQL Warehouse costs |
| `idle_warehouse` | Idle warehouse detection |
| `auto_stop_minutes_warehouse` | Auto-stop compliance |
| `warehouse_channel_preview` | Preview channel usage |
| `warehouse_most_uptime` | Warehouse uptime analysis |
| `serverless_usage_metrics` | Serverless cost & usage |
| `mom_potential_saving_trend` | MoM savings opportunities |
| `log_table` | Execution logs |



### Adjusting Time Windows

By default, queries look back **3 years**. To modify:

```python
# In the notebook, find and replace:
WHERE usage_date BETWEEN current_timestamp() - INTERVAL 3 YEARS AND current_timestamp()

# Change to desired window, e.g., 1 year:
WHERE usage_date BETWEEN current_timestamp() - INTERVAL 1 YEAR AND current_timestamp()
```

## 📖 Usage

### Scheduled Job Execution (RECOMMENDED)
**Recommended Schedule:** Daily

1. Create a new Databricks Job
2. Add the materialization notebook as a task
3. Configure parameters as widgets
4. Set schedule (recommended: daily at off-peak hours)
5. Configure notifications for failures

### Manual Execution

```python
# In a notebook or via Databricks CLI:
dbutils.widgets.text('discount', '33')
dbutils.widgets.text('currency_conversion', '0')
dbutils.widgets.text('destination_catalog', 'main')
dbutils.widgets.text('destination_schema', 'default')

# Then run the notebook
%run /path/to/materialize_dashboard_queries_run_parallely
```

### Querying Materialized Tables

```sql
-- Example: Top 10 most expensive jobs this month
SELECT 
  job_or_pipeline_name,
  SUM(total_cost) as monthly_cost,
  SUM(total_usage_quantity_dbu) as total_dbu
FROM main.default.total_cost_and_quantity_job
WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
GROUP BY job_or_pipeline_name
ORDER BY monthly_cost DESC
LIMIT 10;
```

## 🔧 Maintenance

### Monitoring

Check the `log_table` for execution status:

```sql
SELECT * 
FROM main.default.log_table 
WHERE modified_date >= CURRENT_DATE()
ORDER BY modified_date DESC;
```

### Performance Tuning

**Option 1: Serverless (RECOMMENDED)**
- **Automatic scaling:** Serverless SQL warehouses scale automatically based on workload.
- **Optimized performance:** Built-in Photon acceleration with no configuration needed.
- **Cost-effective:** Pay only for actual compute time used. For better costing use STANDARD mode.
- **Zero management:** No cluster sizing or tuning required.

**Option 2: Job Compute**
- **Cluster sizing:** Use a medium-large cluster (4-8 cores) for optimal parallel execution.
- **Photon acceleration:** Enable Photon for 2-3x faster query execution.
- **Time windows:** Reduce lookback window if processing is slow.
- **Incremental updates:** Modify queries to process only recent data for faster refreshes.

### Data Retention

The notebook automatically:
- Keeps 30 days of logs: `DELETE FROM log_table WHERE modified_date < current_timestamp() - INTERVAL 30 DAYS`
- Vacuums tables after materialization to remove old files and optimize storage.

## 📚 Additional Resources

- **Platform Monitoring.md** - Comprehensive documentation on platform observability concepts
- **Databricks System Tables Documentation** - [Link to official docs](https://docs.databricks.com/en/admin/system-tables/index.html)
- **Lakeview Dashboard Guide** - [Link to official docs](https://docs.databricks.com/en/dashboards/index.html)

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Areas for Contribution

- Additional visualization templates
- Support for other cloud providers (AWS, GCP)
- Enhanced cost allocation algorithms
- Integration with external BI tools
- Anomaly detection and alerting

## 🛟 Project Support

This project is a Field Engineering effort and is **not officially supported by Databricks**.

## Built using:
- **Databricks System Tables** for unified data access
- **Apache Spark** for parallel query execution
- **Delta Lake** for reliable, performant analytics tables
- **Databricks AI BI Dashboards** for interactive dashboards
---
