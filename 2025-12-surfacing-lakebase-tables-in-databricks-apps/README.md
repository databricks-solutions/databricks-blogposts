# PostgreSQL Data Viewer

A Streamlit application for viewing PostgreSQL synced tables in Databricks using OAuth authentication.

## 📝 Blog Post

This code accompanies the Databricks blog post: [Surfacing Lakebase Tables in Databricks Apps](https://www.databricks.com/blog/TBD)

> **Note:** Update the link above once the blog post is published.

## 🔧 **Before You Start - Required Configuration**

**⚠️ IMPORTANT:** This repository contains placeholder configurations. You MUST update these values for your specific database:

### 📝 **Files to Customize:**

1. **`app.py`** - Update the database configuration:
   ```python
   DB_CONFIG = {
       "host": "your-postgres-instance.database.cloud.databricks.com",  # ← Your PostgreSQL host
       "port": 5432,  
       "database": "your_postgres_database",  # ← Your database name
       "schema": "your_schema",  # ← Your schema name  
       "table": "your_table"  # ← Your table name
   }
   ```

2. **`databricks.yml`** - Change the app name:
   ```yaml
   bundle:
     name: your-app-name  # ← Choose your unique app name
   ```

3. **`pyproject.toml`** - Update project name:
   ```toml
   name = "your-app-name"  # ← Match your app name
   ```

---

## Features

- 🔗 **PostgreSQL Database Connection** - Connects to Databricks Lakebase PostgreSQL instances using OAuth authentication
- 📊 **Data Visualization** - Two-column layout with table data and statistics
- 📈 **Table Statistics** - Automatic key column detection, row counts, and numeric summaries
- 🎨 **Modern UI** - Clean Streamlit interface with gradient connection scorecard
- ⚡ **Auto Token Refresh** - Handles Databricks OAuth token refresh automatically

## Quick Start

### Prerequisites
- Databricks workspace access
- Databricks CLI installed and configured
- Access to a PostgreSQL database instance (Lakebase)

### Deployment

1. **Clone the repository:**
   ```bash
   git clone https://github.com/sylvia-222/lakebase-dbx-app-template.git
   cd lakebase-dbx-app-template
   ```

2. **Configure your database connection** (see [Database Configuration](#database-configuration) below)

3. **Deploy using Databricks Asset Bundles:**
   ```bash
   databricks bundle deploy
   ```

4. **Access your app:**
   Your app will be available at the URL shown in the deployment output.

## Database Configuration

To connect to your own PostgreSQL database, update the `DB_CONFIG` section in `app.py`:

```python
# Database Configuration - Single source of truth
DB_CONFIG = {
    "host": "your-instance-hostname.database.cloud.databricks.com",  # Your PostgreSQL host
    "port": 5432,  
    "database": "your_database_name",      # Your database name
    "schema": "your_schema",               # Your schema name
    "table": "your_table_name"             # Your table name
}
```

### Required Changes for Different Databases:

1. **Host**: Update with your PostgreSQL instance hostname
   - Format: `instance-xxxxx.database.cloud.databricks.com`
   - Remove any `https://` prefix or trailing `/`

2. **Database**: Change to your target database name

3. **Schema**: Update to your target schema name

4. **Table**: Specify the table you want to visualize

### Example Configuration:
```python
DB_CONFIG = {
    "host": "instance-12345678-abcd-efgh-ijkl-123456789012.database.cloud.databricks.com",
    "port": 5432,
    "database": "my_analytics_db",
    "schema": "sales_data", 
    "table": "customer_metrics"
}
```

## File Structure

```
├── app.py                              # Main Streamlit application
├── app.yml                             # App configuration for Databricks Apps
├── databricks.yml                      # Databricks Asset Bundle configuration
├── requirements.txt                    # Python dependencies
├── pyproject.toml                      # Project metadata
├── generate_campaign_data.ipynb           # AdTech data generation notebook
├── create_oltp_instance.py             # OLTP instance creation utility
└── README.md                           # This file
```

## Key Files for DAB & App Deployment

- **`app.py`** - Main application code with database configuration
- **`app.yml`** - Streamlit app runtime configuration
- **`databricks.yml`** - Databricks Asset Bundle definition
- **`requirements.txt`** - Python package dependencies
- **`pyproject.toml`** - Project metadata and configuration

## Additional Utilities

- **`generate_campaign_data.ipynb`** - Comprehensive notebook for generating synthetic AdTech campaign performance data with realistic metrics, performance tiers, and Delta table creation with Change Data Feed
- **`create_oltp_instance.py`** - Utility script for creating and managing Databricks OLTP database instances

## Authentication

The app uses Databricks OAuth authentication to connect to PostgreSQL:
- No manual token management required
- Automatic token refresh every 15 minutes
- Uses your Databricks workspace credentials

## App Features

### Connection Scorecard
Beautiful gradient display showing:
- Connected database host
- Database name
- Schema and table being viewed

### Data View (Left Column)
- Full table data display
- Scrollable interface with 600px height
- Row count indicator

### Statistics Panel (Right Column)
- Total rows and column count metrics
- Automatic primary key detection
- Statistical summary for numeric columns

## Troubleshooting

### Common Issues:

1. **Connection Failed**: 
   - Verify your database host format (no `https://` prefix)
   - Ensure you have access to the PostgreSQL instance
   - Check your Databricks workspace permissions

2. **Table Not Found**:
   - Verify schema and table names in `DB_CONFIG`
   - Use the "Test Connection" button to verify connectivity
   - Check that the table exists in your database

3. **Token Issues**:
   - The app handles token refresh automatically
   - If issues persist, restart the app

## Development

To run locally for development:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the Streamlit app
streamlit run app.py
```

## License

This project is licensed under the [Databricks License](../LICENSE).

### Third-Party Dependencies

| Package | License |
|---------|---------|
| [Streamlit](https://streamlit.io/) | Apache 2.0 |
| [psycopg](https://www.psycopg.org/) | LGPL 3.0 |
| [SQLAlchemy](https://www.sqlalchemy.org/) | MIT |
| [Databricks SDK](https://github.com/databricks/databricks-sdk-py) | Apache 2.0 |
