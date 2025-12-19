import streamlit as st
import pandas as pd
import time
from databricks import sdk
from databricks.sdk.core import Config
from sqlalchemy import create_engine, event, text

# Database Configuration - Single source of truth
DB_CONFIG = {
    "host": "your-postgres-instance.database.cloud.databricks.com",  # Replace with your PostgreSQL instance host
    "port": 5432,  
    "database": "your_postgres_database",  # Replace with your database name
    "schema": "your_schema",  # Replace with your schema name
    "table": "your_table"  # Replace with your table name
}

# Databricks config.
app_config = Config()
workspace_client = sdk.WorkspaceClient()

# PostgreSQL config to connect to your database - match original pattern
postgres_username = app_config.client_id
postgres_host = DB_CONFIG["host"]
postgres_port = DB_CONFIG["port"]
postgres_database = DB_CONFIG["database"]

# Validate configuration
if not postgres_host or not postgres_port or not postgres_database:
    st.error("❌ Database configuration is incomplete")
    st.stop()

# sqlalchemy setup + function to refresh the OAuth token that gets used as the PostgreSQL password every 15 minutes.
postgres_pool = create_engine(f"postgresql+psycopg://{postgres_username}:@{postgres_host}:{postgres_port}/{postgres_database}")
postgres_password = None
last_password_refresh = time.time()

@event.listens_for(postgres_pool, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    global postgres_password, last_password_refresh
    if postgres_password is None or time.time() - last_password_refresh > 900:
        print("Refreshing PostgreSQL OAuth token")
        postgres_password = workspace_client.config.oauth_token().access_token
        last_password_refresh = time.time()
    cparams["password"] = postgres_password

def get_table_data():
    """Fetch data from the configured table."""
    try:
        schema = DB_CONFIG["schema"]
        table = DB_CONFIG["table"]
        
        with postgres_pool.connect() as conn:
            query = f"SELECT * FROM {schema}.{table}"
            df = pd.read_sql_query(query, conn)
        return df
        
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return None

def get_table_key_column(df):
    """Dynamically detect the key column for the table."""
    if df is None or df.empty:
        return None
        
    # Check for common key column names
    possible_keys = ['primary_key', 'id', 'key', 'pk']
    
    for key in possible_keys:
        if key in df.columns:
            return key
    
    # If no standard key found, use the first column
    if len(df.columns) > 0:
        return df.columns[0]
    
    return None

def get_summary_stats(df):
    """Calculate summary statistics for numeric columns"""
    if df is None or df.empty:
        return None
        
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        summary = df[numeric_cols].describe()
        return summary
    return None

def get_row_count():
    """Get the total number of rows in the table"""
    try:
        schema = DB_CONFIG["schema"]
        table = DB_CONFIG["table"]
        
        with postgres_pool.connect() as conn:
            query = f"SELECT COUNT(*) as total_rows FROM {schema}.{table}"
            result = conn.execute(text(query)).scalar()
        return result
    except Exception as e:
        st.error(f"Error getting row count: {str(e)}")
        return None

# Page configuration
st.set_page_config(
    page_title="Database Table Viewer",
    page_icon="📊",
    layout="wide"
)

# Header with connection info scorecard
header_left, header_right = st.columns([2, 1])

with header_left:
    st.title("📊 Database Table Viewer")

with header_right:
    # Connection info scorecard
    st.markdown("""
    <div style='background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); 
                padding: 1rem; border-radius: 10px; color: white; margin-top: 1rem;'>
        <div style='font-size: 0.8rem; opacity: 0.9;'>Connected to</div>
        <div style='font-size: 1rem; font-weight: bold; margin: 0.2rem 0;'>{}</div>
        <div style='font-size: 0.8rem; opacity: 0.9;'>Database: <strong>{}</strong></div>
        <div style='font-size: 0.8rem; opacity: 0.9;'>Table: <strong>{}.{}</strong></div>
    </div>
    """.format(
        postgres_host.split('.')[0],  # Short hostname
        postgres_database,
        DB_CONFIG['schema'],
        DB_CONFIG['table']
    ), unsafe_allow_html=True)

# Test connection button
if st.button("🔄 Test Connection"):
    try:
        with postgres_pool.connect() as conn:
            st.success("✅ Connection successful!")
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")

st.markdown("---")

# Main layout: left column for data, right column for statistics
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("📋 Table Data")
    
    # Load and display data
    with st.spinner("Loading data..."):
        df = get_table_data()
    
    if df is not None and not df.empty:
        st.success(f"✅ Successfully loaded {len(df)} rows")
        
        # Display the data
        st.dataframe(df, use_container_width=True, height=600)
        
    else:
        st.warning("⚠️ No data found or table is empty")

with right_col:
    st.subheader("📊 Table Statistics")
    
    if df is not None and not df.empty:
        # Basic info metrics
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Rows", f"{len(df):,}")
        with col2:
            st.metric("Columns", len(df.columns))
        
        # Key column info
        key_col = get_table_key_column(df)
        if key_col:
            st.info(f"🔑 **Key Column:** `{key_col}`")
        
        # Show numeric summary directly (without title)
        summary_stats = get_summary_stats(df)
        if summary_stats is not None:
            st.dataframe(summary_stats, use_container_width=True)
        
    else:
        st.info("Load data to see statistics") 