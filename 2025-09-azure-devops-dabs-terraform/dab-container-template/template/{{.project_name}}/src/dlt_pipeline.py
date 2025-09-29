# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline for Automated Quote Management
# MAGIC 
# MAGIC This notebook defines a Delta Live Tables pipeline for automated quote processing and management.
# MAGIC The pipeline processes email-parsed quotes through bronze, silver, and gold layers to match the database schema.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC The catalog and schema are automatically configured via the pipeline configuration.

# COMMAND ----------

# Get pipeline configuration
catalog = spark.conf.get("catalog", "{{.catalog}}")
schema = spark.conf.get("schema", "{{.schema}}")

print(f"Pipeline catalog: {catalog}")
print(f"Pipeline schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Instructions
# MAGIC 
# MAGIC Before running this pipeline, ensure the following volumes exist:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Create volumes for data storage
# MAGIC CREATE VOLUME IF NOT EXISTS {{.catalog}}.automated_quotes.raw_data;
# MAGIC CREATE VOLUME IF NOT EXISTS {{.catalog}}.automated_quotes.checkpoint;
# MAGIC ```
# MAGIC 
# MAGIC To set up the data generation:
# MAGIC 1. Run the `generate_quote_data_to_volume()` function manually to create initial data
# MAGIC 2. Set up a Databricks job to call this function periodically for new data
# MAGIC 3. The autoloader will automatically pick up new files and process them

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation with AI_GEN
# MAGIC 
# MAGIC Generate realistic automated quote data using Databricks AI_GEN function and save as JSONL text files to volume

# COMMAND ----------

def setup_volumes():
    """
    Create the required volumes for automated quote data processing.
    Run this function once before using the pipeline.
    """
    try:
        # Create schema if it doesn't exist
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}.automated_quotes")
        
        # Create volumes for data storage and checkpointing
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.automated_quotes.raw_data")
        #spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.automated_quotes.checkpoint")
        
        print("✅ Volumes created successfully:")
        print(f"   📁 {catalog}.{schema}.automated_quotes.raw_data")
        print(f"   📁 {catalog}.{schema}.automated_quotes.checkpoint")
        
        # Show existing volumes
        print("\n📋 Existing volumes in automated_quotes schema:")
        volumes_df = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}.automated_quotes")
        volumes_df.show()
        
    except Exception as e:
        print(f"❌ Error creating volumes: {e}")
        print("💡 Make sure you have the necessary permissions to create volumes")

# COMMAND ----------

def generate_quote_data_to_volume():
    """
    Generate realistic automated quote data using ai_gen and save to Databricks volume.
    
    Usage:
    1. Run this function manually to generate initial data
    2. Set up a scheduled job to generate new data periodically
    3. The data will be automatically picked up by the autoloader in bronze_automated_quotes
    """
    
    # Define the AI prompt for generating realistic quote data
    ai_prompt = """Generate realistic automated quote data for an industrial automation company (Emerson Electric). 
    Create exactly 3 JSON objects, one per line (JSONL format), with the following fields and realistic values:

    Rules for each JSON object:
    - id: Quote ID in format QT-XXX (e.g.,QT-<random number>)
    - customer_id: Customer ID in format CUST-XXX (e.g.,CUST-<random number>) 
    - customer_name: Industrial company names like "Manufacturing Plant A", "Chemical Processing Corp", "Power Generation Facility"
    - location: Specific facility locations like "Building 3 Floor 2 Detroit MI", "Reactor Room Houston TX", "Pump Station Phoenix AZ"
    - product_id: Product codes like 3051S-CP, 3051S-IL, 3051S-MV
    - product_description: Detailed Rosemount transmitter descriptions
    - quantity: Random integers between 1-10
    - unit_price: Decimal prices between 2000.00-5000.00
    - total_price: quantity * unit_price (calculated)
    - order_date: Dates within next 30 days in YYYY-MM-DD format
    - status: One of Pending, Approved, Denied
    - priority: One of High, Medium, Low
    - email_source: Professional email addresses matching company domain. Occasionally inject "zach.jacobson@databricks.com"
    - email_subject: Professional quote request subjects
    - email_body: "Please quote options for Transmitter codes 1J, 1K, and 1L"
    - email_received_at: Timestamps within last 24 hours in YYYY-MM-DD HH:MM:SS format
    - assigned_reviewer: Email addresses like sarah.johnson@emerson.com

    Return only the JSONL data (one JSON object per line), no additional text or formatting."""
        
    try:
        # Generate data using ai_gen
        df_generated = spark.sql(f"""
            SELECT ai_gen('{ai_prompt}') as generated_quote_data
        """)
        
        # Collect the generated JSONL content
        generated_content = df_generated.collect()[0]['generated_quote_data']
        
        # Write the generated data to volume
        volume_path = f"/Volumes/{catalog}/{schema}/automated_quotes/raw_data/"
        
        # Create a timestamp for unique file naming
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create DataFrame with the generated JSONL content
        text_df = spark.createDataFrame([(generated_content,)], ["value"])
        
        # Write as text file
        (text_df
         .coalesce(1)
         .write
         .mode("append")
         .text(f"{volume_path}/quotes_{timestamp}.txt"))
        
        # Parse and display sample of generated data for verification
        try:
            import json
            lines = generated_content.strip().split('\n')
            json_objects = [json.loads(line) for line in lines if line.strip()]
            
            print(f"✅ Generated {len(json_objects)} quote records saved to {volume_path}")
            print(f"📁 File: quotes_{timestamp}.txt")
            
            # Display sample of generated data
            print("📋 Sample generated data:")
            sample_df = spark.createDataFrame(json_objects[:3])
            sample_df.show(3, truncate=False)
            
        except Exception as parse_error:
            print(f"⚠️ Data generated but couldn't parse for preview: {parse_error}")
            print(f"✅ Generated data saved to {volume_path}/quotes_{timestamp}.txt")
        
    except Exception as e:
        print(f"❌ Error generating quote data: {e}")
        print("💡 Fallback: Using static sample data generation")
        



# USAGE INSTRUCTIONS:
# Step 1: Set up volumes (run once): setup_volumes()
# Step 2: Generate initial data: generate_quote_data_to_volume()
# Step 3: Run the DLT pipeline to process the data
# Step 4: Set up automatic data generation with a scheduled job
#
# DATA FORMAT: The system generates text files containing quote data that can be in various
# formats (JSONL, CSV, plain text, etc.). The AI extraction approach is flexible and can
# handle different text formats without requiring strict JSON structure.
#
# AUTOLOADER: The bronze layer uses autoloader with TEXT format to read files, then applies
# ai_query with structured JSON schema to extract quote data reliably from any text format.

# Uncomment the lines below to set up and generate data immediately:
setup_volumes()
generate_quote_data_to_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - AI-Powered Text Data Extraction

# COMMAND ----------

@dlt.table(
    name="bronze_automated_quotes",
    comment="Raw automated quote data ingestion from text files using AI extraction with autoloader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_automated_quotes():
    """
    Ingest raw automated quote data from text files using autoloader and ai_query for structured extraction.
    Data is generated using ai_gen function and stored as text files in Databricks volume.
    """
    # Define the volume path where AI-generated quote data is stored
    volume_path = f"/Volumes/{catalog}/{schema}/automated_quotes/raw_data/"
    
    # Read text files using autoloader
    raw_df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("cloudFiles.schemaLocation", f"/Volumes/{catalog}/{schema}/automated_quotes/raw_data/_checkpoints")
            .load(volume_path)
    )
    
    # Use ai_query to extract structured data from text content
    df = (
        raw_df
        .withColumn("extracted_data", 
            F.expr(f"""
                {catalog}.{schema}.parse_email(value)
            """)
        )
        # Extract individual fields from the JSON response
        .withColumn("id", F.get_json_object("extracted_data", "$.id"))
        .withColumn("customer_id", F.get_json_object("extracted_data", "$.customer_id"))
        .withColumn("customer_name", F.get_json_object("extracted_data", "$.customer_name"))
        .withColumn("location", F.get_json_object("extracted_data", "$.location"))
        .withColumn("product_id", F.get_json_object("extracted_data", "$.product_id"))
        .withColumn("product_description", F.get_json_object("extracted_data", "$.product_description"))
        .withColumn("quantity", F.get_json_object("extracted_data", "$.quantity").cast("integer"))
        .withColumn("unit_price", F.get_json_object("extracted_data", "$.unit_price").cast("decimal(10,2)"))
        .withColumn("total_price", F.get_json_object("extracted_data", "$.total_price").cast("decimal(10,2)"))
        .withColumn("order_date", F.to_date(F.get_json_object("extracted_data", "$.order_date")))
        .withColumn("status", F.get_json_object("extracted_data", "$.status"))
        .withColumn("priority", F.get_json_object("extracted_data", "$.priority"))
        .withColumn("email_source", F.get_json_object("extracted_data", "$.email_source"))
        .withColumn("email_subject", F.get_json_object("extracted_data", "$.email_subject"))
        .withColumn("email_body", F.get_json_object("extracted_data", "$.email_body"))
        .withColumn("email_received_at", F.to_timestamp(F.get_json_object("extracted_data", "$.email_received_at")))
        .withColumn("assigned_reviewer", F.get_json_object("extracted_data", "$.assigned_reviewer"))
        # Add processing metadata
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .withColumn("_bronze_ingestion_time", F.current_timestamp())
        .withColumn("_raw_text", F.col("value"))  # Keep original text for debugging
        # Drop intermediate columns
        .drop("value", "extracted_data")
    )
    return df

@dlt.table(
    name="bronze_quote_notes",
    comment="Raw quote notes and reviewer comments",
    table_properties={
        "quality": "bronze", 
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_quote_notes():
    """
    Ingest raw quote notes from reviewer updates and approval workflow.
    """
    df = (
        spark.readStream
            .format("rate-micro-batch")
            .option("rowsPerBatch", 3)
            .load()
            .withColumn("id", F.col("value"))
            .withColumn("quote_id", F.concat(F.lit("QT-"), F.lpad(F.col("value") % 100, 3, "0")))
            .withColumn("content", 
                F.when(F.col("value") % 4 == 0, "Quote pricing verified and approved")
                .when(F.col("value") % 4 == 1, "Customer credit check completed")
                .when(F.col("value") % 4 == 2, "Product availability confirmed")
                .otherwise("Additional technical review required"))
            .withColumn("note_type",
                F.when(F.col("value") % 4 == 0, "Approval")
                .when(F.col("value") % 4 == 1, "Comment")
                .when(F.col("value") % 4 == 2, "Comment")
                .otherwise("Revision"))
            .withColumn("reviewer", F.lit("Sarah Johnson"))
            .withColumn("created_at", F.col("timestamp"))
            .select("id", "quote_id", "content", "note_type", "reviewer", "created_at")
    )
    return df

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from CRM and email parsing",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_customers():
    """
    Ingest customer data from CRM systems and email parsing.
    """
    df = (
        spark.readStream
            .format("rate-micro-batch")
            .option("rowsPerBatch", 3)
            .load()
            .withColumn("customer_id", F.concat(F.lit("CUST-"), F.lpad((F.col("value") % 10) + 1, 3, "0")))
            .withColumn("company_name",
                F.when(F.col("value") % 3 == 0, "Manufacturing Plant A")
                .when(F.col("value") % 3 == 1, "Chemical Processing Corp")
                .otherwise("Power Generation Facility"))
            .withColumn("contact_person",
                F.when(F.col("value") % 3 == 0, "John Doe")
                .when(F.col("value") % 3 == 1, "Jane Smith")
                .otherwise("Bob Wilson"))
            .withColumn("email",
                F.when(F.col("value") % 3 == 0, "john.doe@manufacturingplanta.com")
                .when(F.col("value") % 3 == 1, "jane.smith@chemproc.com")
                .otherwise("bob.wilson@powergen.com"))
            .withColumn("phone",
                F.when(F.col("value") % 3 == 0, "555-0101")
                .when(F.col("value") % 3 == 1, "555-0202")
                .otherwise("555-0303"))
            .withColumn("address",
                F.when(F.col("value") % 3 == 0, "123 Industrial Blvd, Detroit, MI")
                .when(F.col("value") % 3 == 1, "456 Chemical Lane, Houston, TX")
                .otherwise("789 Power Plant Rd, Phoenix, AZ"))
            .withColumn("customer_created_at", F.col("timestamp"))
            .select("customer_id", "company_name", "contact_person", "email", "phone", "address", "customer_created_at")
    )
    return df

@dlt.table(
    name="bronze_products",
    comment="Raw product catalog data from ERP systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_products():
    """
    Ingest product catalog data from ERP and inventory systems.
    """
    df = (
        spark.readStream
            .format("rate-micro-batch")
            .option("rowsPerBatch", 3)
            .load()
            .withColumn("product_id",
                F.when(F.col("value") % 3 == 0, "3051S-CP")
                .when(F.col("value") % 3 == 1, "3051S-IL")
                .otherwise("3051S-MV"))
            .withColumn("product_name",
                F.when(F.col("value") % 3 == 0, "Rosemount 3051S Coplanar Pressure Transmitter")
                .when(F.col("value") % 3 == 1, "Rosemount 3051S In-line Pressure Transmitter")
                .otherwise("Rosemount 3051S MultiVariable Transmitter"))
            .withColumn("description",
                F.when(F.col("value") % 3 == 0, "Industry-leading coplanar pressure transmitter with advanced diagnostics and superior accuracy")
                .when(F.col("value") % 3 == 1, "Versatile in-line pressure transmitter with 4-20mA HART output, 0-6000 PSI range")
                .otherwise("Advanced multivariable transmitter measuring pressure, differential pressure, and temperature"))
            .withColumn("category",
                F.when(F.col("value") % 3 == 0, "Pressure Instruments")
                .when(F.col("value") % 3 == 1, "Pressure Instruments")
                .otherwise("Pressure Instruments"))
            .withColumn("unit_price",
                F.when(F.col("value") % 3 == 0, 3250.00)  # Coplanar model - premium pricing
                .when(F.col("value") % 3 == 1, 2850.00)   # In-line model - standard pricing
                .otherwise(4750.00))                      # MultiVariable - highest price
            .withColumn("availability_status", F.lit("Available"))
            .withColumn("product_created_at", F.col("timestamp"))
            .select("product_id", "product_name", "description", "category", "unit_price", "availability_status", "product_created_at")
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned and Validated Quote Data

# COMMAND ----------

@dlt.table(
    name="silver_automated_quotes",
    comment="Cleaned and validated automated quote data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_quote_id", "id IS NOT NULL AND id != ''")
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL AND customer_id != ''")
@dlt.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND product_id != ''")
@dlt.expect_or_fail("valid_priority", "priority IN ('High', 'Medium', 'Low')")
@dlt.expect_or_fail("valid_status", "status IN ('Pending', 'Approved', 'Denied', 'Delivered')")
@dlt.expect_or_fail("valid_quantity", "quantity > 0")
@dlt.expect_or_fail("valid_prices", "unit_price > 0 AND total_price > 0")
@dlt.expect("valid_customer_name", "customer_name IS NOT NULL AND customer_name != ''")
@dlt.expect("valid_location", "location IS NOT NULL AND location != ''")
@dlt.expect("valid_email_source", "email_source IS NOT NULL AND email_source != ''")
def silver_automated_quotes():
    """
    Clean and validate automated quote data.
    Apply business rules and data quality checks.
    """
    return (
        dlt.read("bronze_automated_quotes")
        .filter(F.col("id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("priority_score", 
            F.when(F.col("priority") == "High", 3)
            .when(F.col("priority") == "Medium", 2)
            .otherwise(1))
        .withColumn("days_to_order", 
            F.datediff(F.col("order_date"), F.current_date()))
        .withColumn("processing_age_hours", 
            F.round((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("email_received_at"))) / 3600, 2))
        .withColumn("is_urgent",
            F.when((F.col("priority") == "High") & (F.col("days_to_order") <= 3), True)
            .when((F.col("priority") == "Medium") & (F.col("days_to_order") <= 1), True)
            .otherwise(False))
        .withColumn("quote_value_tier",
            F.when(F.col("total_price") >= 10000, "Large")
            .when(F.col("total_price") >= 5000, "Medium")
            .otherwise("Small"))
        .withColumn("price_verification", 
            F.when(F.abs(F.col("total_price") - (F.col("quantity") * F.col("unit_price"))) < 0.01, "Verified")
            .otherwise("Error"))
    )

@dlt.table(
    name="silver_quote_notes",
    comment="Cleaned and validated quote notes with enrichments",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_note_id", "id IS NOT NULL")
@dlt.expect_or_fail("valid_quote_ref", "quote_id IS NOT NULL AND quote_id != ''")
@dlt.expect_or_fail("valid_note_type", "note_type IN ('Comment', 'Approval', 'Denial', 'Revision')")
@dlt.expect("valid_content", "content IS NOT NULL AND length(content) > 0")
@dlt.expect("valid_reviewer", "reviewer IS NOT NULL AND reviewer != ''")
def silver_quote_notes():
    """
    Clean and validate quote notes data.
    """
    return (
        dlt.read("bronze_quote_notes")
        .filter(F.col("id").isNotNull())
        .filter(F.col("quote_id").isNotNull())
        .filter(F.col("content").isNotNull())
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("content_length", F.length(F.col("content")))
        .withColumn("note_sentiment",
            F.when(F.lower(F.col("content")).rlike("approv|accept|confirm|good"), "positive")
            .when(F.lower(F.col("content")).rlike("den|reject|concern|issue|problem"), "negative")
            .otherwise("neutral"))
        .withColumn("contains_pricing", F.lower(F.col("content")).contains("price"))
        .withColumn("contains_timeline", F.lower(F.col("content")).rlike("date|schedule|deadline|urgent"))
    )

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data with enrichments",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL AND customer_id != ''")
@dlt.expect_or_fail("valid_company_name", "company_name IS NOT NULL AND company_name != ''")
@dlt.expect("valid_email", "email IS NOT NULL AND email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
@dlt.expect("valid_phone", "phone IS NOT NULL AND phone != ''")
def silver_customers():
    """
    Clean and validate customer data.
    """
    return (
        dlt.read("bronze_customers")
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("company_name").isNotNull())
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("email_domain", F.split(F.col("email"), "@").getItem(1))
        .withColumn("state", F.split(F.col("address"), ", ").getItem(1))
        .withColumn("customer_tier",
            F.when(F.lower(F.col("company_name")).contains("manufacturing"), "Industrial")
            .when(F.lower(F.col("company_name")).contains("chemical"), "Process")
            .when(F.lower(F.col("company_name")).contains("power"), "Utility")
            .otherwise("Other"))
        .withColumnRenamed("customer_created_at", "created_at")
    )

@dlt.table(
    name="silver_products",
    comment="Cleaned and validated product catalog data with enrichments",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_product_id", "product_id IS NOT NULL AND product_id != ''")
@dlt.expect_or_fail("valid_product_name", "product_name IS NOT NULL AND product_name != ''")
@dlt.expect_or_fail("valid_unit_price", "unit_price > 0")
@dlt.expect("valid_category", "category IS NOT NULL AND category != ''")
@dlt.expect("valid_availability", "availability_status IN ('Available', 'Limited', 'Backordered', 'Discontinued')")
def silver_products():
    """
    Clean and validate product data.
    """
    return (
        dlt.read("bronze_products")
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("product_name").isNotNull())
        .filter(F.col("unit_price") > 0)
        .withColumn("processed_timestamp", F.current_timestamp())
        .withColumn("price_tier",
            F.when(F.col("unit_price") >= 4000, "Premium")
            .when(F.col("unit_price") >= 2000, "Standard")
            .otherwise("Economy"))
        .withColumn("product_type",
            F.when(F.lower(F.col("product_name")).contains("coplanar"), "Pressure_Coplanar")
            .when(F.lower(F.col("product_name")).contains("in-line"), "Pressure_Inline")
            .when(F.lower(F.col("product_name")).contains("multivariable"), "Pressure_MultiVariable")
            .when(F.lower(F.col("product_name")).contains("transmitter"), "Pressure_Instrument")
            .otherwise("Other"))
        .withColumn("description_length", F.length(F.col("description")))
        .withColumnRenamed("product_created_at", "created_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Final Quote Management Tables (1:1 with Database Schema)

# COMMAND ----------

@dlt.table(
    name="automated_quotes",
    comment="Final automated quotes table matching database schema exactly",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def automated_quotes():
    """
    Create final automated_quotes table that matches the database schema exactly.
    This aggregates and finalizes quote data from silver layer.
    """
    return (
        dlt.read("silver_automated_quotes")
        .select(
            F.col("id"),
            F.col("customer_id"),
            F.col("customer_name"),
            F.col("location"),
            F.col("product_id"),
            F.col("product_description"),
            F.col("quantity"),
            F.col("unit_price").cast("decimal(10,2)"),
            F.col("total_price").cast("decimal(10,2)"),
            F.col("order_date").cast("date"),
            F.col("status"),
            F.col("priority"),
            F.col("email_source"),
            F.col("email_subject"),
            F.col("email_body"),
            F.col("email_received_at").cast("timestamp"),
            F.col("created_at").cast("timestamp"),
            F.col("assigned_reviewer"),
            F.col("updated_at").cast("timestamp")
        )
        .filter(F.col("price_verification") == "Verified")  # Only include verified quotes
    )

@dlt.table(
    name="quote_notes",
    comment="Final quote notes table matching database schema exactly",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def quote_notes():
    """
    Create final quote_notes table that matches the database schema exactly.
    """
    return (
        dlt.read("silver_quote_notes")
        .select(
            F.col("quote_id"),
            F.col("content"),
            F.col("note_type"),
            F.col("created_at").cast("timestamp"),
            F.col("reviewer")
        )
        .filter(F.col("content_length") > 5)  # Only include meaningful notes
    )

@dlt.table(
    name="customers",
    comment="Final customers table matching database schema exactly",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def customers():
    """
    Create final customers table that matches the database schema exactly.
    This deduplicates and finalizes customer data from silver layer.
    """
    return (
        dlt.read("silver_customers")
        .select(
            F.col("customer_id"),
            F.col("company_name"),
            F.col("contact_person"),
            F.col("email"),
            F.col("phone"),
            F.col("address"),
            F.col("created_at").cast("timestamp"),
            # Include computed columns for analytics
            F.col("email_domain"),
            F.col("customer_tier")
        )
        .dropDuplicates(["customer_id"])  # Ensure unique customers
    )

@dlt.table(
    name="products",
    comment="Final products table matching database schema exactly",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def products():
    """
    Create final products table that matches the database schema exactly.
    This deduplicates and finalizes product catalog data from silver layer.
    """
    return (
        dlt.read("silver_products")
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("description"),
            F.col("category"),
            F.col("unit_price").cast("decimal(10,2)"),
            F.col("availability_status"),
            F.col("created_at").cast("timestamp"),
            # Include computed columns for analytics
            F.col("price_tier"),
            F.col("product_type")
        )
        .dropDuplicates(["product_id"])  # Ensure unique products
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views for Quote Management Analytics

# COMMAND ----------

@dlt.table(
    name="quote_dashboard_view",
    comment="Real-time dashboard view for quote management and approval workflow"
)
def quote_dashboard_view():
    """
    Create a consolidated view for quote management dashboard consumption.
    """
    quotes_with_notes = (
        dlt.read("automated_quotes").alias("aq")
        .join(
            dlt.read("quote_notes").alias("qn")
            .groupBy("quote_id")
            .agg(
                F.count("*").alias("note_count"),
                F.max("created_at").alias("last_note_date"),
                F.sum(F.when(F.col("note_type") == "Approval", 1).otherwise(0)).alias("approval_count"),
                F.sum(F.when(F.col("note_type") == "Denial", 1).otherwise(0)).alias("denial_count")
            ).alias("notes_agg"),
            F.col("aq.id") == F.col("notes_agg.quote_id"),
            "left"
        )
    )
    
    return (
        quotes_with_notes
        .filter(F.col("aq.created_at") >= F.current_date() - 30)  # Last 30 days
        .select(
            F.col("aq.id"),
            F.col("aq.customer_name"),
            F.col("aq.product_id"),
            F.col("aq.total_price"),
            F.col("aq.status"),
            F.col("aq.priority"),
            F.col("aq.assigned_reviewer"),
            F.datediff(F.current_date(), F.col("aq.order_date")).alias("days_to_order"),
            F.round((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("aq.email_received_at"))) / 3600, 1).alias("processing_hours"),
            F.coalesce("notes_agg.note_count", F.lit(0)).alias("total_notes"),
            F.coalesce("notes_agg.approval_count", F.lit(0)).alias("approvals"),
            F.coalesce("notes_agg.denial_count", F.lit(0)).alias("denials"),
            F.col("aq.created_at"),
            F.col("aq.order_date")
        )
        .orderBy(F.desc("aq.created_at"), "aq.priority", "aq.total_price")
    )

@dlt.table(
    name="product_performance_view", 
    comment="Product sales performance and quote conversion analysis"
)
def product_performance_view():
    """
    Product performance analysis for sales and inventory planning.
    """
    return (
        dlt.read("automated_quotes").alias("aq")
        .join(dlt.read("products").alias("p"), F.col("aq.product_id") == F.col("p.product_id"), "left")
        .groupBy(
            F.col("aq.product_id"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.date_trunc("month", F.col("aq.created_at")).alias("month")
        )
        .agg(
            F.count("*").alias("total_quotes"),
            F.sum(F.when(F.col("aq.status") == "Approved", 1).otherwise(0)).alias("approved_quotes"),
            F.sum(F.when(F.col("aq.status") == "Denied", 1).otherwise(0)).alias("denied_quotes"),
            F.sum(F.when(F.col("aq.status") == "Pending", 1).otherwise(0)).alias("pending_quotes"),
            F.sum("aq.quantity").alias("total_quantity_quoted"),
            F.sum(F.when(F.col("aq.status") == "Approved", F.col("aq.total_price")).otherwise(0)).alias("approved_revenue"),
            F.sum("aq.total_price").alias("total_quoted_value"),
            F.avg("aq.total_price").alias("avg_quote_value")
        )
        .withColumn("approval_rate",
            F.round(F.col("approved_quotes") / F.col("total_quotes") * 100, 2))
        .filter(F.col("month") >= F.add_months(F.current_date(), -6))  # Last 6 months
        .orderBy(F.desc("total_quoted_value"), F.desc("approval_rate"))
    )

@dlt.table(
    name="customer_analytics_view",
    comment="Customer quote patterns and relationship analysis"
)
def customer_analytics_view():
    """
    Customer analytics for account management and sales forecasting.
    """
    customer_quotes = (
        dlt.read("automated_quotes").alias("aq")
        .join(dlt.read("customers").alias("c"), F.col("aq.customer_id") == F.col("c.customer_id"), "left")
        .groupBy(
            F.col("aq.customer_id"),
            F.col("c.company_name"),
            F.col("c.customer_tier"),
            F.col("c.email_domain")
        )
        .agg(
            F.count("*").alias("total_quotes"),
            F.sum(F.when(F.col("aq.status") == "Approved", 1).otherwise(0)).alias("approved_quotes"),
            F.sum(F.when(F.col("aq.status") == "Approved", F.col("aq.total_price")).otherwise(0)).alias("total_revenue"),
            F.sum("aq.total_price").alias("total_quoted_value"),
            F.avg("aq.total_price").alias("avg_quote_value"),
            F.min("aq.created_at").alias("first_quote_date"),
            F.max("aq.created_at").alias("last_quote_date"),
            F.countDistinct("aq.product_id").alias("unique_products_quoted")
        )
        .withColumn("approval_rate",
            F.round(F.col("approved_quotes") / F.col("total_quotes") * 100, 2))
        .withColumn("days_as_customer",
            F.datediff(F.col("last_quote_date"), F.col("first_quote_date")))
    )
    
    return (
        customer_quotes
        .filter(F.col("total_quotes") >= 1)
        .select(
            "customer_id",
            "company_name", 
            "customer_tier",
            "total_quotes",
            "approved_quotes",
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_quote_value", 2).alias("avg_quote_value"),
            F.round("approval_rate", 1).alias("approval_rate_pct"),
            "unique_products_quoted",
            "days_as_customer",
            "first_quote_date",
            "last_quote_date"
        )
        .orderBy(F.desc("total_revenue"), F.desc("approval_rate_pct"))
    )

@dlt.table(
    name="reviewer_performance_view",
    comment="Quote reviewer performance and workload analysis"
)
def reviewer_performance_view():
    """
    Reviewer performance analysis for workflow optimization.
    """
    return (
        dlt.read("automated_quotes").alias("aq")
        .groupBy(
            F.col("aq.assigned_reviewer"),
            F.date_trunc("week", F.col("aq.created_at")).alias("week")
        )
        .agg(
            F.count("*").alias("total_quotes_assigned"),
            F.sum(F.when(F.col("aq.status") == "Approved", 1).otherwise(0)).alias("approved_quotes"),
            F.sum(F.when(F.col("aq.status") == "Denied", 1).otherwise(0)).alias("denied_quotes"),
            F.sum(F.when(F.col("aq.status") == "Pending", 1).otherwise(0)).alias("pending_quotes"),
            F.sum(F.when(F.col("aq.priority") == "High", 1).otherwise(0)).alias("high_priority_quotes"),
            F.avg(F.datediff(F.current_date(), F.col("aq.created_at"))).alias("avg_quote_age_days"),
            F.sum("aq.total_price").alias("total_value_reviewed")
        )
        .withColumn("approval_rate",
            F.round(F.col("approved_quotes") / (F.col("approved_quotes") + F.col("denied_quotes")) * 100, 2))
        .withColumn("processing_efficiency",
            F.round((F.col("approved_quotes") + F.col("denied_quotes")) / F.col("total_quotes_assigned") * 100, 2))
        .filter(F.col("week") >= F.date_trunc("week", F.current_date()) - F.expr("INTERVAL 8 WEEKS"))
        .orderBy(F.desc("week"), "assigned_reviewer")
    )