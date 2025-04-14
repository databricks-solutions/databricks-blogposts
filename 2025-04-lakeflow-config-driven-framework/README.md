# Configuration driven pipelines / workflows

Our goal was to implement the below workflow using a config-driven framework

![Pipeline Architecture Diagram](/2025-04-lakeflow-config-driven-framework/Images/pipeline_design.png)

## Requirements
   * Incremental ingestion from cloud storage into the bronze layer
   * The source data is semi-structured JSON
   * Schema evolution is an absolute requirement
      * Adding of new columns
      * Data type enhancement
      * New columns should automatically be added to the merge tables
   * Change data feed (CDF) is present only at the top parent node. 
      * Child nodes can be any level deep in the JSON
      * Need to create separate tables for the child nodes and update them incrementally
      * So change data feed needs to be built for child nodes
   * Need SCD Type 1 merge in bronze layer
      * Bronze layer contains mirrored tables from source systems
      * So bronze layer contains both append only tables & final merge tables

   * Need SCD Type 2 merge in the silver layer
      * Silver layer contains joins of bronze tables. Hence silver layer does not have a direct change data feed
      * Change data feed needs to be built for silver layer
   * The framework should be extensible & scalable
   * Cloud storage contains data for several tenants. This is a typical requirement for any SAAS company
   * Automatic onboarding of new tenant's data 
   * CI-CD pipeline deployment is needed as part of the developer's best practices 


## Challenges and mitigations
* We wanted to handle the below complexities in the bronze layer itself when the ingestion is happening in append only mode
  * Schema evolution
  * Generate the change data feed for the child nodes

* We started with the DLT because of the following inherent capabilities
  * DLT can incrementally read data from cloud storage using autoloader
  * DLT has the inbuilt schema inference & schema evolution capabilities
  * DLT apply_changes is very powerful and has the below capabilities
    * Schema evolution. When a new column is added to the source table, apply_changes propagates the new column to target table
    * Supports both SCD Type 1 & Type 2 merge

* But we also had the below challenges with DLT
    * One of the source column is coming as binary format
      * This is always the case when data is coming from Kafka streams
      * DLT cannot infer schema on a binary column
      * This binary column first needed to be converted to JSON string
    * from_json() needs to infer & evolve the schema from the JSON string incrementally
      * The schema evolution of the from_json() was still in private preview at the time of this writing
      * Implemented the schema evolution functionality in a notebook using spark streaming & foreachbatch using custom python UDF
    * This child nodes were array elements and did not have a primary key
      * But the tables created from child nodes need to be updated incrementally
      * Implemented change data feed functionality for child nodes in a notebook using spark streaming & foreachbatch
    * Silver layer tables are created by joining multiple bronze tables and hence a direct change data feed is not available
      * Implemented change data feed needed to incrementally update the silver tables in a notebook using spark streaming & foreachbatch

# The framework details
* We built the framework by interweaving the powerful capabilities of both DLT & spark streaming and Implemented the below pipeline

![alt text](/2025-04-lakeflow-config-driven-framework//Images/Blogathon_workflow.png)

## Implementation details
* This is a complex pipeline and hence needed 4 config tables to drive the framework
* For the demonstration, the code follows the below catalog & schema setup
    * dbx is  the catalog
    * bronze schema is the ingestion layer
    * silver schema is the curated layer
    * metadata schema contains the config tables
      * config_bronze_raw
      * config_bronze_childnodes_raw
      * config_bronze
      * config_silver
        
![alt text](/2025-04-lakeflow-config-driven-framework//Images/schema.png)

## Code generator notebook
* When a DLT was reading directly from the config tables, debugging became difficult. 
  * So implemented an intermediate notebook generator that reads the config tables and generates the underlying code for DLT 
  * the code generator notebook is @ /2025-04-lakeflow-config-driven-framework//raw_notebook_generator.py

## Deep dive into each of the config tables
  * **config_bronze_raw**
    * This table drives the ingestion into the bronze layer
    * Bronze tables again are divided into
      * Raw1 tables - pre-schema inferred tables
      * Raw2 tables - post-schema inferred tables with all the columns
      * Raw3 tables - post-schema inferred tables with a select on subset of the raw2 columns
      * Merge tables
    * The DDL & INSERT scripts for this config table is @ /2025-04-lakeflow-config-driven-framework//DDLs/1_config_bronze_raw.sql
    * Below is the schema and sample data
      
![alt text](/2025-04-lakeflow-config-driven-framework//Images/config_bronze_raw.png)

    * Lets deep dive into the schema
      * **autoloader_path** column
        * The ingestion data is sometimes stored in more than one directory paths on the cloud storage bucket. 
        * So a **MAP** data type column was used for the autoloader paths
        * Used DLT append flows to read data from these multiple directories into raw1 tables
        * data_column is an important column that is coming as binary and contains 90% of the actual data. 
          * All raw1 tables contain data_column. 
          * data_column is converted to JSON string. 
        * DLT schema evolution is applied on all the remaining columns except data_column
        * All the raw1 tables in a single workflow are ingested in a single DLT
        * The DLT generated is @ /2025-04-lakeflow-config-driven-framework//inventory_pipeline_raw1.py
    * **sch_inf_notebook, sch_inf_notebook_params** columns
      * The schema needs to be inferred & evolved for data_column in all the raw1 tables
      * Databricks sql has from_json() which can infer & evolve schema on an incremental stream. 
      * But this feature was in  private preview during the implementation and hence we did not recommend it for the customer’s production scenarios
      * Implemented a custom python UDF in  spark streaming & foreachbatch to incremental evolve schema
      * The notebook is @ /2025-04-lakeflow-config-driven-framework//ntb_merge_schema.py
      * The same notebook is used to generate the change data feed for each raw1 table with different parameters
      * The final eveolved schema for each raw1 table is upserted into the schem_registry delta table 
    * **selectExpr** column
      * The data type is **array<map<string,string>>** 
      * The second DLT in the workflow is defined on both raw2, raw3 bronze tables
      * There is a corresponding raw2 & raw3 table for each of the raw1 tables
      * The schema from the schema_registry table is applied on the data_column on the incremental records flowing into raw2 table
      * raw3 table is a select expression defined on the raw2 table with the needed subset columns. 
      * The DLT generated is @ /2025-04-lakeflow-config-driven-framework//inventory_pipeline_raw2_raw3.py

  * The second config table in the framework is **config_bronze_childnodes_raw**
    * The DDL & INSERT scripts are @ /2025-04-lakeflow-config-driven-framework//DDLs/2_config_bronze_childnodes_raw.sql
    * This is an important table as the data_column in all raw2 tables is a JSON struct
      * Many use cases will need to explode the child nodes at very different levels
      * The child nodes can be deeply nested up-to 10+ levels
    * Below is the structure of the config table and sample data
      
  ![alt text](/2025-04-lakeflow-config-driven-framework//Images/config_bronze_childnodes_raw.png)  

    * Lets deep dive in to the schema
      * **selectExpr** column
      * This column data type is an **array<map<string,array<string>>>**
      * A map data type will have the select expression order number as the key & fields in select expression as the array
        * A JSON child node can be very deep and will need multiple select statements to be executed in an order.
        * Select expression order number will provide the order for the execution
        * The child node is flattened into a child table using these multiple select expressions
          * The child tables created also have the suffix raw3
        * The DLT generated is @ /2025-04-lakeflow-config-driven-framework//inventory_pipeline_raw2_raw3.py

  * The third config table is **config_bronze**
    * The notebook with DDL & INSERT scripts is @ /2025-04-lakeflow-config-driven-framework//DDLs/3_config_bronze.sql
    * Below is the structure of the config table and sample data
      
![alt text](/2025-04-lakeflow-config-driven-framework//Images/config_bronze.png)

    * This config table implements a DLT apply_chages for all  the bronze tables
      * DLT apply_changes needs a change data feed
      * But child tables that are generally created by exploding the nested child arrays in the JSON will not have a proper change data feed
      * Lets deep dive into the schema
        * **cdf_notebook, cdf_notebook_params** columns
          * A notebook is built to create change data feed on the child tables using spark streaming & foreachbatch
          * The notebook is @ /2025-04-lakeflow-config-driven-framework//ntb_cdf_creator_bronze.py
          * The same notebook is used to generate the change data feed for each child table but with different parameters
        * **primary_key, sequence_by, delete_expr, except_column_list, scd_type**
          * These columns are useful to create the DLT apply_changes on target table for each raw3 table
          * Even the child tables will have their respective apply_changes target table
          * The final DLT generated is @ /2025-04-lakeflow-config-driven-framework//ntb_cdf_creator_bronze.py
          * scd_type column allows us to dynamically change the merge type to SCD Type 1 Or SCD Type 2 
  * The fourth and the last config table is **config_silver**
    * The notebook with DDL & INSERT scripts is @ /2025-04-lakeflow-config-driven-framework//DDLs/4_config_silver.sql
    * Below is the structure of the config table and sample data
      
![alt text](/2025-04-lakeflow-config-driven-framework//Images/config_silver.png) 

    * Silver tables are joins on multiple bronze tables
    * A view is built on this complex logic and hence lacks change data feed
    * Lets deep dive into the schema
    * **cdf_notebook, cdf_notebook_params** columns
      * A notebook is built to create the change data feed for the view
      * The notebook is @ /2025-04-lakeflow-config-driven-framework//ntb_cdf_creator_silver.py
      * The same notebook is called for all the silver views but with different parameters
    * **primary_key, sequence_by, delete_expr, except_column_list, scd_type** columns
      * These columns are useful to create the DLT apply_changes on target table for each silver view
      * The silver DLT generated is @ /2025-04-lakeflow-config-driven-framework//inventory_pipeline_silver_scd.py
      * DLT apply_changes used to merge the new / updated / deleted records from change data feed into final silver table
      * DLT apply_changes has inherent support for SCD Type 2 merge
* CI-CD best practices
  * Once all the DLTs are generated, the workflow needs to be deployed from github actions as part of CI-CD best practices
  * But the challenge with the config driven frameworks is the number of the tasks in the workflow is known only after reading the config driven tables
  * So we used databricks sdk for python, databricks-connect to deploy the workflow
    * databricks-connect will enable you to read the config tables
      *  databricks-connect==15.1.3 will enable you to use the databricks serverless cluster to read the config files
    *  The end-to-end deplyment is done by the  python script - [/Deployment/create_workflow.py](/2025-04-lakeflow-config-driven-framework/Deployment/create_workflow.py)


