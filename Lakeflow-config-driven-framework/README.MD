# Configuration driven pipelines

Our goal was to implement the below pipeline using a config-driven framework
![alt text](https://github.com/srinivasadmala/databricks-blogposts/blob/config_driven_framework_blogathon_fy26_h1/Lakeflow-config-driven-framework/Images/pipeline_design.png)

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
  * Generate the change data feed (CDF) for the child nodes
  * For covenience we called applend only tables as raw tables
    * We had 3 sets of raw tables - raw1, raw2, raw3
    * raw1 tables are pre-schema inferred tables
    * raw2 tables are post-schema inferred tables with all the columns
    * raw3 tables are post-schema inferred tables with a select on subset of the raw2 columns


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

## The framework details
We built the framework by interweaving the powerful capabilities of both DLT & spark streaming and Implemented the below pipeline 
![alt text](https://github.com/srinivasadmala/databricks-blogposts/blob/config_driven_framework_blogathon_fy26_h1/Lakeflow-config-driven-framework/Images/Blogathon_workflow.png)

##Implementation details
* This is a complex pipeline and hence needed 4 configuration tables

For the demonstration the code follows the below catalog & schema setup
Dbx is  the catalog
Bronze schema is the ingestion layer
Silver schema is the curated layer


Metadata contains the config tables that drive the framework
4 config tables - config_bronze_raw, config_bronze_childnodes_raw, config_bronze, config_silver - drive the framework 

Lets deep dive into each of these tables
First config table in the framework is config_bronze_raw
This table drives the ingestion into the bronze layer
Bronze tables again are divided into
Raw1 tables 
Raw2 tables
Raw3 tables
Final merge table
The DDL & INSERT scripts for this config table is @  https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1332008936163821?o=984752964297111#command/1332008936163822
Schema and sample data is below

Lets look into the highlighted columns
Brown highlighted - 
The ingestion data is sometimes stored in more than one directory path on an S3 bucket. So we stored these autoloader paths in a column with MAP data type so that multiple paths can be accommodated
Used DLT append flows to read data from these multiple directories into raw1 tables
Data_column is an important column that is coming as binary and contains 90% of the actual data. All raw1 tables contain data_column. Data_column is converted to JSON string. 
DLT schema evolution is applied on all the remaining columns except data_column
All the raw1 tables are ingested in a single DLT. 
The DLT generated is @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571881270?o=984752964297111#command/1412556571881292 
When DLT was reading directly from the config tables, debugging was difficult. So we developed an intermediate notebook generator  that reads the config tables and generates the underlying code for DLT https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1242762839975211?o=984752964297111#command/1242762839975510 
Green highlighted
The schema needs to be inferred & evolved for data_column in all the raw1 tables
Databricks sql has from_json() which can infer & evolve schema on an incremental stream. But this feature is in  private preview and we did not recommend it for the customer’s production scenarios
So we implemented a custom python UDF in  spark streaming & foreachbatch to incremental evolve schema
The notebook details are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571896213?o=984752964297111#command/1412556571896214
The same notebook is used to generate the CDF for each child table with different parameters
Blue-highlighted
Raw3 table is a select expression defined on the raw2 table with the needed columns. The number columns were as high as 150 in prod scenarios.
The DLT generated is @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571881274?o=984752964297111#command/1412556571881300 
When DLT was reading directly from the config tables, debugging was difficult. So we developed a notebook generator  that reads the config tables and generates the underlying code for DLT https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1242762839975211?o=984752964297111#command/1242762839975510 
The second config table in the framework is config_bronze_childnodes_raw
The DDL & INSERT scripts are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1332008936163802?o=984752964297111#command/1412556571883423 
This is an important table as the data_column in all raw2 tables is a JSON struct
Many use cases will need to explode the child nodes at very different levels
The child nodes can be deeply nested up-to 10+ levels
Below is the structure of the config table and sample data


Lets deep dive in to the brown highlighted column
This column data type is an array<map<string, array>>
A map data type will have the select expression order number as the key & fields in select expression as the array
A JSON child node can be very deep and will need multiple select statements to be executed in an order.
Select expression order number will provide the order for the execution
The DLT generated is @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571881274?o=984752964297111#command/1412556571881300 
When DLT was reading directly from the config tables, debugging was difficult. So we developed a notebook generator  that reads the config tables and generates the underlying code for DLT https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1242762839975211?o=984752964297111#command/1242762839975510 
The third config table is config_bronze
The DDL & INSERT scripts are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1332008936163836?o=984752964297111#command/1332008936267951
Below is the structure of the config table and sample data

This config table implements a DLT apply_chages for all  the bronze tables
But child tables that are generally created by exploding the nested child arrays will not have a proper CDF
Lets deep dive into the brown highlighted columns
A notebook is built to create CDF on the child tables using spark streaming & foreachbatch
The notebook details are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571896237?o=984752964297111#command/1412556571896239 
The same notebook is used to generate the CDF for each child table with different parameters
Lets deep dive into the green highlighted columns
The columns are useful to create the DLT apply_changes target table for each raw3 table
Even the child tables will have their respective apply_changes target table
The final DLT generated is @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571881286?o=984752964297111#command/1412556571881306 
The fourth and the last config table is config_silver
The DDL & INSERT scripts are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1332008936163813?o=984752964297111#command/1332008936163814 
Below is the structure of the config table and sample data
 
Generally silver tables are joins on multiple bronze tables
There will be lot of complexities
Each of the join table can be a select within a select / aggregated query etc
A view is built on this complex logic
Lets deep dive into the brown highlighted columns
A notebook is built to create the CDF for the view
The notebook details are @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571896224?o=984752964297111#command/1412556571896225
The same notebook is called for any silver view but with different parameters
Lets deep dive into the green highlighted columns
DLT apply_changes used to merge the new / updated / deleted records from CDF into final silver table
DLT apply_changes has inherent capability for SCD Type 2 merge and is used as per the requirement
The silver DLT generated is @ https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1412556571881310?o=984752964297111#command/1412556571881311 
When DLT was reading directly from the config tables, debugging was difficult. So we developed a notebook generator  that reads the config tables and generates the underlying code for DLT https://adb-984752964297111.11.azuredatabricks.net/editor/notebooks/1242762839975211?o=984752964297111#command/1242762839975510 


