# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

dbutils.widgets.text("catalog", "dbx", "Catalog name")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema name")
dbutils.widgets.text("silver_schema", "silver", "Silver schema name")
dbutils.widgets.text("gold_schema", "gold", "Gold schema name")
dbutils.widgets.text("metadata_schema", "metadata", "Metadata schema name")

dbutils.widgets.text("notebook_base_path", "/Workspace/Users/srinivasreddy.admala@databricks.com/databricks-blogposts/2025-04-lakeflow-config-driven-framework/", "Notebook base path")
dbutils.widgets.text("jsonColumnName", "data.data", "JSON column name")
dbutils.widgets.text("schemaRegistryTable", "dbx.metadata.schema_registry", "Schema registry name")


# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")

notebook_base_path = dbutils.widgets.get("notebook_base_path")
jsonColumnName = dbutils.widgets.get("jsonColumnName")
schemaRegistryTable = dbutils.widgets.get("schemaRegistryTable")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat,ExportFormat,Language

import base64

import time

import sys
import os
sys.path.append(os.path.abspath('.'))

# COMMAND ----------

## Non-Config variables (Mostly Static)

generalTablePropertiesBronze = {
    "quality": "bronze",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.dataSkippingNumIndexedCols": '-1'
    }

generalTablePropertiesSilver = {
    "quality": "silver",
    "delta.tuneFileSizesForRewrites": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.dataSkippingNumIndexedCols": '-1'
    }

schemaInferenceProperties = {
    "spark.databricks.cloudFiles.schemaInference.sampleSize.numBytes": "500g",
    "spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles": "100000"
    }

autoMergeProperties = """spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")"""

tenantExtractionLogic = {                      
    "tenant" : '.withColumn("tenant", col("tenant"))',
    "core"   : '.withColumn("tenant", when(col("tenant") == "", "core").otherwise(col("tenant")))'
    }

dataCastingLogic = '.withColumn("data", col("data").cast(StringType()))'

auditColumnList= [
    "_metadata.file_path",
    "_metadata.file_modification_time", 
    "_metadata.file_size", 
    "_metadata.file_name"
    ]

# COMMAND ----------

def create_notebook_init_content(pipeline_name, raw_layer):
    return f"""# Notebook for {pipeline_name} - {raw_layer}
import dlt

from pyspark.sql.functions import *
from pyspark.sql.types import * 

catalog = '{catalog}'
bronze_schema = '{bronze_schema}'
silver_schema = '{silver_schema}'
gold_schema = '{gold_schema}'
            """

# COMMAND ----------

def create_raw_1_dlt_def(tableName, tablePropDict, partitionColumnList, schemaInferenceProperties, schemaLocation, pathGlobFilter, sourcePaths, auditColumnList, applyTenantExtraction, applyDataCasting):
    # Convert table properties to string
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])

    # Prepare schema inference properties setting
    schema_inference_settings = "\n    ".join([f'spark.conf.set("{k}", "{v}")' for k, v in schemaInferenceProperties.items()])
    
    # Convert partition columns to string
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])
    
    # Convert audit columns to string
    audit_cols = ", ".join([f'"{col}"' for col in auditColumnList])
    
    if len(sourcePaths) == 1:
        key, sourcePath = next(iter(sourcePaths.items()))

        # Prepare the transformation logic
        transformation_logic = []
        if applyTenantExtraction:
            transformation_logic.append(tenantExtractionLogic[key])
        if applyDataCasting:
            transformation_logic.append(dataCastingLogic)
        transformation_logic = "\n            ".join(transformation_logic)

        dlt_table_definition = f"""
@dlt.table(
    name = "{tableName}",
    table_properties = {{
        {table_props}
    }},
    partition_cols=[{partition_cols}]
)
def {tableName}():
    {schema_inference_settings}
    
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{schemaLocation}/{tableName}")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "{pathGlobFilter}")
            .option("recursiveFileLookup", "true")
            .load("{sourcePath}")
            {transformation_logic}
            .selectExpr({audit_cols}, "*")
    )
"""
    elif len(sourcePaths) > 1:
        dlt_table_definition = f"""
dlt.create_streaming_table(
    name = "{tableName}",
    comment = "Ingesting parquet files from multiple S3 paths",
    table_properties = {{
        {table_props}
    }},
    partition_cols=[{partition_cols}]
)

"""
        for i, (key, sourcePath) in enumerate(sourcePaths.items()):
            # Prepare the transformation logic for each key
            transformation_logic = []
            if applyTenantExtraction:
                transformation_logic.append(tenantExtractionLogic[key])
            if applyDataCasting:
                transformation_logic.append(dataCastingLogic)
            transformation_logic = "\n            ".join(transformation_logic)

            dlt_table_definition += f"""
@dlt.append_flow(target="{tableName}")
def append_from_source_{tableName}_{key}():
    {schema_inference_settings}
    
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", f"{schemaLocation}/{tableName}")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("pathGlobFilter", "{pathGlobFilter}")
            .option("recursiveFileLookup", "true")
            .load("{sourcePath}")
            {transformation_logic}
            .selectExpr({audit_cols}, "*")
    )
"""

    return dlt_table_definition

# COMMAND ----------

def create_raw_2_dlt_def(sourceTableName, targetTableName, tablePropDict, partitionColumnList, schemaRegistryTable, jsonColumnName , autoMergeProperties):
    # Convert table properties to string
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])
    
    # Convert partition columns to string
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])

    dlt_table_definition = f"""
@dlt.table(
    name = "{targetTableName}",
    table_properties = {{
        {table_props}
    }},
    partition_cols=[{partition_cols}]
)
def {targetTableName}():

    {autoMergeProperties}

    # Read the existing schema from the schema registry table
    existing_schema_df = spark.read.table("{schemaRegistryTable}").where(f"table_name = '{sourceTableName}'")

    existing_schema_string = existing_schema_df.select("schema").first()[0]
   
    # Create a StructType from the schema string
    schema = StructType.fromDDL(existing_schema_string)
        
    return (
        spark
            .readStream
            .option("skipChangeCommits", "true")
            .table(f"{catalog}.{bronze_schema}.{sourceTableName}")
            .withColumn('{jsonColumnName}',  from_json(col('{jsonColumnName}'), schema))
    )
"""
    return dlt_table_definition

# COMMAND ----------

def create_raw_3_dlt_def(sourceTableName, targetTableName, tablePropDict, partitionColumnList, auditColumnList, selectExprList):
    # Convert table properties to string
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])
    
    # Convert partition columns to string
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])
    
    # Function to remove first qualifier and enclose in quotes
    def process_column_name(col):
        parts = col.split('.')
        return f'"""{parts[-1]}"""' if len(parts) > 1 else f'"""{col}"""'
    
    # Convert audit columns to string, removing first qualifier
    audit_cols = ", ".join([process_column_name(col) for col in auditColumnList])
    
    # Function to enclose items in triple quotes
    def enclose_in_triple_quotes(select_expr):
        return [f'"""{item}"""' for item in select_expr]
    
    # Process selectExpr
    select_expr_statements = []
    for i, select_expr_dict in enumerate(selectExprList, 1):
        select_expr = enclose_in_triple_quotes(next(iter(select_expr_dict.values())))
        select_expr_statements.append(f".selectExpr({audit_cols}, {', '.join(select_expr)})")

    # Join all selectExpr statements
    cascaded_select_expr = "\n        ".join(select_expr_statements)

    dlt_table_definition = f"""
@dlt.table(
    name = "{targetTableName}",
    table_properties = {{
        {table_props}
    }},
    partition_cols=[{partition_cols}]
)
def {targetTableName}():
    return (
        dlt.read_stream("{sourceTableName}")
        {cascaded_select_expr}
    )
"""
    return dlt_table_definition

# COMMAND ----------

def create_raw_child_dlt_def(sourceTableName, targetTableName, tablePropDict, partitionColumnList, auditColumnList, selectExprList):
    # Convert table properties to string
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])
    
    # Convert partition columns to string
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])
    
    # Convert audit columns to string
    audit_cols = ", ".join([f'"{col}"' for col in auditColumnList])
    
    # Process selectExpr
    select_expr_statements = []
    for i, select_expr_dict in enumerate(selectExprList, 1):
        select_expr = ", ".join([f'"""{item}"""' for item in next(iter(select_expr_dict.values()))])
        select_expr_statements.append(f".selectExpr({select_expr})")

    # Join all selectExpr statements
    cascaded_select_expr = "\n        ".join(select_expr_statements)

    dlt_table_definition = f"""
@dlt.table(
    name = "{targetTableName}",
    comment = "Child table generated from {sourceTableName}",
    table_properties = {{
        {table_props}
    }},
    partition_cols=[{partition_cols}]
)
def {targetTableName}():
    return (
        dlt.read_stream("{sourceTableName}")
        {cascaded_select_expr}
    )
"""
    return dlt_table_definition

# COMMAND ----------

def create_bronze_scd_dlt_def(sourceTableName, targetTableName, targetTableComment, tablePropDict, partitionColumnList, primaryKeys, sequenceBy, deleteExpr, exceptColumnList, scdType):
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])
    primary_keys = ", ".join([f'"{key}"' for key in primaryKeys])

    # Check if exceptColumnList is not null and not empty
    except_columns = ", ".join([f'"{col}"' for col in exceptColumnList]) if exceptColumnList else None

    # Check if sourceTableName is already fully qualified
    if sourceTableName.count('.') == 2:
        fully_qualified_source = sourceTableName
    else:
        fully_qualified_source = f"{catalog}.{bronze_schema}.{sourceTableName}"

    dlt_def = f"""
@dlt.view
def {sourceTableName.split('.')[-1]}():
    return spark.readStream.option("skipChangeCommits", "true").table("{fully_qualified_source}")

dlt.create_streaming_table("{targetTableName}",
                           comment = "{targetTableComment}",
                           table_properties = {{
                               {table_props}
                           }},
                           partition_cols=[{partition_cols}])

dlt.apply_changes(
    target = "{targetTableName}",
    source = "{sourceTableName.split('.')[-1]}",
    keys = [{primary_keys}],
    sequence_by = col("{sequenceBy}"),
    apply_as_deletes = expr("{deleteExpr}"),
    {"except_column_list = [" + except_columns + "]," if except_columns else "# All Columns Included"}
    stored_as_scd_type = "{scdType}"
)
"""
    return dlt_def

# COMMAND ----------

def generate_bronze_raw_base_dlt_notebooks(spark, workspace, config_table_name, notebook_base_path):
    df = spark.table(config_table_name)
    grouped_df = df.groupBy("pipeline_name").agg(collect_list(struct(*df.columns)).alias("configs"))

    for row in grouped_df.collect():
        pipeline_name, configs = row["pipeline_name"], row["configs"]
        
        notebook_contents = {
            "raw1": create_notebook_init_content(pipeline_name, "raw1")
            , "raw2_raw3": create_notebook_init_content(pipeline_name, "raw2_raw3")
        }
        
        for config in configs:
            dlt_defs = {
                "raw1": create_raw_1_dlt_def(
                    tableName=config["pre_sch_inf_table"],
                    tablePropDict=generalTablePropertiesBronze,
                    partitionColumnList=config["patition_columns"],
                    schemaInferenceProperties=schemaInferenceProperties,
                    schemaLocation=config["sch_inf_path"],
                    pathGlobFilter=config["globfilter"],
                    sourcePaths=config["autoloader_path"],
                    auditColumnList=auditColumnList,
                    applyTenantExtraction=True,
                    applyDataCasting=True
                ),
                "raw2": create_raw_2_dlt_def(
                    sourceTableName=config["pre_sch_inf_table"],
                    targetTableName=config["post_sch_inf_table"],
                    tablePropDict=generalTablePropertiesBronze,
                    partitionColumnList=config["patition_columns"],
                    schemaRegistryTable=schemaRegistryTable,
                    jsonColumnName=jsonColumnName,
                    autoMergeProperties=autoMergeProperties
                ),
                "raw3": create_raw_3_dlt_def(
                    sourceTableName=config["post_sch_inf_table"],
                    targetTableName=config["target_table"],
                    tablePropDict=generalTablePropertiesBronze,
                    partitionColumnList=config["patition_columns"],
                    auditColumnList=auditColumnList,
                    selectExprList=config["selectExpr"]
                )
            }
            
            notebook_contents["raw1"] += f"\n# COMMAND ----------\n{dlt_defs['raw1']}"
            notebook_contents["raw2_raw3"] += f"\n# COMMAND ----------\n{dlt_defs['raw2']}\n# COMMAND ----------\n{dlt_defs['raw3']}"

        for notebook_type, content in notebook_contents.items():
            notebook_path = f"{notebook_base_path}/{pipeline_name}_{notebook_type}"
            content_base64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
            # print(f"notebook_type: {notebook_type}")
            # print(f"content: {content}")
            # print(f"notebook_path: {notebook_path}")
            
            workspace.workspace.import_(
                path=notebook_path,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                content=content_base64,
                overwrite=True
            )
            print(f"Created notebook: {notebook_path}")

    print("[1] Base Raw Notebook generation complete.")



# COMMAND ----------

def generate_bronze_raw_child_dlt_notebooks(spark, workspace, config_table_name, notebook_base_path):
    df = spark.table(config_table_name)
    grouped_df = df.groupBy("pipeline_name").agg(collect_list(struct(*df.columns)).alias("configs"))
            
    for row in grouped_df.collect():
        pipeline_name, configs = row["pipeline_name"], row["configs"]

        notebook_path = f"{notebook_base_path}/{pipeline_name}_raw2_raw3"
        file_info = workspace.workspace.export(path=notebook_path, format=ExportFormat.SOURCE)
        existing_content = base64.b64decode(file_info.content).decode('utf-8')

        notebook_content = existing_content
        
        for config in configs:
            dlt_def = create_raw_child_dlt_def(
                sourceTableName=config["source_table"],
                targetTableName=config["target_table"],
                tablePropDict=generalTablePropertiesBronze,
                partitionColumnList=config["patition_columns"],
                auditColumnList=auditColumnList,
                selectExprList=config["selectExpr"]
            )
            
            notebook_content += f"\n# COMMAND ----------\n{dlt_def}"

        content_base64 = base64.b64encode(notebook_content.encode("utf-8")).decode("ascii")
        
        workspace.workspace.import_(
            path=notebook_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=content_base64,
            overwrite=True
        )
        print(f"Updated notebook: {notebook_path}")

    print("[2] Child Raw notebook generation complete.")

# COMMAND ----------

def generate_bronze_scd_dlt_notebooks(spark, workspace, config_table_name, notebook_base_path):
    df = spark.table(config_table_name)
    grouped_df = df.groupBy("pipeline_name").agg(collect_list(struct(*df.columns)).alias("configs"))

    for row in grouped_df.collect():
        pipeline_name, configs = row["pipeline_name"], row["configs"]
        
        notebook_path = f"{notebook_base_path}/{pipeline_name}_bronze_scd"
        
        notebook_content = create_notebook_init_content(pipeline_name, "bronze_scd")
        
        for config in configs:
            dlt_def = create_bronze_scd_dlt_def(
                sourceTableName=config["source_table"],
                targetTableName=config["target_table"],
                targetTableComment=config["target_table_comment"],
                tablePropDict=generalTablePropertiesBronze,
                partitionColumnList=config["patition_columns"],
                primaryKeys=config["primary_key"],
                sequenceBy=config["seqyence_by"],
                deleteExpr=config["delete_expr"],
                exceptColumnList=config["except_column_list"],
                scdType=config["scd_type"]
            )
            
            notebook_content += f"\n# COMMAND ----------\n{dlt_def}"

        content_base64 = base64.b64encode(notebook_content.encode("utf-8")).decode("ascii")
        
        workspace.workspace.import_(
            path=notebook_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=content_base64,
            overwrite=True
        )
        print(f"Created notebook: {notebook_path}")

    print("[3] Bronze SCD notebook generation complete.")

# COMMAND ----------


def create_silver_scd_dlt_def(sourceTableName, targetTableName, targetTableComment, tablePropDict, partitionColumnList, primaryKeys, sequenceBy, deleteExpr, exceptColumnList, scdType):
    table_props = ", ".join([f'"{k}": "{v}"' for k, v in tablePropDict.items()])
    partition_cols = ", ".join([f'"{col}"' for col in partitionColumnList])
    primary_keys = ", ".join([f'"{key}"' for key in primaryKeys])

    # Check if exceptColumnList is not null and not empty
    except_columns = ", ".join([f'"{col}"' for col in exceptColumnList]) if exceptColumnList else None

    # Check if sourcetableName is already fully qualified
    if sourceTableName.count('.') == 2:
        fully_qualified_source = sourceTableName
    else:
        fully_qualified_source = f"{catalog}.{silver_schema}.{sourceTableName}"
        
    dlt_def = f"""
@dlt.view
def {sourceTableName.split('.')[-1]}():
    return spark.readStream.option("skipChangeCommits", "true").table("{fully_qualified_source}")

dlt.create_streaming_table("{targetTableName}",
                           comment = "{targetTableComment}",
                           table_properties = {{
                               {table_props}
                           }},
                           partition_cols=[{partition_cols}])

dlt.apply_changes(
    target = "{targetTableName}",
    source = "{sourceTableName.split('.')[-1]}",
    keys = [{primary_keys}],
    sequence_by = col("{sequenceBy}"),
    apply_as_deletes = expr("{deleteExpr}"),
    {"except_column_list = [" + except_columns + "]," if except_columns else "# All Columns Included"}
    stored_as_scd_type = "{scdType}"
)
"""
    return dlt_def


# COMMAND ----------

def create_silver_materialized_view_dlt_def(dimenstions):
    notebook_content = ""

    for dim in dimenstions:
        dlt_def = f"""
@dlt.table
def {dim}():
    return spark.sql(\"\"\"
{globals()[dim]}
\"\"\")
\n# COMMAND ----------\n
"""
        notebook_content += dlt_def

    return notebook_content

# COMMAND ----------

def generate_silver_scd_dlt_notebooks(spark, workspace, config_table_name, notebook_base_path):
    df = spark.table(config_table_name)
    grouped_df = df.groupBy("pipeline_name").agg(collect_list(struct(*df.columns)).alias("configs"))

    for row in grouped_df.collect():
        pipeline_name, configs = row["pipeline_name"], row["configs"]
        
        notebook_path = f"{notebook_base_path}/{pipeline_name}_silver_scd"
        
        notebook_content = create_notebook_init_content(pipeline_name, "silver_scd")
        
        for config in configs:
            dlt_def = create_silver_scd_dlt_def(
                sourceTableName=config["source_table"],
                targetTableName=config["target_table"],
                targetTableComment=config["target_table_comment"],
                tablePropDict=generalTablePropertiesSilver,
                partitionColumnList=config["patition_columns"],
                primaryKeys=config["primary_key"],
                sequenceBy=config["seqyence_by"],
                deleteExpr=config["delete_expr"],
                exceptColumnList=config["except_column_list"],
                scdType=config["scd_type"]
            )
            
            notebook_content += f"\n# COMMAND ----------\n{dlt_def}"

            # dlt_materialized_view_def = create_silver_materialized_view_dlt_def(
            #     dimenstions = config["dimensions"]
            # )

            # notebook_content += f"\n# COMMAND ----------\n{dlt_materialized_view_def}"

        content_base64 = base64.b64encode(notebook_content.encode("utf-8")).decode("ascii")
        
        workspace.workspace.import_(
            path=notebook_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=content_base64,
            overwrite=True
        )
        print(f"Created notebook: {notebook_path}")

    print("[4] Silver SCD notebook generation complete.")



# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
metadata_schema = dbutils.widgets.get("metadata_schema")

notebook_base_path = dbutils.widgets.get("notebook_base_path")
jsonColumnName = dbutils.widgets.get("jsonColumnName")
schemaRegistryTable = dbutils.widgets.get("schemaRegistryTable")

# COMMAND ----------

# Usage
if __name__ == "__main__":
    spark = SparkSession.builder.appName("NotebookGenerator").getOrCreate()
    workspace = WorkspaceClient()

    # Generating Bronze - Raw1 , Raw2 , Raw3
    generate_bronze_raw_base_dlt_notebooks(spark, workspace, f"{catalog}.{metadata_schema}.config_bronze_raw", notebook_base_path)

    # Generating Bronze - Child Raw3
    generate_bronze_raw_child_dlt_notebooks(spark, workspace, f"{catalog}.{metadata_schema}.config_bronze_childnodes_raw", notebook_base_path)

    # Generating Bronze - SCD1
    generate_bronze_scd_dlt_notebooks(spark, workspace, f"{catalog}.{metadata_schema}.config_bronze", notebook_base_path)

    # Generating Silver - SCD2
    generate_silver_scd_dlt_notebooks(spark, workspace, f"{catalog}.{metadata_schema}.config_silver", notebook_base_path) 
