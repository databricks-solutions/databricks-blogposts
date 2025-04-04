from config import load_config
from databricks.connect import DatabricksSession

# databricks-connect -- config tables
# databricks sdk for python -- DLTs, Workflow with dependencies
# databricks cli -- environment variable -- authentication

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary, PipelineClusterAutoscale, PipelineClusterAutoscaleMode
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import SchemasAPI
from databricks.sdk.service.jobs import Task, NotebookTask, Source, compute, PythonWheelTask,JobCluster, PipelineTask, TaskDependency
from itertools import tee

config = load_config('configs/config.json')

# spark = DatabricksSession.builder.profile(config['authentication_profile']).getOrCreate()
# SET environment variable DATABRICKS_CONFIG_PROFILE to "FLEXERA"
# export DATABRICKS_CONFIG_PROFILE=FLEXERA
# echo DATABRICKS_CONFIG_PROFILE

#Comment out this line when running from CI-CD pipeline
spark = DatabricksSession.builder.profile("adb-984752964297111").serverless().getOrCreate()
# spark = DatabricksSession.builder.profile("FLEXERA").getOrCreate()
#Un-comment the below line when using Serverless in the CI-CD pipeline
#spark = DatabricksSession.builder.remote().serverless().getOrCreate()
#Un-comment the below lline when using an all purpose cluster in the CI-CD pipeline
# spark = DatabricksSession.builder.remote(cluster_id=config['cluster_id']).getOrCreate()

ws = WorkspaceClient(host="https://adb-984752964297111.11.azuredatabricks.net/")
spark_configuration = {
            "spark.sql.shuffle.partitions": "1000"
        }

def pipeline_create_or_update(dlt_name, catalog, target, notebook_path, node_type_id, autoscale_max):
    all_piplelines = ws.pipelines.list_pipelines(filter=f"name LIKE '%{dlt_name}%'")
    all_pipleline_ids = [x.pipeline_id for x in all_piplelines]
    if len(all_pipleline_ids) == 0:
        id = ws.pipelines.create(catalog=catalog,
                            name=dlt_name,
                            configuration=spark_configuration,
                            libraries=[
                                PipelineLibrary(
                                    notebook=NotebookLibrary(
                                        path=f"{config['notebook_basepath']}{notebook_path}"
                                    )
                                )
                            ],
                            target=target,
                            clusters=[pipelines.PipelineCluster(label="default",
                                                                node_type_id = node_type_id,
                                                                autoscale=PipelineClusterAutoscale(1, autoscale_max, PipelineClusterAutoscaleMode.ENHANCED)
                                                                )
                                    ],
                            photon = True,
                            channel="PREVIEW"
                            )
        return id
    elif len(all_pipleline_ids) == 1:
        ws.pipelines.update(pipeline_id = all_pipleline_ids[0], #only one pipeline exists with the full name              
                            catalog=catalog,
                            name=dlt_name,
                            configuration=spark_configuration,
                            libraries=[
                                PipelineLibrary(
                                    notebook=NotebookLibrary(
                                        path=f"{config['notebook_basepath']}{notebook_path}"
                                    )
                                )
                            ],
                            target=target,
                            clusters=[pipelines.PipelineCluster(label="default",
                                                                node_type_id = node_type_id,
                                                                autoscale=PipelineClusterAutoscale(1, autoscale_max, PipelineClusterAutoscaleMode.ENHANCED)
                                                                )
                                    ],
                            photon = True,
                            channel="PREVIEW"
                            )
        return all_pipleline_ids[0]
    else:
        raise Exception("There are more than 1 pipelines with the same name")

########Creation of raw DLTs#####################################################################
created_bronze_raw1_dlt = pipeline_create_or_update(f"{config['workflow_name']}_raw1_{config['postfix']}", config['bronze_catalog'], config['bronze_schema'],
                                                        f"{config['workflow_name']}_raw1", config["node_type_id"], config['autoscale_max'])
print(f"raw1 dlt got created / updated: {created_bronze_raw1_dlt}")
created_bronze_raw3_dlt = pipeline_create_or_update(f"{config['workflow_name']}_raw2_raw3_{config['postfix']}", config['bronze_catalog'], config['bronze_schema'],
                                                        f"{config['workflow_name']}_raw2_raw3", config["node_type_id"], config['autoscale_max'])
print(f"raw2_raw3 dlt got created / updated: {created_bronze_raw3_dlt}")
########Creation of bronze SCD DLT#################################################################
created_bronze_dlt = pipeline_create_or_update(f"{config['workflow_name']}_bronze_scd_{config['postfix']}", config['bronze_catalog'], config['bronze_schema'],
                                                f"{config['workflow_name']}_bronze_scd", config["node_type_id"], config['autoscale_max'])
print(f"bronze scd dlt got created / updated: {created_bronze_dlt}")
########Creation of silver SCD DLT#####################################################################
created_silver_dlt = pipeline_create_or_update(f"{config['workflow_name']}_silver_scd_{config['postfix']}", config['bronze_catalog'], config['bronze_schema'],
                                                f"{config['workflow_name']}_silver_scd", config["node_type_id"], config['autoscale_max'])
print(f"silver scd dlt got created / updated: {created_silver_dlt}")
########Creation of raw1 task###########################################################################
bronze_raw1_dlt = PipelineTask(pipeline_id=created_bronze_raw1_dlt)
bronze_raw1_task = Task(task_key="bronze_raw1", pipeline_task=bronze_raw1_dlt)

########Creation of schema inference tasks###########################################################################
bronze_raw3_dependson = []
sch_inf_tasks = []

def create_sch_inf_task(task_key, notebook_path, base_parameters):
    task1 =  Task(
        task_key=task_key,
        description=f"Task for {task_key}",
        job_cluster_key=f"config_driven_job_cluster",
        notebook_task = NotebookTask(notebook_path = notebook_path, base_parameters=base_parameters),
        depends_on=[TaskDependency("bronze_raw1")]
    )
    bronze_raw3_dependson.append(TaskDependency(task_key))
    return task1

df = (spark
        .read
        .table(config['config_bronze_raw'])
        .filter(f"pipeline_name = '{config['workflow_name']}'")
        .filter(f"sch_inf_notebook IS NOT NULL")
        .select("sch_inf_notebook", "sch_inf_notebook_params", "pre_sch_inf_table")
    )

config_rows = df.collect()
# print(config_rows)

for x in config_rows:
    sch_inf_tasks.append(create_sch_inf_task(f"sch_inf_{x.pre_sch_inf_table}", x.sch_inf_notebook, x.sch_inf_notebook_params))

# print(bronze_raw3_dependson)

########Creation of raw3 task###########################################################################
bronze_raw3_dlt = PipelineTask(pipeline_id=created_bronze_raw3_dlt)
bronze_raw3_task = Task(task_key="bronze_raw2_raw3", pipeline_task=bronze_raw3_dlt, depends_on=bronze_raw3_dependson)

########Creation of raw child tasks#######################################
bronze_scd_dependson = []
child_nodes_cdf_tasks = []

def create_child_nodes_cdf_task(task_key, notebook_path, base_parameters):
    task2 =  Task(
        task_key=task_key,
        description=f"Task for {task_key}",
        job_cluster_key=f"config_driven_job_cluster",
        notebook_task = NotebookTask(notebook_path = notebook_path, base_parameters=base_parameters),
        depends_on=[TaskDependency("bronze_raw2_raw3")]
    )
    bronze_scd_dependson.append(TaskDependency(task_key))
    return task2

df = (spark
        .read
        .table(config['config_bronze'])
        .filter(f"pipeline_name = '{config['workflow_name']}'")
        .filter(f"cdf_notebook IS NOT NULL")
        .select("target_table", "cdf_notebook", "cdf_notebook_params")
    )
config_rows = df.collect()
# print(config_rows)

for x in config_rows:
    child_nodes_cdf_tasks.append(create_child_nodes_cdf_task(f"cdf_{x.target_table}", x.cdf_notebook, x.cdf_notebook_params))
# print(bronze_scd_dependson)
########Creation of bronze SCD task#######################################
bronze_scd_pipeline = PipelineTask(pipeline_id=created_bronze_dlt)
bronze_scd_task = Task(task_key="bronze_scd", pipeline_task=bronze_scd_pipeline, depends_on=bronze_scd_dependson)
########Creation of silver CDF task#######################################
df = (spark
        .read
        .table(config['config_silver'])
        .filter(f"pipeline_name = '{config['workflow_name']}'")
        .select("target_table", "cdf_notebook", "cdf_notebook_params")
    )
config_rows = df.collect()[0]

silver_cdf_task = Task(task_key=f"silver_cdf_{config_rows.target_table}"
                        ,description=f"Task for silver_cdf_{config_rows.target_table}", 
                        job_cluster_key=f"config_driven_job_cluster",
                        notebook_task = NotebookTask(notebook_path = config_rows.cdf_notebook, base_parameters=config_rows.cdf_notebook_params),
                        depends_on=[TaskDependency("bronze_scd")]
    )

silver_scd_pipeline = PipelineTask(pipeline_id=created_silver_dlt)
silver_scd_task = Task(task_key="silver_scd", pipeline_task=silver_scd_pipeline, depends_on=[TaskDependency(f"silver_cdf_{config_rows.target_table}")])
########Create / Update of Workflow#######################################

def create_or_update_workflow():
    try:
        tasks = []
        tasks.append(bronze_raw1_task)
        tasks.extend(sch_inf_tasks)
        tasks.append(bronze_raw3_task)
        tasks.extend(child_nodes_cdf_tasks)
        tasks.append(bronze_scd_task)
        tasks.append(silver_cdf_task)
        tasks.append(silver_scd_task)
        
        job_clusters = [
            JobCluster(
                job_cluster_key=f"config_driven_job_cluster",
                new_cluster=compute.ClusterSpec(
                                node_type_id=config['node_type_id'],
                                spark_version=config['spark_version'],
                                autoscale=compute.AutoScale(min_workers=1, max_workers=int(config['autoscale_max'])),
                                data_security_mode=compute.DataSecurityMode.SINGLE_USER
                            )
            )
        ]
        all_worklows = []
        ####give the full job name when listing out the jobs###########
        name1 = f"{config['workflow_name']}_{config['postfix']}"
        print(name1)
        all_worklows = ws.jobs.list(name = name1)
        all_job_ids = [x.job_id for x in all_worklows]
        print(all_job_ids)
        if len(all_job_ids) == 0:
            return ws.jobs.create(name=name1, tasks=tasks, job_clusters=job_clusters)
        elif len(all_job_ids) == 1:
            return ws.jobs.update(job_id=all_job_ids[0]
                                    , new_settings=jobs.JobSettings(name=name1
                                                                            , tasks=tasks
                                                                            , job_clusters=job_clusters)
                                    # , fields_to_remove = ['tasks/child_nodes_cdf_notebook0', 'tasks/child_nodes_cdf_notebook1', 'tasks/child_nodes_cdf_notebook2']                
                                    )
        else:
            raise Exception("There are more than 1 workflows with the same name")
    except Exception as e:
        print(f"Error creating workflow: {e}")
        raise e

create_or_update_workflow()