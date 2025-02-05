# Databricks notebook source
# MAGIC %md
# MAGIC # Experiments
# MAGIC
# MAGIC In this notebook we demonstrate a series of experiments, where we explore different approaches to train machine learning models on the same dataset: 
# MAGIC 1. Training a global model for the whole dataset with hyper parameter optimisation using [Optuna](https://optuna.org/).
# MAGIC 2. Training a model per group based on the groups identified in the dataset. For this experiments we keep the best hyperparameters form the first experiment.
# MAGIC 3. Training a model per group with hyperparameter optimisation per model using [Optuna](https://optuna.org/).

# COMMAND ----------

# MAGIC %md 
# MAGIC # Experiments Setup

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Environment setup

# COMMAND ----------

# MAGIC %pip install optuna optuna-integration

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

# MAGIC %md ## Data setup

# COMMAND ----------

from pyspark.sql.functions import col, when
import pandas as pd

source_data_file_path = "./synthetic_data/synthetic_polynomial_groups_binary.csv"

import os

data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", True)
    .load(f"file:{os.getcwd()}/{source_data_file_path}")
)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

GROUP_COL = 'group'
N_GROUPS = train_data.select(GROUP_COL).distinct().count()

display(data)

# COMMAND ----------

# MAGIC %md ## ML-related setup

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score
from mlflow.models import infer_signature
from mlflow.utils.mlflow_tags import MLFLOW_PARENT_RUN_ID
import mlflow
import xgboost as xgb
import pandas as pd

mlflow.autolog(disable=True)

# Get or create MLflow experiment
experiment_name = '/Shared/Grouped_Optuna'
if not mlflow.get_experiment_by_name(experiment_name):
    mlflow.create_experiment(experiment_name)
experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

# Delete all prior runs from the MLflow experiment
clean_experiment = True
if clean_experiment:
  mlflow.delete_runs(experiment_id=experiment_id, max_timestamp_millis=99999999999999)


TARGET_COL = 'target'
FEATURE_COLS = [GROUP_COL] + [col for col in train_data.columns if col.startswith('num_')]

signature = infer_signature(train_data.limit(10).toPandas()[FEATURE_COLS], [0]*10)

# COMMAND ----------

# MAGIC %md # Model training

# COMMAND ----------

from sklearn.model_selection import cross_val_score
# As we will explore different patterns, this train-and-log procedure will be reused across all examples
def train_and_log(X_train, y_train, X_valid, y_valid, log_group=False, log_model=True, **params):
    # Train and evaluate the model
    model = xgb.XGBClassifier(**params).fit(X_train, y_train, eval_set=[(X_valid, y_valid)], early_stopping_rounds=50, verbose=False)
    predictions = model.predict(X_valid)

    # # Log params and metrics
    f1 = f1_score(y_valid, predictions)
    mlflow.log_metric('f1_score', f1)
    mlflow.log_metric('accuracy_score', accuracy_score(y_valid, predictions))
    mlflow.log_metric('precision_score', precision_score(y_valid, predictions))
    mlflow.log_metric('recall_score', recall_score(y_valid, predictions))
    mlflow.log_params(params)
    mlflow.log_param('n_train_rows', len(X_train))
    if log_group:
      group_key = X_train[GROUP_COL].iloc[0]
      mlflow.log_param(GROUP_COL, group_key)

    # Optionally log the model (usually avoided during hyperparameter search)
    if log_model:
      # Use non-conflicting artifact paths so that models for individual groups can be stored in a wrapper model.
      artifact_path = f'model_{GROUP_COL}={group_key}' if log_group else 'model'
      signature = infer_signature(X_valid, predictions)
      model_uri = f'runs:/{mlflow.active_run().info.run_id}/{artifact_path}'
      mlflow.sklearn.log_model(model, artifact_path=artifact_path, signature=signature, input_example=X_valid.head())

    # Return useful metadata
    return {
      GROUP_COL:   group_key if log_group else None,
      'model_uri': model_uri if log_model else None,
      'f1_score':  f1,
      **params}

# COMMAND ----------

def run_optimization(grandparent_run_id, pdf: pd.DataFrame, use_group: bool = True):
    """
    Runs hyperparameter optimisation and training for grouped or non-grouped data.

    Parameters:
    - grandparent_run_id: ID of the parent MLflow run.
    - pdf: A pandas DataFrame containing the dataset.
    - use_group: Boolean indicating whether to use grouping logic or not.

    Returns:
    - A DataFrame containing the results of the optimisation and training.
    """
    if use_group:
        group_key = pdf[GROUP_COL].iloc[0]  # Use the group key if grouping is enabled
        run_name = f'Hyperparameter search for {GROUP_COL}={group_key}'
    else:
        group_key = "no_group"
        run_name = 'Hyperparameter search for all data'

    X, y = pdf[FEATURE_COLS], pdf[TARGET_COL]
    X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, random_state=42)

    tags = {MLFLOW_PARENT_RUN_ID: grandparent_run_id}  # Optionally include group info if needed

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id, nested=True, tags=tags) as parent_run:
        try:
            # Run hyperparameter search
            study = optuna.create_study(direction="maximize")
            study.optimize(
                lambda trial: objective(trial, X_train, y_train, X_valid, y_valid),
                n_trials=N_TRIALS
            )

            # Retrain and log the best model
            result = train_and_log(
                X_train, y_train, X_valid, y_valid,
                log_group=use_group,  # Log group-specific information only if use_group is True
                log_model=True,
                **study.best_trial.params
            )

            # Return metadata for the best model
            return pd.DataFrame([result])
        except Exception as e:
            # Return a DataFrame indicating the error
            return pd.DataFrame(columns=[field.name for field in schema.fields], 
                                data=[[group_key, str(e)] + [None] * 7])


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Experiment 1
# MAGIC ## One model for all data
# MAGIC
# MAGIC Our first experiment focuses on training a single global model on the entire dataset. To ensure the model generalises well across all groups, we optimise its hyperparameters using Optuna, a robust framework for hyperparameter search. This process involves:
# MAGIC
# MAGIC * Defining a search space for key hyperparameters, such as learning rate, maximum depth, and tree subsampling.
# MAGIC * Iteratively training models with sampled hyperparameters and evaluating them on a validation set.
# MAGIC * Selecting the best hyperparameter configuration that maximises the F1 score.
# MAGIC
# MAGIC The result is a single global model trained on the entire dataset, leveraging the optimal hyperparameters discovered during the search. This model serves as a baseline for comparison in subsequent experiments.

# COMMAND ----------

import optuna

pdf = train_data.toPandas()

X_train, X_valid, y_train, y_valid = train_test_split(
    pdf[FEATURE_COLS], pdf[TARGET_COL], test_size=0.2, random_state=42)

# Define the number of trials for Optuna
N_TRIALS = 50

# Objective function for Optuna to optimise
def objective(trial):
    # Define the hyperparameter search space
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 5, 200),
        'max_depth': trial.suggest_int('max_depth', 2, 15),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'lambda': trial.suggest_float('lambda', 1e-8, 10.0, log=True),
        'gamma': trial.suggest_float('gamma', 0.0, 1.0), 
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 10) 
    }

    # Start an MLflow run for tracking
    with mlflow.start_run(experiment_id=experiment_id, nested=True) as run:
        # Train the model with the current hyperparameters
        model = xgb.XGBClassifier(**params).fit(
            X_train, y_train,
            eval_set=[(X_valid, y_valid)],
            early_stopping_rounds=50,
            verbose=False
        )

        # Make predictions and calculate the F1 score
        predictions = model.predict(X_valid)
        f1 = f1_score(y_valid, predictions)

        # Log parameters and metrics to MLflow
        mlflow.log_params(params)
        mlflow.log_metric('f1_score', f1)

        # Return the F1 score as the optimisation objective
        return f1

# Run the Optuna optimisation process
study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=N_TRIALS)

# Log the best parameters and retrain the final model
best_params = study.best_params
with mlflow.start_run(experiment_id=experiment_id, run_name="Global Model with Optuna") as run:
    final_model = xgb.XGBClassifier(**best_params).fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],
        early_stopping_rounds=50,
        verbose=False
    )
    predictions = final_model.predict(X_valid)
    f1 = f1_score(y_valid, predictions)

    # Log the final model and its metrics
    mlflow.log_params(best_params)
    mlflow.log_metric('f1_score', f1)
    mlflow.sklearn.log_model(final_model, "model", signature=infer_signature(X_valid, predictions))

    global_model_uri = mlflow.get_artifact_uri("model")

print("Best parameters:", best_params)
print("Global model URI:", global_model_uri)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Experiment 2 
# MAGIC ## One model per group - same hyperparameters for each
# MAGIC
# MAGIC Building on the global model, the second experiment shifts focus to group-specific training. 
# MAGIC
# MAGIC Here, we use the best hyperparameters found in the `global experiment` to train individual models for each group in the dataset. This approach acknowledges that each group may exhibit unique patterns, which could benefit from separate models.
# MAGIC
# MAGIC For each group:
# MAGIC
# MAGIC * We extract the subset of data corresponding to that group.
# MAGIC * A model is trained using the previously optimised hyperparameters, ensuring consistency across groups.
# MAGIC * The trained models are packaged into a wrapper model, allowing for unified predictions across groups.
# MAGIC
# MAGIC This experiment highlights the potential benefits of accounting for group-level distinctions, even without performing additional hyperparameter optimisation.

# COMMAND ----------

# DBTITLE 1,Training UDF to be applied to each group
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

params = best_params

# Resulting metadata from training per group
schema = StructType([
    StructField(GROUP_COL, IntegerType(), True),
    StructField("model_uri", StringType(), True),
    StructField("f1_score", FloatType(), True),
    StructField("n_estimators", FloatType(), True),
    StructField("max_depth", FloatType(), True),
    StructField("learning_rate", FloatType(), True),
    StructField("colsample_bytree", FloatType(), True),
    StructField("subsample", FloatType(), True),
    StructField("lambda", FloatType(), True),
    StructField("gamma", FloatType(), True),
    StructField("min_child_weight", FloatType(), True)
])


def train_udf(parent_run_id, pdf: pd.DataFrame) -> pd.DataFrame:
    tags = {MLFLOW_PARENT_RUN_ID: parent_run_id}
    run_name = f'Model for {GROUP_COL}={pdf[GROUP_COL].iloc[0]}'
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name, tags=tags, nested=True) as run:
      X_train, X_valid, y_train, y_valid = train_test_split(pdf[FEATURE_COLS], pdf[TARGET_COL], test_size=0.2, random_state=42)
      result = train_and_log(X_train, y_train, X_valid, y_valid, log_group=True, log_model=True, **params)
    return pd.DataFrame([result])

# COMMAND ----------

# DBTITLE 1,Wrapper model that holds all individual models in memory
class WrapperModel(mlflow.pyfunc.PythonModel):
  def load_context(self, context):
    # Instantiate all models in a dictionary of format {<key>: <model>}
    self.models = {k: mlflow.sklearn.load_model(v) for k, v in context.artifacts.items()}

  def predict(self, context, model_input):
    # Assume that `model_input` may contain a mixture of multiple groups, and apply each
    # corresponding model efficienty while retaining the original ordering of `model_input`.
    def _predict(group: pd.DataFrame) -> pd.Series:
      return pd.Series(
        self.models[group.name].predict(group),
        index=group.index)
    return model_input.groupby(GROUP_COL, group_keys=False).apply(_predict).reindex(model_input.index)

# COMMAND ----------

# DBTITLE 1,Perform distributed training of one model per group
from functools import partial

with mlflow.start_run(experiment_id=experiment_id, run_name='Wrapper Model') as parent_run:
    # Train one model per group. The resulting metadata contains references to the best mlflow runs.
    _train_udf = partial(train_udf, parent_run.info.run_id)
    pdf_results = train_data.repartition(N_GROUPS, GROUP_COL).groupBy(GROUP_COL).applyInPandas(_train_udf, schema=schema).toPandas()

    # Package all trained models referenced via `model_uri` in the metadata.
    model = WrapperModel()
    artifacts = {row[GROUP_COL]: row['model_uri'] for _, row in pdf_results.iterrows()}
    mlflow.pyfunc.log_model('model', python_model=model, artifacts=artifacts, signature=signature)

segmented_model_uri = f'runs:/{parent_run.info.run_id}/model'

display(pdf_results)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Experiment 3
# MAGIC ## Group-Based Hyperparameter Optimisation
# MAGIC
# MAGIC In the final experiment, we further refine our group-based approach by performing hyperparameter optimisation for each group individually. Instead of reusing the global hyperparameters, we allow each group to have its own optimised configuration.
# MAGIC
# MAGIC The [bias-variance tradeoff](https://en.wikipedia.org/wiki/Bias%E2%80%93variance_tradeoff) problem is a fundamental concept in machine learning that highlights the need to balance two opposing forces when building predictive models. Bias refers to the error introduced by overly simplistic models that fail to capture the underlying patterns in the data, often leading to underfitting. Variance, on the other hand, represents the error caused by models that are too complex, overfitting to the training data and failing to generalize to unseen data.
# MAGIC
# MAGIC Hyperparameter search plays a critical role in navigating this tradeoff by tuning the parameters that control a model’s complexity and learning process. Distinct groups within a dataset may have unique characteristics and patterns, requiring tailored hyperparameters that minimize both bias and variance.
# MAGIC
# MAGIC By **conducting hyperparameter search for each group individually**, we can:
# MAGIC
# MAGIC 1. **Capture Group-Specific Variations**: Different groups may have varying levels of complexity and feature interactions. Fine-tuning hyperparameters for each group ensures that the model is well-suited to the specific data distribution and characteristics of that group, leading to better predictive performance.
# MAGIC 2. **Enhance Generalization**: By optimizing hyperparameters for each group, we reduce the risk of overfitting to a particular group, thereby improving the model's ability to generalize to new, unseen data within each group.

# COMMAND ----------

import optuna

N_TRIALS = 50  # number of models to train during hyperparameter search (for each individual group)

schema = StructType([
    StructField(GROUP_COL, IntegerType(), True),
    StructField("model_uri", StringType(), True),
    StructField("f1_score", FloatType(), True),
    StructField("n_estimators", FloatType(), True),
    StructField("max_depth", FloatType(), True),
    StructField("learning_rate", FloatType(), True),
    StructField("colsample_bytree", FloatType(), True),
    StructField("subsample", FloatType(), True),
    StructField("lambda", FloatType(), True),
    StructField("gamma", FloatType(), True),  # Add gamma
    StructField("min_child_weight", FloatType(), True)  # Add min_child_weight
])

# Optuna objective function that trains a single model and returns a single metric
def objective(trial, X_train, y_train, X_valid, y_valid):
    # Define hyperparameter search space and sample a set of parameters from it
    params = {
      'n_estimators': trial.suggest_int('n_estimators', 10, 150),  
      'max_depth': trial.suggest_int('max_depth', 3, 10),  
      'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
      'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
      'subsample': trial.suggest_float('subsample', 0.6, 1.0),
      'lambda': trial.suggest_float('lambda', 1e-8, 3.0, log=True),
      'gamma': trial.suggest_float('gamma', 0.0, 0.3), 
      'min_child_weight': trial.suggest_int('min_child_weight', 1, 5)
    }

    run_name = f'Model for {GROUP_COL}={X_train[GROUP_COL].iloc[0]}'
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name, nested=True) as run:
      return train_and_log(X_train, y_train, X_valid, y_valid, log_group=True, log_model=False, **params)['f1_score']


# Run hyperparameter search for a single group
def run_optimization(grandparent_run_id, pdf: pd.DataFrame):
    group_key = pdf[GROUP_COL].iloc[0]
    X, y = pdf[FEATURE_COLS], pdf[TARGET_COL]
    X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, random_state=42)

    run_name = f'Hyperparameter search for {GROUP_COL}={group_key}'
    tags = {MLFLOW_PARENT_RUN_ID: grandparent_run_id}  

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id, nested=True, tags=tags) as parent_run:
      try:
        # Run hyperparameter search
        study = optuna.create_study(direction="maximize")
        study.optimize(
          lambda trial: objective(trial, X_train, y_train, X_valid, y_valid),
          n_trials=N_TRIALS)

        # Retrain and log the best model
        result = train_and_log(
          X_train, y_train, X_valid, y_valid,
          log_group=True, log_model=True,
          **study.best_trial.params)

        # Return metadata for the best model
        return pd.DataFrame([result])
      except Exception as e:
        return pd.DataFrame(columns=[field.name for field in schema.fields], data=[[group_key, str(e)] + [None] * 7])

# COMMAND ----------

with mlflow.start_run(experiment_id=experiment_id, run_name='Optimal Wrapper Model') as grandparent_run:
    # Run hyperparameter search for every single group. The resulting metadata contains references to the best mlflow runs.
    _run_optimization = partial(run_optimization, grandparent_run.info.run_id)
    pdf_results = train_data.repartition(N_GROUPS, GROUP_COL).groupBy(GROUP_COL).applyInPandas(_run_optimization, schema=schema).toPandas()

    # Package all best models referenced via `model_uri` in the metadata.
    model = WrapperModel()
    artifacts = {row[GROUP_COL]: row['model_uri'] for _, row in pdf_results.iterrows()}
    mlflow.pyfunc.log_model('model', python_model=model, artifacts=artifacts, signature=signature)

optimal_model_uri = f'runs:/{grandparent_run.info.run_id}/model'

display(pdf_results)

# COMMAND ----------

# MAGIC %md # Batch inference using the three models

# COMMAND ----------

from pyspark.sql.types import StringType

# Retrieve all 3 models we have trained as Spark UDFs
global_model_udf       = mlflow.pyfunc.spark_udf(spark, global_model_uri, result_type=StringType())
segmented_model_udf = mlflow.pyfunc.spark_udf(spark, segmented_model_uri, result_type=StringType())
optimal_model_udf      = mlflow.pyfunc.spark_udf(spark, optimal_model_uri, result_type=StringType())

(test_data
 # Run inference with all 3 models we have trained
 .withColumn('prediction_global',       global_model_udf(*FEATURE_COLS))
 .withColumn('prediction_segmented', segmented_model_udf(*FEATURE_COLS))
 .withColumn('prediction_optimal',      optimal_model_udf(*FEATURE_COLS))
 # Reorder columns for readability
 .select(['ID', 'group', 'target', 'prediction_global', 'prediction_segmented', 'prediction_optimal']
         + [col for col in test_data.columns if col.startswith('num_')])
 .createOrReplaceTempView('predictions'))

display(spark.table('predictions').limit(10))

# COMMAND ----------

# MAGIC %md # Analysis
# MAGIC
# MAGIC In the final stages of our experiments, we focus on evaluating and comparing the performance of the models across different configurations—global, segmented, and optimal. This comprehensive analysis provides insights into how well each approach captures group-level nuances and performs overall.
# MAGIC
# MAGIC We begin by calculating key performance metrics—precision, recall, and F1 score—for each model on a per-group basis. By grouping the results, we can assess how each model performs for specific subsets of the data. These metrics are then aggregated to allow for a holistic comparison of model performance across the entire dataset.

# COMMAND ----------

# MAGIC %sql select
# MAGIC   `group`,
# MAGIC   sum(case when prediction_global = `target` and `target` = 1 then 1 else 0 end) as TP,
# MAGIC   sum(case when prediction_global = `target` and `target` = 0 then 1 else 0 end) as TN,
# MAGIC   sum(case when prediction_global != `target` and prediction_global = 1 then 1 else 0 end) as FP,
# MAGIC   sum(case when prediction_global != `target` and prediction_global = 0 then 1 else 0 end) as FN,
# MAGIC   TP + FN as P,
# MAGIC   TN + FP as N,
# MAGIC   TP + FP as PP,
# MAGIC   TN + FN as PN,
# MAGIC   TP / P as recall,
# MAGIC   TP / PP as precision,
# MAGIC   (TP + TN) / (P + N) as accuracy,
# MAGIC   2 * precision * recall / (precision + recall) as f1
# MAGIC from predictions
# MAGIC group by `group`
# MAGIC   

# COMMAND ----------

from pyspark.sql.functions import col
from sklearn.metrics import f1_score, precision_score, recall_score

# Convert Spark DataFrame to Pandas DataFrame
predictions_df = spark.table('predictions').select(
    'group', 'target', 'prediction_global', 'prediction_segmented', 'prediction_optimal'
).toPandas()

# Convert predictions to integers
predictions_df['prediction_global'] = predictions_df['prediction_global'].astype(int)
predictions_df['prediction_segmented'] = predictions_df['prediction_segmented'].astype(int)
predictions_df['prediction_optimal'] = predictions_df['prediction_optimal'].astype(int)

# Calculate F1 scores grouped by 'group' and pivot the results
scores = predictions_df.groupby('group').apply(
    lambda df: pd.DataFrame({
        'model': ['global', 'segmented', 'optimal'],
        **{name: [fun(df['target'], df[model_name])
                  for model_name in ('prediction_global', 'prediction_segmented', 'prediction_optimal')]
           for name, fun in (('f1_score', f1_score), ('precision', precision_score), ('recall', recall_score))}
    })
).reset_index(level=1, drop=True).reset_index()

display(scores)

# COMMAND ----------

predictions_df.head()

# COMMAND ----------

from sklearn.metrics import precision_score, recall_score, f1_score

precision_global = precision_score(predictions_df["target"], predictions_df["prediction_global"])
recall_global = recall_score(predictions_df["target"], predictions_df["prediction_global"])
f1_global = f1_score(predictions_df["target"], predictions_df["prediction_global"])

precision_segmented = precision_score(predictions_df["target"], predictions_df["prediction_segmented"])
recall_segmented = recall_score(predictions_df["target"], predictions_df["prediction_segmented"])
f1_segmented = f1_score(predictions_df["target"], predictions_df["prediction_segmented"])

precision_optimal = precision_score(predictions_df["target"], predictions_df["prediction_optimal"])
recall_optimal = recall_score(predictions_df["target"], predictions_df["prediction_optimal"])
f1_optimal = f1_score(predictions_df["target"], predictions_df["prediction_optimal"])

# COMMAND ----------

overall_metrics = pd.DataFrame({
    "Model": ["global", "segmented", "optimal"],
    "Precision": [precision_global, precision_segmented, precision_optimal],
    "Recall": [recall_global, recall_segmented, recall_optimal],
    "F1-Score": [f1_global, f1_segmented, f1_optimal]
})

# COMMAND ----------

overall_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualisations
# MAGIC
# MAGIC By juxtaposing the results, we can clearly see the impact of group-specific training and hyperparameter optimisation on both group-level and overall performance.

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df_melt = scores.melt(
    id_vars=["group", "model"], 
    value_vars=["f1_score", "precision", "recall"],
    var_name="metric", 
    value_name="value"
)

g = sns.catplot(
    data=df_melt,
    x="group",
    y="value",
    hue="model",
    col="metric",   # each metric in its own column
    kind="bar",
    height=4,
    aspect=1.2,
    ci=None
)
g.set_xticklabels(rotation=90)
g.set_axis_labels("Group", "Score")

g._legend.set_bbox_to_anchor((1.05, 1))
g._legend.set_frame_on(True)
g._legend.set_title("Model")

plt.tight_layout()
plt.show()



# COMMAND ----------

sns.barplot(x="group", y="f1_score", hue="model", data=scores, ci=None)
plt.xticks(rotation=90)
plt.title("F1 Score by Group and Model")
plt.ylabel("F1 Score")
plt.xlabel("Group")
legend = plt.legend(title="Model")
legend.set_bbox_to_anchor((1.05, 1))
legend.set_frame_on(True)
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Registering the model in Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

catalog = "kyra_wulffert"
schema = "default"
model_name = "Optimal"
UC_MODEL_NAME = f"{catalog}.{schema}.{model_name}"

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=optimal_model_uri, name=UC_MODEL_NAME)