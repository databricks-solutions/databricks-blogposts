# Databricks notebook source
# MAGIC %md
# MAGIC # Audience Segmentation

# COMMAND ----------

# MAGIC %pip install threadpoolctl=="3.1.0" openai=="1.35.3" httpx=="0.27.2" --ignore-installed
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import matplotlib.pyplot as plt
import mlflow
import os
import pandas as pd
import pyspark.sql.functions as F

from openai import OpenAI
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# COMMAND ----------

# Set the registry URI manually for serverless only
mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = lambda: "databricks-uc"
mlflow.login()

# COMMAND ----------

mlflow.autolog()

# COMMAND ----------

# MAGIC %run ./_resources/00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build clustering model (KMeans)

# COMMAND ----------

# Read demographic table
demographic_df = spark.read.table(f"audience_demographic").toPandas()

display(demographic_df)

# COMMAND ----------

# Define numerical and categorical features
numerical_features = ["age", "income", "number_dependants"]
categorical_features = ["location", "education", "relationship_status", "occupation"]

# Create preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), numerical_features),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features)
    ])

X = preprocessor.fit_transform(demographic_df)

# Calculate inertia for different values of k
k_range = range(2, 8)
inertias = []

for k in k_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto")
    kmeans.fit(X)
    inertias.append(kmeans.inertia_)

# Plot the elbow curve
plt.figure(figsize=(10, 6))
plt.plot(k_range, inertias, marker="o")
plt.xlabel("Number of Clusters (k)")
plt.ylabel("Inertia")
plt.title("Elbow Method for Optimal k")

# Save and log the plot
plot_filename = "elbow_plot.png"
plt.savefig(plot_filename)

plt.show()

mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log optimal K based on elbow method

# COMMAND ----------

optimal_k = 5
kmeans_pipeline = Pipeline([
    ("preprocessor", preprocessor),
    ("kmeans", KMeans(n_clusters=optimal_k, random_state=42, n_init="auto"))
])

# COMMAND ----------

# Do final run with optimal k

with mlflow.start_run(run_name="KMeans_clustering_optimal"):
    # Fit the pipeline
    kmeans_pipeline.fit(demographic_df)
    
    # Make predictions
    labels = kmeans_pipeline.predict(demographic_df)
    
    # Evaluate the model
    silhouette = silhouette_score(kmeans_pipeline.named_steps["preprocessor"].transform(demographic_df), labels)
    
    # Log optimal k and elbow plot
    mlflow.log_param("optimal_n_clusters", optimal_k)
    mlflow.log_artifact(plot_filename)

    # Log parameters and metrics
    mlflow.log_param("numerical_features", numerical_features)
    mlflow.log_param("categorical_features", categorical_features)
    mlflow.log_metric("silhouette_score", silhouette)
    
    # Log the model
    mlflow.sklearn.log_model(kmeans_pipeline, "kmeans_model")
    
    print(f"Silhouette score: {silhouette}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate tribes

# COMMAND ----------

demographic_df["cluster"] = labels
demographic_sdf = spark.createDataFrame(demographic_df)

display(demographic_sdf)

# COMMAND ----------

display(demographic_sdf.groupBy("cluster").count())

# COMMAND ----------

# Find aggregates to understand the demographics of our clusters
tribe_summary_sdf = (
  demographic_sdf.groupby("cluster").agg(
    F.expr("percentile_approx(age, 0.5)").alias("median_age"),
    F.expr("percentile_approx(income, 0.5)").alias("median_income"),
    F.concat_ws(", ", F.collect_set("location")).alias("locations"),
    F.expr("mode() within group (order by education)").alias("mode_education"),
    F.expr("mode() within group (order by relationship_status)").alias("mode_relationship_status"),
    F.expr("percentile_approx(number_dependants, 0.5)").alias("median_number_dependants"),
    F.concat_ws(", ", F.collect_set("occupation")).alias("occupations"),
  ).orderBy("cluster")
)

display(tribe_summary_sdf)

# COMMAND ----------

# Assign names (tribes) to clusters
tribe_summary_sdf = (
  tribe_summary_sdf
    .withColumn("tribe", 
      F.when(demographic_sdf.cluster == 0, "The Luxe Lifers (High-Income Empty Nester)")
       .when(demographic_sdf.cluster == 1, "The Campus Creatives (College Student)")
       .when(demographic_sdf.cluster == 2, "The Homebodies (Suburban Family-Oriented)")
       .when(demographic_sdf.cluster == 3, "The Quiet Seekers (Retired Rural Dweller)")
       .when(demographic_sdf.cluster == 4, "The Innovators (Tech-Savvy Professional)")
    )
)

demographic_sdf = demographic_sdf.join(tribe_summary_sdf.select("cluster", "tribe"), "cluster"). drop("cluster")

# COMMAND ----------

display(demographic_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Use LLM to generate customer profiles

# COMMAND ----------

# Get social media posts data
posts_sdf = spark.read.json(config['vol_social_media_feed'])

display(posts_sdf)

# COMMAND ----------

# Join demographic and posts data
audience_sdf = demographic_sdf.join(posts_sdf, [demographic_sdf.uuid == posts_sdf.author_id], how="left")

# Aggregate the posts data by tribe
tribe_posts_sdf = audience_sdf.groupBy("tribe").agg(
  F.concat_ws("\n\n", F.collect_list("post")).alias("posts")
)

# Join to the tribe summaries from earlier
tribe_summary_sdf = tribe_summary_sdf.join(tribe_posts_sdf, "tribe")

# COMMAND ----------

display(tribe_summary_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create profiles

# COMMAND ----------

def create_prompt(tribe, age, income, locations, education, dependants, occupations, social_posts):

    prompt = f"""
        You are an expert marketing analyst tasked with creating a concise customer persona (less than 400 words). Use the provided demographic information and social media posts to describe the customer tribe’s characteristics. Use the tribe as the persona name.

        ### Demographic Information:
        - Tribe: {tribe}
        - Average Age: {age}
        - Average Income: {income}
        - Locations: {locations}
        - Education: {education}
        - Dependants: {dependants}
        - Occupations: {occupations}

        ### Aggregated Social Media Posts:
        "{social_posts}"

        ### Instructions:
        Based on the demographic data and social media content:
        1. Describe the tribe’s **core values**, and **motivations**.
        2. Highlight their **interests** and **product preferences**.
        3. Summarize any **pain points** or **common complaints**.
        4. State their **communication style** and **media preferences**.

        The profile should be fluid, easy to read, and written in a professional yet conversational style.

        ### Output Format:
        - **Persona Name:**
         
        - **Overview:**  
        - **Values & Motivations:**  
        - **Interests & Purchasing Considerations:**  
        - **Challenges & Pain Points:**  
        - **Communication Style & Media Preferences:**

        Output only the profile text without additional commentary.
        """

    return prompt

# COMMAND ----------

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

client = OpenAI(
  api_key=DATABRICKS_TOKEN,
  base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

def generate_profile(prompt):
  chat_completion = client.chat.completions.create(
    messages=[
    {
      "role": "system",
      "content": "You are an AI assistant."
    },
    {
      "role": "user",
      "content": prompt
    }
    ],
    model="databricks-meta-llama-3-3-70b-instruct",
    max_tokens=512
  )

  return chat_completion.choices[0].message.content

# COMMAND ----------

profiles = {}

# Iterate through tribes to get profile
for row in tribe_summary_sdf.collect():
  prompt = create_prompt(
    row.tribe, 
    row.median_age, 
    row.median_income, 
    row.locations, 
    row.mode_education, 
    row.median_number_dependants, 
    row.occupations, 
    row.posts
  )

  profiles[row.tribe] = generate_profile(prompt)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save profiles

# COMMAND ----------

# Save dict to JSON in volume
with open(config['vol_profiles'], "w") as f:
  json.dump(profiles, f)
