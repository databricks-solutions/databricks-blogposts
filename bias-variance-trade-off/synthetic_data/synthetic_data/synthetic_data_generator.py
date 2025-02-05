# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Dataset for Binary Classification
# MAGIC
# MAGIC This notebook generates, processes, and analyses a synthetic dataset designed for binary classification tasks. 
# MAGIC <br>The notebook emphasises group-level relationships and provides insights into statistical distributions, correlations, and visual patterns within the data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Group-Specific Polynomial Functions
# MAGIC The dataset is structured such that each group has a unique polynomial function, which determines the relationship between input features and the target variable. This approach creates a rich and varied dataset for analysis.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dataset Structure
# MAGIC 1. **Numerical Features**:
# MAGIC    - Three numerical features (`num_1`, `num_2`, `num_3`) are independently sampled from a normal distribution.
# MAGIC
# MAGIC 2. **Group Identifier**:
# MAGIC    - `group`: A numerical identifier representing the group to which each observation belongs.
# MAGIC
# MAGIC 3. **Binary Target (`target`)**:
# MAGIC    - Generated using the following steps:
# MAGIC      1. **Random Coefficients**:
# MAGIC         - For each observation, seven coefficients (`w_0` to `w_6`) are randomly drawn from a normal distribution.
# MAGIC      2. **Score Calculation**:
# MAGIC         - A unique polynomial function per group \(g\) calculates the score for each observation \(i\):
# MAGIC         <br>score_{i, g} = w_{0, g} + w_{1, g} * num_1 + w_{2, g} * (num_1^2) + w_{3, g} * num_2 + w_{4, g} * (num_2^2) + w_{5, g} * num_3 + w_{6, g} * (num_3^2)
# MAGIC      3. **Probability Transformation**:
# MAGIC         - The score is converted to a probability using the logistic function:
# MAGIC         <br>p = 1 / (1 + exp(-score))
# MAGIC      4. **Binary Target Sampling**:
# MAGIC         - The binary target (`target`) is sampled from a Bernoulli distribution, using the calculated probability as the parameter.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic Dataset Generation

# COMMAND ----------

import numpy as np
import pandas as pd


np.random.seed(42)

# Parameters
num_groups = 20
obs_per_group = 400
total_obs = num_groups * obs_per_group


group_ids = np.repeat(np.arange(num_groups), obs_per_group)

num_1 = np.random.normal(0, 1, total_obs)
num_2 = np.random.normal(0, 1, total_obs)
num_3 = np.random.normal(0, 1, total_obs)

num_coeffs = 7  # w0 through w6
group_coefs = np.random.normal(loc=0.0, scale=1.0, size=(num_groups, num_coeffs))

w0 = group_coefs[group_ids, 0]
w1 = group_coefs[group_ids, 1]
w2 = group_coefs[group_ids, 2]
w3 = group_coefs[group_ids, 3]
w4 = group_coefs[group_ids, 4]
w5 = group_coefs[group_ids, 5]
w6 = group_coefs[group_ids, 6]

score = (w0 
         + w1*num_1 + w2*(num_1**2) 
         + w3*num_2 + w4*(num_2**2) 
         + w5*num_3 + w6*(num_3**2))

p = 1 / (1 + np.exp(-score))

y = np.random.binomial(1, p, size=total_obs)

df = pd.DataFrame({
    "group": group_ids,
    "num_1": num_1,
    "num_2": num_2,
    "num_3": num_3,
    "target": y
})

# Shuffle rows for randomness
df = df.sample(frac=1).reset_index(drop=True)
df["ID"] = np.arange(len(df))

# Compute mean target per group
group_means = df.groupby("group")["target"].mean().reset_index()

threshold_min = 0.4
threshold_max = 0.6
keep_groups = group_means[(group_means["target"] >= threshold_min)&(group_means["target"] <= threshold_max)]["group"].values
df = df[df["group"].isin(keep_groups)].reset_index(drop=True)

print(df.head())
# df.to_csv("synthetic_polynomial_groups_binary.csv", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Synthetic Dataset Visualisation

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

sns.countplot(x="target", data=df)
plt.title("Global Distribution of the Target")
plt.show()

group_means = df.groupby("group")["target"].mean().reset_index()

plt.figure(figsize=(10, 6))
sns.barplot(x="group", y="target", data=group_means, order=sorted(df["group"].unique()))
plt.xticks(rotation=90)
plt.title("Mean Target Rate by Group")
plt.ylabel("Mean Target Rate")
plt.xlabel("Group")
plt.show()


# COMMAND ----------

group_summary = df.groupby("group").agg({
    "num_1": "mean",
    "num_2": "mean",
    "num_3": "mean",
    "target": "mean"
}).reset_index()

numeric_cols = ["num_1", "num_2", "num_3", "target"]
corr_matrix = group_summary[numeric_cols].corr()

plt.figure(figsize=(6, 4))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1)
plt.title("Correlation Between Group-Level Aggregates")
plt.show()


# COMMAND ----------

numeric_features = ["num_1", "num_2", "num_3"]

# COMMAND ----------

fig, axes = plt.subplots(nrows=1, ncols=len(numeric_features), figsize=(15,6), sharey=True)

for ax, feat in zip(axes, numeric_features):
    sns.boxplot(
        x="group", 
        y=feat, 
        data=df, 
        order=sorted(df["group"].unique()), 
        ax=ax
    )
    ax.set_title(f"Distribution of {feat} by Group")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

plt.tight_layout()
plt.show()


# COMMAND ----------

fig, axes = plt.subplots(nrows=1, ncols=len(numeric_features), figsize=(15,6), sharey=True)

for ax, feat in zip(axes, numeric_features):
    sns.violinplot(
        x="group", 
        y=feat, 
        data=df, 
        order=sorted(df["group"].unique()), 
        ax=ax
    )
    ax.set_title(f"Distribution of {feat} by Group")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

# plt.title("Violin Plots of numerical features by Group")
plt.tight_layout()
plt.show()

# COMMAND ----------

for col in numeric_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(df[col], kde=True, bins=30)
    plt.title(f"Distribution of {col}")
    plt.show()

# COMMAND ----------

sns.pairplot(df, hue="group", vars=["num_1", "num_2", "num_3"])


# COMMAND ----------

group_means = df.groupby("group")["target"].mean().reset_index()

plt.figure(figsize=(8, 5))
sns.barplot(x="group", y="target", data=group_means, order=sorted(df["group"].unique()))
plt.title("Mean Target Rate by Group")
plt.xlabel("Group")
plt.ylabel("Mean Target = 1")
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()