# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation

# COMMAND ----------

# MAGIC %pip install faker=="36.1.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
import numpy as np
import random
import uuid

from datetime import datetime
from faker import Faker
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %run ./_resources/00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate structured data for clustering

# COMMAND ----------

# Set seed
np.random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC We need to use conditional probabilities in our data gen code in order to "force" the clusters for later.

# COMMAND ----------

# Defining our tribes and sizes
tribe_sizes = {
  "The Innovators (Tech-Savvy Professional)": 250,
  "The Homebodies (Suburban Family-Oriented)": 250,
  "The Quiet Seekers (Retired Rural Dweller)": 150,
  "The Campus Creatives (College Student)": 150,
  "The Luxe Lifers (High-Income Empty Nester)": 200,
}

# COMMAND ----------

# Function to generate correlated data per tribe
def generate_tribe_data(tribe_name, size):
    if "Tech-Savvy Professional" in tribe_name:
        ages = np.random.randint(25, 35, size)
        incomes = np.random.normal(50000, 10000, size).clip(30000, 150000)
        locations = random.choices(["Santa Monica", "Venice", "Downtown LA"], k=size)
        education_levels = np.random.choice(["Bachelor's", "Post Graduate"], size, p=[0.6, 0.4])
        relationship_statuses = np.random.choice(["Single", "Cohabiting"], size, p=[0.6, 0.4])
        number_dependants = np.random.choice([0, 1], size, p=[0.8, 0.2])
        occupations = random.choices(["Software Engineer", "UX/UI Designer", "Product Manager"], k=size)

    elif "Suburban Family-Oriented" in tribe_name:
        ages = np.random.randint(35, 50, size)
        incomes = np.random.normal(50000, 10000, size).clip(40000, 150000)
        locations = random.choices(["Glendale", "Pasadena", "Burbank"], k=size)
        education_levels = np.random.choice(["Some College", "Bachelor's", "Post Graduate"], size, p=[0.3, 0.5, 0.2])
        relationship_statuses = ["Cohabiting"] * size
        number_dependants = np.random.choice([1, 2, 3, 4], size, p=[0.3, 0.4, 0.2, 0.1])
        occupations = random.choices(["School Teacher", "Stay-at-home Parent", "Doctor"], k=size)

    elif "Retired Rural Dweller" in tribe_name:
        ages = np.random.randint(60, 81, size)
        incomes = np.random.normal(40000, 5000, size).clip(20000, 60000)
        locations = random.choices(["Topanga", "Malibu", "Agoura Hills"], k=size)
        education_levels = np.random.choice(["High School", "Some College", "Bachelor's", "Post Graduate"], size, p=[0.5, 0.3, 0.1, 0.1])
        relationship_statuses = np.random.choice(["Cohabiting", "Widowed"], size, p=[0.7, 0.3])
        number_dependants = np.random.choice([0, 1], size, p=[0.8, 0.2])
        occupations = ["Retired"] * size

    elif "College Student" in tribe_name:
        ages = np.random.randint(18, 22, size)
        incomes = np.random.normal(20000, 3000, size).clip(0, 40000)
        locations = random.choices(["Westwood", "Silver Lake", "Echo Park"], k=size)
        education_levels = ["Some College"] * size
        relationship_statuses = ["Single"] * size
        number_dependants = [0] * size
        occupations = random.choices(["Student", "Intern", "Part-time Worker"], k=size)

    elif "High-Income Empty Nester" in tribe_name:
        ages = np.random.randint(50, 65, size)
        incomes = np.random.normal(120000, 20000, size).clip(80000, 200000)
        locations = random.choices(["Beverly Hills", "Bel Air", "Brentwood"], k=size)
        education_levels = np.random.choice(["Bachelor's", "Post Graduate"], size, p=[0.5, 0.5])
        relationship_statuses = ["Cohabiting"] * size
        number_dependants = [0] * size
        occupations = random.choices(["Corporate Executive", "Investment Banker", "Lawyer"], k=size)

    return pd.DataFrame({
        "age": ages,
        "income": incomes.round(-3),
        "location": locations,
        "education": education_levels,
        "relationship_status": relationship_statuses,
        "number_dependants": number_dependants,
        "occupation": occupations,
        "tribe": tribe_name
    })

# COMMAND ----------

# Generate data for all tribes
tribe_dfs = [generate_tribe_data(tribe, size) for tribe, size in tribe_sizes.items()]
demographic_df = pd.concat(tribe_dfs, ignore_index=True)

# Shuffle data
demographic_df = demographic_df.sample(frac=1).reset_index(drop=True)

# Add UUID
demographic_df.insert(0, 'uuid', [str(uuid.uuid4()) for _ in range(len(demographic_df))])

# COMMAND ----------

demographic_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional Step: Plot Locations
# MAGIC *Requires Mapbox access token*

# COMMAND ----------

import plotly.express as px

# Set your Mapbox access token
px.set_mapbox_access_token("") # ADD TOKEN

# Define location coordinates
location_coords = {
    "Santa Monica": (34.0195, -118.4912),  # The Innovators
    "Venice": (33.9850, -118.4695),  # The Innovators
    "Downtown LA": (34.0522, -118.2437),  # The Innovators

    "Glendale": (34.1426, -118.2551),  # The Homebodies
    "Pasadena": (34.1466, -118.1445),  # The Homebodies
    "Burbank": (34.1808, -118.3082),  # The Homebodies

    "Topanga": (34.0934, -118.5984),  # The Quiet Seekers
    "Malibu": (34.0259, -118.7798),  # The Quiet Seekers
    "Agoura Hills": (34.1443, -118.7815),  # The Quiet Seekers

    "Westwood": (34.0561, -118.4290),  # The Campus Creatives
    "Silver Lake": (34.0872, -118.2707),  # The Campus Creatives
    "Echo Park": (34.0782, -118.2606),  # The Campus Creatives

    "Beverly Hills": (34.0736, -118.4004),  # The Luxe Lifers
    "Bel Air": (34.1000, -118.4614),  # The Luxe Lifers
    "Brentwood": (34.0479, -118.4750),  # The Luxe Lifers
}

# Add latitude and longitude to the dataframe
demographic_df['latitude'] = demographic_df['location'].map(lambda x: location_coords.get(x.split('(')[0].strip(), (None, None))[0])
demographic_df['longitude'] = demographic_df['location'].map(lambda x: location_coords.get(x.split('(')[0].strip(), (None, None))[1])

# Check if latitude and longitude columns were added correctly
if demographic_df[['latitude', 'longitude']].isnull().any().any():
    print("Warning: Some locations do not have coordinates in the dictionary.")
    missing_locations = set(demographic_df['location'].map(lambda x: x.split('(')[0].strip())) - set(location_coords.keys())
    print("Missing locations:", missing_locations)

fig = px.scatter_mapbox(
    demographic_df,
    lat="latitude",
    lon="longitude",
    color="tribe",
    size="income",
    hover_name="tribe",
    hover_data=["age", "occupation", "education"],
    zoom=10,
    height=600
)

fig.update_layout(
    mapbox_style="mapbox://styles/mapbox/streets-v12",
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    legend=dict(
        orientation="h",  # Horizontal orientation
        yanchor="bottom",  # Position at the bottom
        y=1.02,  # Slightly above the bottom
        xanchor="right",  # Align to the right
        x=1,  # Position at the right edge
        font=dict(size=8)  # Reduce font size
    )
)

fig.show()

# COMMAND ----------

demographic_df.drop(columns=["latitude", "longitude"], inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate paid reviews

# COMMAND ----------

# Get random sample from demographic data
sampled_df = demographic_df.sample(n=100).reset_index(drop=True)

# COMMAND ----------

# Define tribe-specific products and possible emotions
tribe_products = {
    "The Innovators (Tech-Savvy Professional)": ["smartphone", "laptop", "wireless earbuds", "smartwatch", "portable charger"],
    "The Homebodies (Suburban Family-Oriented)": ["family SUV", "grill", "home security system", "washing machine", "family board game"],
    "The Quiet Seekers (Retired Rural Dweller)": ["gardening tools", "golf clubs", "outdoor furniture", "fishing gear", "hiking boots"],
    "The Campus Creatives (College Student)": ["backpack", "coffee maker", "gaming console", "textbooks", "bicycle"],
    "The Luxe Lifers (High-Income Empty Nester)": ["luxury watch", "high-end camera", "luxury car", "premium wine", "holiday package"]
}

emotions = ["excited", "angry", "satisfied", "frustrated", "disappointed", "overwhelmed", "relaxed", "confused", "amazed", "curious"]

# Generate 100 unique combinations
combinations = []
unique_combinations = set()

while len(unique_combinations) < 100:
    tribe = random.choice(list(tribe_products.keys()))
    author_id = demographic_df[demographic_df["tribe"] == tribe]["uuid"].sample(1).values[0]
    product = random.choice(tribe_products[tribe])
    emotion = random.choice(emotions)

    # Create a tuple to check for uniqueness
    combination_tuple = (tribe, product, emotion)

    # Add only if the combination is unique
    if combination_tuple not in unique_combinations:
        unique_combinations.add(combination_tuple)
        combinations.append({
            "author_id": author_id,
            "tribe": tribe,
            "product": product,
            "emotion": emotion
        })

# Convert to DataFrame
combinations_df = pd.DataFrame(combinations)
combinations_sdf = spark.createDataFrame(combinations_df)

# COMMAND ----------

display(combinations_sdf.groupBy("tribe").count())

# COMMAND ----------

# Creat temp view for AI_QUERY
combinations_sdf.createOrReplaceTempView("sampled_audience")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sampled_audience_reviews AS
# MAGIC SELECT
# MAGIC   author_id,
# MAGIC   AI_QUERY(
# MAGIC     "databricks-meta-llama-3-3-70b-instruct", 
# MAGIC     "Generate a realistic paid review from a consumer who recently purchased a " || product||  "from the perspective of a " || tribe || "who is " || emotion || "about the product. The review should reflect their genuine experience, including specific details about the product's features, performance, and how it fits into their lifestyle. Maintain a conversational and engaging tone, similar to how people naturally write a review. Optionally, include a hashtag or emoji for authenticity. Don't explicitly mention the segment or that you are an AI assistant. Remove quotation marks."
# MAGIC   ) AS review
# MAGIC FROM sampled_audience

# COMMAND ----------

reviews_df = spark.sql("select * from sampled_audience_reviews").toPandas()

# COMMAND ----------

display(reviews_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Ad Campaigns

# COMMAND ----------

# We can re-use the tribe_products dict from earlier and add tone, ctas to create variation in ad copy
ad_tones = ["Exciting", "Informative", "Persuasive", "Trustworthy"]
ctas = ["Shop Now", "Hurry - Limited Time Offer", "Discover More", "Upgrade Today", "Claim Your Deal"]

# Generate campaigns
campaigns = []
campaign_counter = 1

for tribe, products in tribe_products.items():
    for product in products:
        for tone in ad_tones:
            campaign_id = f"AD-{random.randint(1000,9999)}"  # Format as AD-0001
            cta = random.choice(ctas) # Random CTA
            ctr = round(random.uniform(5.0, 15.0), 2)
            impressions = random.randint(50000, 500000)

            campaigns.append((campaign_id, tribe, product, tone, cta, ctr, impressions))
            campaign_counter += 1

# Convert to DataFrame
campaigns_df = pd.DataFrame(campaigns, columns=["campaign_id", "tribe", "product", "tone", "cta", "ctr", "impressions"])

campaigns_sdf = spark.createDataFrame(campaigns_df)

# COMMAND ----------

# Creat temp view for AI_QUERY
campaigns_sdf.createOrReplaceTempView("campaigns")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW campaigns_performance AS
# MAGIC SELECT
# MAGIC   campaign_id,
# MAGIC   tribe,
# MAGIC   AI_QUERY(
# MAGIC     "databricks-meta-llama-3-3-70b-instruct", 
# MAGIC     "Write a unique and persuasive online advertisement for a " || product || ". The ad should be targeted at " || tribe || ", highlighting key benefits. The tone should be " || tone || ". and the ad should include a compelling call-to-action that encourages the user to " || cta || ". Ensure creativity, keep it concise, clear, and optimised for digital platforms like Facebook, Instagram or Google Ads. Don't state the segment name. Use an emoji if appropriate. Remove quotation marks. Don't include the CTA button in the response."
# MAGIC   ) AS ad_copy,
# MAGIC   impressions,
# MAGIC   ctr
# MAGIC FROM campaigns

# COMMAND ----------

campaigns_performance_df = spark.sql("select * from campaigns_performance").toPandas()

# COMMAND ----------

display(campaigns_performance_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write paid reviews to volume JSON and save demographic + campaign tables

# COMMAND ----------

fake = Faker()

# Generate post id and creation date
reviews_df.insert(0, 'id', [str(uuid.uuid4()) for _ in range(len(reviews_df))])
reviews_df['created_at'] = [
  fake.date_time_between(datetime(2024, 1, 1), datetime(2024, 12, 31)).strftime('%Y-%m-%d %H:%M:%S') for _ in range(len(reviews_df))]

# COMMAND ----------

# Write paid reviews to volume
reviews_df.to_json(config['vol_reviews'], orient='records')

# COMMAND ----------

# Write demographic data to UC table dropping tribe
demographic_sdf = spark.createDataFrame(demographic_df)
demographic_sdf = demographic_sdf.drop("tribe")
demographic_sdf.write.format("delta").mode("overwrite").saveAsTable("audience_demographic")

# COMMAND ----------

# Write campaigns data to UC table
campaigns_performance_sdf = spark.createDataFrame(campaigns_performance_df)
campaigns_performance_sdf.write.format("delta").mode("overwrite").saveAsTable(f"campaigns_performance")
