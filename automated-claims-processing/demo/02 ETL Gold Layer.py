# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Notebook: 02 ETL Gold Layer
# MAGIC
# MAGIC This notebook implements the **Gold Layer (AI-Powered Analytics)** of the solution accelerator, transforming transcribed call data into enriched insights using **Databricks AI Functions** and **provisionless batch inference**.
# MAGIC
# MAGIC It introduces structured AI outputs like sentiment, summaries, classifications, named entities, and even generates professional follow-up emails — ready for downstream workflows or customer engagement.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Purpose
# MAGIC
# MAGIC To apply **advanced AI inference** on transcribed customer calls and output enriched, actionable insights in a format that can be:
# MAGIC - Embedded in dashboards
# MAGIC - Trigger customer communications
# MAGIC - Drive operational decisions

# COMMAND ----------

# DBTITLE 1,Restart for clean environment
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Initial Resources for Notebook Execution
# MAGIC %run "./resources/init" 

# COMMAND ----------

# DBTITLE 1,Load Silver Layer Data
silver_df = spark.table(f"{CATALOG}.{SCHEMA}.{SILVER_TABLE}")
meta_path = f"{CATALOG}.{SCHEMA}.{META_TABLE}"

if spark._jsparkSession.catalog().tableExists(meta_path):
    processed_df = spark.table(meta_path).filter("processed = True")
    df = silver_df.join(processed_df, "file_name", "left_anti")
else:
    df = silver_df

if df.isEmpty():
    dbutils.notebook.exit("✅ No new files to process. Exiting Silver Layer.")

# COMMAND ----------

# DBTITLE 1,Construct Parameters for AI Functions
# Prompt template for ai_query()
prompt = """Using the following call transcript, generate a professional yet friendly email from the agent to the customer. The email should summarize the key points of the conversation, including the reason for the call, the resolution provided, and any necessary next steps for the customer. The tone should be courteous, clear, and supportive.

Email Structure:
- Subject Line: A clear and concise subject summarizing the purpose of the email (e.g., “Follow-up on Your Prescription Claim - VitalGuard Health Insurance”).
- Greeting: A friendly yet professional greeting addressing the customer by name.
- Call Summary: A recap of the discussion, including the inquiry and the response of the agent.
- Next Steps: A clear outline of any actions the customer needs to take (e.g., contacting their doctor, submitting forms, waiting for updates).
- Contact Information: An invitation for the customer to reach out if they have further questions.
- Closing: A polite and professional closing with the name of the agent and company details.

Call Transcript:
\n
"""

# Response schema for structured email output
response_format = '''{
    "type": "json_schema",
    "json_schema": {
        "name": "vitalguard_call_followup_email",
        "schema": {
            "type": "object",
            "properties": {
                "subject": {
                    "type": "string",
                    "description": "The subject line of the email summarizing the purpose of the follow-up."
                },
                "greeting": {
                    "type": "string",
                    "description": "A friendly yet professional greeting addressing the customer by name."
                },
                "call_summary": {
                    "type": "string",
                    "description": "A summary of the inquiry of the customer and the response given by the agent."
                },
                "next_steps": {
                    "type": "string",
                    "description": "Clear and concise next steps that the customer needs to take, if applicable."
                },
                "contact_information": {
                    "type": "string",
                    "description": "Details on how the customer can reach out for further assistance."
                },
                "closing": {
                    "type": "string",
                    "description": "A polite closing statement including the name of the agent and company details."
                }
            },
            "required": [
                "subject",
                "greeting",
                "call_summary",
                "next_steps",
                "contact_information",
                "closing"
            ]
        },
        "strict": true
    }
}'''

# Fetch call reason categories from lookup table
reasons_df = spark.table(f"{CATALOG}.{SCHEMA}.call_centre_reasons")
reasons_array = [f"'{row['reason_for_call']}'" for row in reasons_df.select("reason_for_call").distinct().collect()]
reasons_sql = ", ".join(reasons_array)

# Define NER targets
ner_targets = ["firstName_lastName", "dateOfBirth_yyyy-mm-dd", "policy_number"]
ner_sql = ", ".join([f"'{x}'" for x in ner_targets])

# COMMAND ----------

# DBTITLE 1,Apply AI Enrichments via SQL
df.createOrReplaceTempView("transcriptions_temp")

query = f"""
SELECT *,
       ai_analyze_sentiment(transcription) AS sentiment,
       ai_summarize(transcription) AS summary,
       ai_classify(transcription, ARRAY({reasons_sql})) AS classification,
       ai_extract(transcription, ARRAY({ner_sql})) AS ner,
       ai_query('{ENDPOINT_NAME}', CONCAT('{prompt}', transcription), responseFormat => '{response_format}') AS email_response
FROM transcriptions_temp
"""

enriched_df = spark.sql(query)

# Add masking layer
enriched_df.createOrReplaceTempView("enriched_temp")

query_masked = """
SELECT *,
       ai_mask(summary, ARRAY('person', 'address')) AS summary_masked
FROM enriched_temp
"""

final_enriched_df = spark.sql(query_masked)

display(final_enriched_df)

# COMMAND ----------

# DBTITLE 1,Flatten NER Output & Cleanup
from pyspark.sql.functions import col

flattened_df = final_enriched_df \
    .withColumn("customer_name", col("ner.firstName_lastName")) \
    .withColumn("birth_date", col("ner.dateOfBirth_yyyy-mm-dd")) \
    .withColumn("policy_number", col("ner.policy_number")) \
    .drop("modificationTime", "transcription", "ner")

display(flattened_df)

# COMMAND ----------

# DBTITLE 1,Write Gold Layer Table
gold_table_path = f"{CATALOG}.{SCHEMA}.{GOLD_TABLE}"

if not spark._jsparkSession.catalog().tableExists(gold_table_path):
    flattened_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table_path)
else:
    flattened_df.write.mode("append").saveAsTable(gold_table_path)

print(f"✅ AI Enriched insights written to {GOLD_TABLE}")

# COMMAND ----------

# DBTITLE 1,Process and Save Metadata with File Names
from pyspark.sql.functions import lit

file_names_df = flattened_df.select("file_name").distinct().withColumn("processed", lit(True))

if spark._jsparkSession.catalog().tableExists(meta_path):
    current_meta_df = spark.table(meta_path)
    updated_meta_df = current_meta_df.unionByName(file_names_df).dropDuplicates(["file_name"])
else:
    updated_meta_df = file_names_df

# Overwrite metadata table with updated status
updated_meta_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(meta_path)

print(f"📌 Metadata table updated at: {meta_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Output
# MAGIC **Table: analysis_gold**
# MAGIC
# MAGIC Includes:
# MAGIC - AI sentiment classification
# MAGIC - Summaries (original and masked)
# MAGIC - Call reason classification
# MAGIC - Extracted customer info
# MAGIC - Structured JSON for follow-up emails

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏭ Next Step
# MAGIC
# MAGIC Consume this enriched dataset in:
# MAGIC - **Dashboards** (e.g., sentiment trends, fraud alerts, agent metrics)
# MAGIC - **Case management systems**
# MAGIC - **Automated email or notification APIs**