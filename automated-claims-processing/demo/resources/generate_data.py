# Databricks notebook source
# MAGIC %run ./init

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import Row

# Generate 70 unique call records
num_records = 70

# Generate random datetime within the past 30 days
def random_datetime():
    return datetime.now() - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23), minutes=random.randint(0, 59))

# Generate a unique call ID
def generate_call_id():
    return str(uuid.uuid4())[:8]  # Shortened UUID for readability

# Generate random agent IDs (5 agents)
agent_ids = [f"AGT{str(i).zfill(3)}" for i in range(1, 6)]

# Generate random customer names (unique)
first_names = ["John", "Sarah", "Michael", "Emily", "David", "Anna", "James", "Sophia", "Robert", "Olivia",
               "Daniel", "Emma", "Matthew", "Isabella", "Andrew", "Mia", "Joshua", "Charlotte", "William", "Amelia",
               "Alexander", "Evelyn", "Henry", "Abigail", "Samuel", "Ella", "Joseph", "Scarlett", "Benjamin", "Grace",
               "Nicholas", "Chloe", "Ethan", "Lily", "Logan", "Hannah", "Christopher", "Zoe", "Nathan", "Avery",
               "Ryan", "Madison", "Jack", "Layla", "Luke", "Nora", "Dylan", "Riley", "Caleb", "Aria"]

last_names = ["Smith", "Johnson", "Brown", "Taylor", "Anderson", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
              "Clark", "Lewis", "Young", "Walker", "Allen", "King", "Wright", "Scott", "Green", "Baker",
              "Adams", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Hall", "Evans", "Campbell", "Miller",
              "Davis", "Garcia", "Rodriguez", "Martinez", "Lopez", "Harris", "Gomez", "Diaz", "Torres", "Flores",
              "Sanchez", "Reed", "Stewart", "Murphy", "Howard", "Brooks", "Gray", "Murray", "Ford", "Castro"]

# Ensure we have exactly num_records unique customer names
customer_names = list(set([f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_records * 2)]))[:num_records]

# Generate random DOB (ages between 69 and 19)
def random_dob():
    return (datetime.now() - timedelta(days=random.randint(19 * 365, 69 * 365))).date()

# Generate policy numbers
def generate_policy_number():
    return f"VG{random.randint(100000, 999999)}"

# Sentiment categories
sentiments = ["Happy", "Neutral", "Frustrated", "Angry", "Confused"]

reasons_dict = {
    "Claim status inquiry": "Provide claim status update",
    "Coverage details request": "Explain coverage details",
    "Billing and premium question": "Assist with billing",
    "Finding in-network provider": "Find in-network provider",
    "Policy renewal": "Initiate policy renewal",
    "Updating personal details": "Update customer details",
    "Technical support": "Provide technical support",
    "Filing a new claim": "File new claim request",
    "Canceling a policy": "Process policy cancellation"
}

financial_hardship_dict = {
    "Requesting premium payment deferral due to financial hardship": "Review eligibility for payment deferral",
    "Inquiry about hardship assistance programs": "Explain available financial hardship assistance options",
    "Request to lower coverage temporarily due to income loss": "Adjust policy coverage as requested"
}

fraud_dict = {
    "Fraudulent claim attempt": "Escalate suspected fraud"
}

# Combine all reasons for general selection
all_reason_mappings = list(reasons_dict.items())

# Generate call durations (up to 5 minutes max)
def random_call_duration():
    return random.randint(30, 300)  # 30 sec to 5 min

# Assign 10 fraud and 3 financial hardship randomly
fraud_indices = set(random.sample(range(num_records), 10))
remaining_indices = list(set(range(num_records)) - fraud_indices)
hardship_indices = set(random.sample(remaining_indices, 3))

# Generate the dataframe
call_data = []
for i in range(num_records):
    if i in fraud_indices:
        reason, step = list(fraud_dict.items())[0]
    elif i in hardship_indices:
        reason, step = random.choice(list(financial_hardship_dict.items()))
    else:
        reason, step = random.choice(all_reason_mappings)

    call_data.append(Row(
        datetime=random_datetime(),
        call_id=generate_call_id(),
        agent_id=random.choice(agent_ids),
        customer_name=customer_names[i],
        dob=random_dob(),
        policy_number=generate_policy_number(),
        sentiment=random.choice(sentiments),
        reason_for_call=reason,
        next_steps=step,
        duration_sec=random_call_duration()
    ))

# Convert to Spark DataFrame
df_calls = spark.createDataFrame(call_data)
display(df_calls)

# COMMAND ----------

from pyspark.sql.functions import when

# Combine all_reason_mappings, financial_hardship_dict, and fraud_dict
combined_reason_mappings = all_reason_mappings + list(financial_hardship_dict.items()) + list(fraud_dict.items())

# Convert combined_reason_mappings into a DataFrame
df_reasons = spark.createDataFrame(combined_reason_mappings, ["reason_for_call", "next_steps"])

# Update the rows where reason_for_call comes from financial_hardship_dict to 'Financial hardship'
df_reasons = df_reasons.withColumn(
    "reason_for_call",
    when(df_reasons["reason_for_call"].isin(list(financial_hardship_dict.keys())), "Financial hardship").otherwise(df_reasons["reason_for_call"])
)

display(df_reasons)

# Save the DataFrame as a table
df_reasons.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.call_centre_reasons")

# COMMAND ----------

# DBTITLE 1,VitalGuard Customer Call Handling Template
prompt_template = """
CONCAT('Context:  
You are an agent working for **VitalGuard**, a UK-based health insurance provider. You are handling a customer service call at the VitalGuard call centre. The conversation should be professional, clear, and helpful, following the standard call flow.

---

### Customer Details:  
- Customer Name: ' ,customer_name, '\n  
- Date of Birth: ' ,dob, '\n  
- Policy Number: ' ,policy_number, '\n  
- Customer Sentiment: ' ,sentiment, '\n  

---

### Call Scenario:  
- Reason for Call: ' ,reason_for_call, '\n  
- Next Steps: ' ,next_steps, '\n  
- Call Duration: ' ,duration_sec, ' seconds  

---

### Transcript Guidelines:  
Follow the structured conversation format:  
1. **Greeting & Verification:** The agent greets the customer and verifies their identity (name, DOB, policy number).  
2. **Understanding the Issue:** The agent asks clarifying questions based on the **Reason for Call**.  
3. **Providing Assistance:** The agent gives solutions, answers questions, and explains the next steps clearly.  
4. **Handling Concerns:** If the customer is **frustrated, concerned, or angry**, the agent should use a calm and reassuring tone. If the customer is **happy or relieved**, the agent should maintain an upbeat and professional tone.  
5. **Summarizing & Closing:** The agent recaps the next steps and ends the call politely.  

---

### Example Input:  
Customer Name: Emma Johnson  
Date of Birth: 07/22/1990  
Policy Number: VG434271  
Customer Sentiment: Happy  
Reason for Call: Seeking advice on coverage  
Next Steps: Provide complaint reference number  
Call Duration: 188 seconds  

---

### Example Output:  

Agent: "Hello, thank you for calling VitalGuard. My name is Agent. How can I assist you today?"  

Customer: "Hi, this is Emma Johnson. I was hoping to get some advice on my coverage options."  

Agent: "Of course, Emma. For security, can I confirm your date of birth and policy number?"  

Customer: "Sure, my DOB is 22nd July 1990, and my policy number is VG434271."  

Agent: "Thank you for confirming. What kind of coverage details are you looking for?"  

Customer: "I want to know if my policy covers physiotherapy sessions."  

Agent: "Let me check that for you... Yes, your current plan includes up to 10 physiotherapy sessions per year, but they need to be prescribed by a GP."  

Customer: "That is great to know! Also, if I need to make a complaint about a billing issue, how do I do that?"  

Agent: "I understand. I will provide you with a complaint reference number, and you can submit your complaint through our online portal or over the phone. Here is your reference: VG-CMP-4823."  

Customer: "Thanks, that helps!"  

Agent: "You are very welcome. Is there anything else I can assist you with today?"  

Customer: "No, that is all for now."  

Agent: "Great! Just to summarize, I have confirmed your policy covers physiotherapy with a GP referral, and I have provided a complaint reference number for your billing issue. If you need further assistance, don’t hesitate to reach out. Have a wonderful day!"  

Customer: "Thanks! You too."  

Agent: "Goodbye!"')  
"""


response_format = """{
    "type": "json_schema",
    "json_schema": {
        "name": "vitalguard_call_transcript",
        "schema": {
            "type": "object",
            "properties": {
                "customer_name": {
                    "type": "string",
                    "description": "The full name of the customer calling the VitalGuard support centre."
                },
                "dob": {
                    "type": "string",
                    "description": "The date of birth of the customer for verification purposes."
                },
                "policy_number": {
                    "type": "string",
                    "description": "The unique policy number associated with the customer."
                },
                "sentiment": {
                    "type": "string",
                    "enum": ["Calm", "Frustrated", "Concerned", "Neutral", "Angry", "Happy", "Confused", "Relieved"],
                    "description": "The sentiment of the customer during the call."
                },
                "reason_for_call": {
                    "type": "string",
                    "description": "The primary reason the customer is calling."
                },
                "next_steps": {
                    "type": "string",
                    "description": "A summary of the next actions that will be taken following the call."
                },
                "duration_sec": {
                    "type": "number",
                    "description": "The total duration of the call in seconds."
                },
                "transcript": {
                    "type": "array",
                    "description": "A structured call transcript representing the conversation between the agent and the customer.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "speaker": {
                                "type": "string",
                                "enum": ["Agent", "Customer"],
                                "description": "Indicates whether the statement was made by the agent or the customer."
                            },
                            "message": {
                                "type": "string",
                                "description": "The specific statement made during the call."
                            }
                        },
                        "required": ["speaker", "message"]
                    }
                }
            },
            "required": [
                "customer_name",
                "dob",
                "policy_number",
                "sentiment",
                "reason_for_call",
                "next_steps",
                "duration_sec",
                "transcript"
            ]
        },
        "strict": true
    }
}"""

# COMMAND ----------

# DBTITLE 1,AI Query Execution on Call Centre Scenarios
df_calls.createOrReplaceTempView("scenarios_temp")

query = f"""
    SELECT *,
          ai_query('{ENDPOINT_NAME}', {prompt_template}, responseFormat => '{response_format}') AS script
    FROM scenarios_temp
"""

df_scripts = spark.sql(query)

display(df_scripts)

df_scripts.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.call_centre_scenarios")

# COMMAND ----------

from pyspark.sql.functions import col, get_json_object, expr, udf
from pyspark.sql.types import StringType
import json

def extract_messages(transcript):
    return ' '.join([x['message'] for x in json.loads(transcript)])

extract_messages_udf = udf(extract_messages, StringType())

# COMMAND ----------

df = spark.table(f"{CATALOG}.{SCHEMA}.call_centre_scenarios").select("datetime", "call_id", "agent_id", "duration_sec", "script")
new_df = df.withColumn("transcript", get_json_object(col("script"), "$.transcript"))
new_df = new_df.withColumn("messages", extract_messages_udf(col("transcript"))).drop("script", "transcript")
display(new_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, unix_timestamp, concat, lit, col

transcriptions_df = new_df.select(
    concat(
        lit('/Volumes/'), 
        lit(CATALOG), 
        lit('/'), 
        lit(SCHEMA), 
        lit('/audio_recordings/mp3_audio_recordings/'), 
        col("call_id"), 
        lit('_'), 
        col("agent_id"), 
        lit('_'), 
        date_format(col("datetime"), 'yyyy-MM-dd HH:mm:ss'), 
        lit('.mp3')
    ).alias("file_path"),
    concat(
        col("call_id"), 
        lit('_'), 
        col("agent_id"), 
        lit('_'), 
        date_format(col("datetime"), 'yyyy-MM-dd HH:mm:ss')
    ).alias("file_name"),
    col("messages").alias("transcription"),
    col("duration_sec").alias("audio_duration"),
    current_timestamp().alias("modificationTime")
)

transcriptions_df = transcriptions_df.withColumn("path", concat(lit('dbfs:'), col("file_path"))).withColumn("modificationTime", unix_timestamp(col("modificationTime")))


# transcriptions_df = transcriptions_df.select("path", "modificationTime", "file_path", "transcription", "audio_duration")
transcriptions_df = transcriptions_df.select("file_path", "file_name", "transcription", "audio_duration")

display(transcriptions_df)

transcriptions_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.simulated_transcriptions")