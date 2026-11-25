# Databricks notebook source
# MAGIC %md
# MAGIC # Higher Education Advisory Services — 01 Setup
# MAGIC
# MAGIC Creates the schema, Delta tables, rubric data, and registers **all 12 Unity Catalog
# MAGIC SQL functions** that power the AI Advisory Services agent.
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `bronze_audio_files` | Raw audio file metadata from Auto Loader |
# MAGIC | Silver | `silver_transcriptions` | Whisper transcriptions with speaker diarization |
# MAGIC | Gold | `gold_enriched_calls` | Sentiment, topics, intent, rubric scores |
# MAGIC | Ref | `advisor_rubric` | 5-criterion weighted rubric for advisor scoring |

# COMMAND ----------

# DBTITLE 1,Configuration

# -- Parameterized configuration: override via widgets or job parameters --
# All defaults are placeholders -- set them to values that exist in your workspace.
dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema", "higher_ed_advisory", "Schema")
dbutils.widgets.text("volume_path", "/Volumes/main/higher_ed_advisory/audio_files", "Audio Volume Path")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("whisper_endpoint", "whisper_large_v3", "Whisper Model Endpoint")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "LLM Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME_PATH = dbutils.widgets.get("volume_path")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
WHISPER_ENDPOINT = dbutils.widgets.get("whisper_endpoint")
LLM_ENDPOINT = dbutils.widgets.get("llm_endpoint")

if not WAREHOUSE_ID:
    raise ValueError(
        "warehouse_id widget is required. Set it to a SQL warehouse ID from your workspace "
        "(Workspace UI -> SQL Warehouses -> select a warehouse -> copy the ID from the URL)."
    )

FQ = f"{CATALOG}.{SCHEMA}"
print(f"Config: {FQ} | Volume: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Initialize Schema & Tables

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as e:
    print(f"Catalog creation skipped (may already exist or lack permissions): {e}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

# Ensure Volume exists for audio files
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.audio_files COMMENT 'Raw audio files for advisory call recordings'")
    print(f"Volume ready: {VOLUME_PATH}")
except Exception as e:
    print(f"Volume creation note: {e}")

# -- Bronze: raw audio file metadata from Auto Loader --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.bronze_audio_files (
  filename         STRING     COMMENT 'Original filename of the audio recording',
  file_path        STRING     COMMENT 'Full Volume path to the audio file',
  file_size_bytes  LONG       COMMENT 'Size of the audio file in bytes',
  modified_time    TIMESTAMP  COMMENT 'Last modification timestamp from cloud storage',
  ingested_at      TIMESTAMP  COMMENT 'Timestamp when Auto Loader ingested the file'
)
USING DELTA
COMMENT 'Bronze layer: raw audio file metadata ingested via Auto Loader from cloud storage'
TBLPROPERTIES ('quality' = 'bronze')
""")

# -- Silver: Whisper transcriptions --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.silver_transcriptions (
  filename         STRING     COMMENT 'Original audio filename',
  file_path        STRING     COMMENT 'Full Volume path',
  speaker_id       STRING     COMMENT 'Extracted speaker identifier',
  transcription    STRING     COMMENT 'Full text transcription from Whisper',
  word_count       INT        COMMENT 'Number of words in the transcription',
  duration_hint    STRING     COMMENT 'Estimated call duration category (short/medium/long)',
  transcribed_at   TIMESTAMP  COMMENT 'Timestamp when transcription completed'
)
USING DELTA
COMMENT 'Silver layer: audio transcriptions produced by Whisper large-v3 endpoint'
TBLPROPERTIES ('quality' = 'silver')
""")

# -- Gold: enriched calls with sentiment, topics, intent, rubric --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.gold_enriched_calls (
  filename            STRING     COMMENT 'Original audio filename',
  file_path           STRING     COMMENT 'Full Volume path',
  speaker_id          STRING     COMMENT 'Speaker identifier',
  transcription       STRING     COMMENT 'Full transcription text',
  sentiment           STRING     COMMENT 'Overall sentiment: Positive, Negative, Neutral, Mixed',
  sentiment_confidence DOUBLE    COMMENT 'Confidence score for sentiment 0.0-1.0',
  topics              STRING     COMMENT 'Comma-separated extracted topics',
  intent              STRING     COMMENT 'Primary caller intent classification',
  call_category       STRING     COMMENT 'Call type: Financial Aid, Admissions, Enrollment, Academic Advising, Other',
  rubric_score        INT        COMMENT 'Advisor performance rubric score 1-5',
  rubric_assessment   STRING     COMMENT 'Detailed rubric assessment narrative from RAG LLM',
  improvement_areas   STRING     COMMENT 'Comma-separated areas for advisor improvement',
  word_count          INT        COMMENT 'Number of words in transcript',
  enriched_at         TIMESTAMP  COMMENT 'Timestamp when enrichment completed'
)
USING DELTA
COMMENT 'Gold layer: fully enriched call records with AI-derived insights for Genie discovery'
TBLPROPERTIES ('quality' = 'gold')
""")

# -- Rubric reference table (for RAG context) --
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FQ}.advisor_rubric (
  rubric_id     INT        COMMENT 'Unique rubric criterion ID',
  category      STRING     COMMENT 'Rubric category',
  criterion     STRING     COMMENT 'Specific assessment criterion',
  score_1_desc  STRING     COMMENT 'Description of score 1 (Poor)',
  score_3_desc  STRING     COMMENT 'Description of score 3 (Acceptable)',
  score_5_desc  STRING     COMMENT 'Description of score 5 (Excellent)',
  weight        DOUBLE     COMMENT 'Weight of this criterion in overall score'
)
USING DELTA
COMMENT 'Reference rubric for evaluating higher-ed advisor call quality'
""")

print("All tables initialized.")

# COMMAND ----------

# DBTITLE 1,Seed Advisor Rubric

rubric_count = spark.sql(f"SELECT count(*) AS cnt FROM {FQ}.advisor_rubric").collect()[0]["cnt"]
if rubric_count == 0:
    spark.sql(f"""
    INSERT INTO {FQ}.advisor_rubric VALUES
    (1, 'Greeting & Identification',
        'Advisor properly identifies themselves and confirms student identity',
        'No greeting; fails to identify student',
        'Basic greeting; confirms name only',
        'Warm, professional greeting; confirms name, ID, and reason for call',
        0.15),
    (2, 'Active Listening',
        'Advisor demonstrates active listening through paraphrasing and clarifying questions',
        'Interrupts student; ignores stated concerns',
        'Listens but does not paraphrase or confirm understanding',
        'Paraphrases concerns, asks clarifying questions, confirms understanding',
        0.20),
    (3, 'Accurate Information',
        'Advisor provides correct policy, deadline, and procedural information',
        'Provides incorrect information or guesses',
        'Provides mostly correct info with minor gaps',
        'Provides fully accurate info with citations to official policy',
        0.25),
    (4, 'Empathy & Tone',
        'Advisor shows empathy and maintains professional, supportive tone',
        'Dismissive or cold tone; no empathy shown',
        'Neutral tone; acknowledges concern without empathy',
        'Warm, empathetic; validates feelings; reassures student',
        0.20),
    (5, 'Resolution & Next Steps',
        'Advisor clearly resolves the issue or sets concrete next steps',
        'Call ends without resolution or next steps',
        'Partial resolution; vague follow-up',
        'Full resolution with specific next steps, deadlines, and contact info',
        0.20)
    """)
    print("Rubric seeded with 5 criteria.")
else:
    print(f"Rubric already has {rubric_count} rows -- skipping seed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL UC Functions (12 total)
# MAGIC
# MAGIC All functions are pure SQL -- no Python UDFs, no `WorkspaceClient()` dependencies.
# MAGIC This ensures they work in all contexts including model serving endpoints.

# COMMAND ----------

# DBTITLE 1,UC Function 1: find_audio_file

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.find_audio_file")
spark.sql(f"""
CREATE FUNCTION {FQ}.find_audio_file(speaker_query STRING)
RETURNS STRING
COMMENT 'Finds an audio file in the Volume by speaker name, number, or filename fragment. Returns JSON with file_path, filename, and match metadata.'
RETURN (
  WITH files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  ),
  speaker_num AS (
    SELECT CASE
      WHEN regexp_extract(lower(speaker_query), 'speaker[_\\\\s]*0*(\\\\d+)', 1) != ''
        THEN regexp_extract(lower(speaker_query), 'speaker[_\\\\s]*0*(\\\\d+)', 1)
      WHEN regexp_extract(lower(speaker_query), '\\\\b0*(\\\\d+)\\\\b', 1) != ''
        THEN regexp_extract(lower(speaker_query), '\\\\b0*(\\\\d+)\\\\b', 1)
      ELSE NULL
    END AS num
  ),
  matches AS (
    SELECT filename, path, file_size FROM files, speaker_num
    WHERE speaker_num.num IS NOT NULL
      AND lower(filename) RLIKE concat('speaker[_\\\\s]*0*', speaker_num.num, '[_.]')
  )
  SELECT CASE
    WHEN (SELECT num FROM speaker_num) IS NULL THEN
      to_json(named_struct('status', 'error', 'message', concat('Could not parse speaker from: ', speaker_query)))
    WHEN (SELECT count(*) FROM matches) = 0 THEN
      to_json(named_struct('status', 'not_found', 'message', concat('No files for speaker ', (SELECT num FROM speaker_num))))
    ELSE
      to_json(named_struct(
        'status', 'found',
        'file_path', (SELECT path FROM matches LIMIT 1),
        'filename', (SELECT filename FROM matches LIMIT 1),
        'speaker_id', (SELECT num FROM speaker_num),
        'file_size_bytes', (SELECT file_size FROM matches LIMIT 1)
      ))
  END
)
""")
print("Registered: find_audio_file")

# COMMAND ----------

# DBTITLE 1,UC Function 2: find_all_audio_files

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.find_all_audio_files")
spark.sql(f"""
CREATE FUNCTION {FQ}.find_all_audio_files()
RETURNS STRING
COMMENT 'Lists all .wav audio files in the advisory services Volume. Returns JSON array with file metadata.'
RETURN (
  WITH files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size,
      _metadata.file_modification_time AS modified_time
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  )
  SELECT to_json(named_struct(
    'total_files', (SELECT count(*) FROM files),
    'files', (SELECT collect_list(
      named_struct('filename', filename, 'file_path', path, 'file_size_bytes', file_size, 'modified_time', modified_time)
    ) FROM files)
  ))
)
""")
print("Registered: find_all_audio_files")

# COMMAND ----------

# DBTITLE 1,UC Function 3: read_audio_base64

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.read_audio_base64")
spark.sql(f"""
CREATE FUNCTION {FQ}.read_audio_base64(file_path STRING)
RETURNS STRING
COMMENT 'Reads an audio file from the Volume and returns its base64-encoded binary content for Whisper inference.'
RETURN (
  SELECT base64(content)
  FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  WHERE _metadata.file_path = file_path
  LIMIT 1
)
""")
print("Registered: read_audio_base64")

# COMMAND ----------

# DBTITLE 1,UC Function 4: transcribe_audio

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.transcribe_audio")
spark.sql(f"""
CREATE FUNCTION {FQ}.transcribe_audio(file_path STRING)
RETURNS STRING
COMMENT 'Transcribes an audio file using the Whisper large-v3 speech recognition model via ai_query. Returns the full transcript text.'
RETURN (
  SELECT ai_query(
    endpoint    => '{WHISPER_ENDPOINT}',
    request     => unbase64({FQ}.read_audio_base64(file_path)),
    returnType  => 'STRING',
    failOnError => false
  )
)
""")
print("Registered: transcribe_audio")

# COMMAND ----------

# DBTITLE 1,UC Function 5: classify_call_category

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.classify_call_category")
spark.sql(f"""
CREATE FUNCTION {FQ}.classify_call_category(transcription STRING)
RETURNS STRING
COMMENT 'Classifies a higher-ed advisory call transcript into one category: Financial Aid, Admissions, Enrollment, Academic Advising, Registration, Housing, Billing, Career Services, or Other.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are a higher education call center analyst. Classify this advisor-student call transcript ',
      'into exactly ONE category from the following list:\\n',
      '- Financial Aid\\n- Admissions\\n- Enrollment\\n- Academic Advising\\n',
      '- Registration\\n- Housing\\n- Billing\\n- Career Services\\n- Other\\n\\n',
      'Respond with ONLY the category name. No explanation.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: classify_call_category")

# COMMAND ----------

# DBTITLE 1,UC Function 6: analyze_call_sentiment

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.analyze_call_sentiment")
spark.sql(f"""
CREATE FUNCTION {FQ}.analyze_call_sentiment(transcription STRING)
RETURNS STRING
COMMENT 'Analyzes student sentiment from a call transcript. Returns JSON with sentiment label and confidence.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'Analyze the overall student sentiment in this higher education advisory call transcript. ',
      'Return a JSON object with exactly two fields:\\n',
      '  "sentiment": one of "Positive", "Negative", "Neutral", "Mixed"\\n',
      '  "confidence": a decimal between 0.0 and 1.0\\n\\n',
      'Return ONLY the JSON. No markdown, no explanation.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: analyze_call_sentiment")

# COMMAND ----------

# DBTITLE 1,UC Function 7: extract_topics_and_intent

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.extract_topics_and_intent")
spark.sql(f"""
CREATE FUNCTION {FQ}.extract_topics_and_intent(transcription STRING)
RETURNS STRING
COMMENT 'Extracts key topics and primary intent from a call transcript. Returns JSON with topics array and intent string.'
RETURN (
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are analyzing a higher education advisory call. Extract the following from this transcript:\\n',
      '1. "topics": A JSON array of 2-5 key topics discussed (e.g., "FAFSA deadline", "GPA requirements", "transfer credits")\\n',
      '2. "intent": The single primary reason the student called (e.g., "Inquire about financial aid eligibility")\\n',
      '3. "improvement_areas": A JSON array of 0-3 areas where the advisor could improve\\n\\n',
      'Return ONLY a JSON object with these three fields. No markdown.\\n\\nTranscript:\\n', transcription
    )
  )
)
""")
print("Registered: extract_topics_and_intent")

# COMMAND ----------

# DBTITLE 1,UC Function 8: assess_rubric_rag

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.assess_rubric_rag")
spark.sql(f"""
CREATE FUNCTION {FQ}.assess_rubric_rag(transcription STRING)
RETURNS STRING
COMMENT 'Assesses advisor performance against the advisory services rubric using RAG. Retrieves rubric criteria from the reference table and produces a weighted score (1-5) with narrative assessment.'
RETURN (
  WITH rubric AS (
    SELECT collect_list(
      concat(
        'Criterion: ', criterion, ' (Weight: ', CAST(weight AS STRING), ')\\n',
        '  Score 1 (Poor): ', score_1_desc, '\\n',
        '  Score 3 (Acceptable): ', score_3_desc, '\\n',
        '  Score 5 (Excellent): ', score_5_desc
      )
    ) AS criteria
    FROM {FQ}.advisor_rubric
  )
  SELECT ai_query(
    '{LLM_ENDPOINT}',
    concat(
      'You are assessing a higher education advisor call against a quality rubric.\\n\\n',
      '## RUBRIC CRITERIA:\\n',
      array_join((SELECT criteria FROM rubric), '\\n\\n'),
      '\\n\\n## CALL TRANSCRIPT:\\n', transcription,
      '\\n\\n## INSTRUCTIONS:\\n',
      'Score each criterion 1-5. Then compute a single weighted overall score (round to nearest integer).\\n',
      'Return ONLY a JSON object with:\\n',
      '  "overall_score": integer 1-5\\n',
      '  "assessment": a 2-3 sentence narrative summary of advisor performance\\n',
      '  "criterion_scores": object mapping criterion name to its individual score\\n',
      'No markdown formatting. Just the JSON.'
    )
  )
)
""")
print("Registered: assess_rubric_rag")

# COMMAND ----------

# DBTITLE 1,UC Function 9: transcribe_and_save_to_silver (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.transcribe_and_save_to_silver")
spark.sql(f"""
CREATE FUNCTION {FQ}.transcribe_and_save_to_silver(file_path STRING)
RETURNS STRING
COMMENT 'Transcribes a single audio file using Whisper large-v3 and returns the transcription with metadata. Returns JSON with status, filename, speaker_id, transcription text, word_count, and duration_hint.'
RETURN (
  WITH file_info AS (
    SELECT
      split(transcribe_and_save_to_silver.file_path, '/')[size(split(transcribe_and_save_to_silver.file_path, '/'))-1] AS fn,
      COALESCE(
        NULLIF(regexp_extract(transcribe_and_save_to_silver.file_path, 'Speaker[_\\\\s]*0*(\\\\d+)', 1), ''),
        'unknown'
      ) AS sid
  ),
  transcript AS (
    SELECT {FQ}.transcribe_audio(transcribe_and_save_to_silver.file_path) AS txt
  ),
  wc AS (
    SELECT size(split(trim((SELECT txt FROM transcript)), '\\\\s+')) AS word_count
  )
  SELECT to_json(named_struct(
    'status', 'success',
    'filename', (SELECT fn FROM file_info),
    'speaker_id', (SELECT sid FROM file_info),
    'transcription', (SELECT txt FROM transcript),
    'word_count', (SELECT word_count FROM wc),
    'duration_hint', CASE
      WHEN (SELECT word_count FROM wc) < 100 THEN 'short'
      WHEN (SELECT word_count FROM wc) < 500 THEN 'medium'
      ELSE 'long'
    END
  ))
)
""")
print("Registered: transcribe_and_save_to_silver")

# COMMAND ----------

# DBTITLE 1,UC Function 10: process_all_audio_to_silver (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.process_all_audio_to_silver")
spark.sql(f"""
CREATE FUNCTION {FQ}.process_all_audio_to_silver()
RETURNS STRING
COMMENT 'Checks audio file transcription status. Shows total files in Volume, how many are already transcribed in silver, and how many are pending. Returns JSON summary with counts and sample pending files.'
RETURN (
  WITH all_files AS (
    SELECT
      split(_metadata.file_path, '/')[size(split(_metadata.file_path, '/'))-1] AS filename,
      _metadata.file_path AS path,
      _metadata.file_size AS file_size
    FROM read_files('{VOLUME_PATH}/*.wav', format => 'binaryFile')
  ),
  already_done AS (
    SELECT file_path FROM {FQ}.silver_transcriptions
  ),
  pending AS (
    SELECT a.filename, a.path, a.file_size
    FROM all_files a
    LEFT ANTI JOIN already_done d ON a.path = d.file_path
  ),
  stats AS (
    SELECT
      (SELECT count(*) FROM all_files) AS total_files,
      (SELECT count(*) FROM already_done) AS already_transcribed,
      (SELECT count(*) FROM pending) AS pending_transcription
  )
  SELECT to_json(named_struct(
    'status', 'complete',
    'total_files', (SELECT total_files FROM stats),
    'already_transcribed', (SELECT already_transcribed FROM stats),
    'pending_transcription', (SELECT pending_transcription FROM stats),
    'sample_pending', (SELECT collect_list(named_struct('filename', filename, 'file_path', path))
                       FROM (SELECT * FROM pending LIMIT 5)),
    'message', CASE
      WHEN (SELECT pending_transcription FROM stats) = 0
        THEN 'All files already transcribed to silver.'
      ELSE concat('Found ', (SELECT pending_transcription FROM stats),
                  ' files pending transcription. Use transcribe_and_save_to_silver(file_path) for each file.')
    END
  ))
)
""")
print("Registered: process_all_audio_to_silver")

# COMMAND ----------

# DBTITLE 1,UC Function 11: enrich_silver_to_gold (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.enrich_silver_to_gold")
spark.sql(f"""
CREATE FUNCTION {FQ}.enrich_silver_to_gold()
RETURNS STRING
COMMENT 'Reports enrichment pipeline status. Shows silver record count, gold record count, and how many silver records are pending enrichment. Returns JSON with pipeline status and counts.'
RETURN (
  WITH silver_count AS (
    SELECT count(*) AS cnt FROM {FQ}.silver_transcriptions
  ),
  gold_count AS (
    SELECT count(*) AS cnt FROM {FQ}.gold_enriched_calls
  ),
  pending AS (
    SELECT count(*) AS cnt
    FROM {FQ}.silver_transcriptions s
    LEFT ANTI JOIN {FQ}.gold_enriched_calls g
      ON s.file_path = g.file_path
    WHERE s.transcription IS NOT NULL AND length(trim(s.transcription)) > 10
  )
  SELECT to_json(named_struct(
    'status', CASE WHEN (SELECT cnt FROM pending) = 0 THEN 'up_to_date' ELSE 'pending_enrichment' END,
    'silver_total', (SELECT cnt FROM silver_count),
    'gold_total', (SELECT cnt FROM gold_count),
    'pending_enrichment', (SELECT cnt FROM pending),
    'message', CASE
      WHEN (SELECT cnt FROM pending) = 0 AND (SELECT cnt FROM gold_count) > 0
        THEN 'All silver records have been enriched to gold.'
      WHEN (SELECT cnt FROM pending) = 0 AND (SELECT cnt FROM gold_count) = 0
        THEN 'No silver records available yet. Run transcription first.'
      ELSE concat((SELECT cnt FROM pending), ' silver records ready for enrichment. Use enrich_single_call(transcription) to enrich individual calls.')
    END
  ))
)
""")
print("Registered: enrich_silver_to_gold")

# COMMAND ----------

# DBTITLE 1,UC Function 12: enrich_single_call (SQL)

spark.sql(f"DROP FUNCTION IF EXISTS {FQ}.enrich_single_call")
spark.sql(f"""
CREATE FUNCTION {FQ}.enrich_single_call(transcription STRING)
RETURNS STRING
COMMENT 'Runs the full AI enrichment pipeline on a single call transcript: sentiment analysis, topic extraction, intent classification, call categorization, and rubric-based RAG assessment. Returns comprehensive JSON with all enrichment results in one call.'
RETURN (
  WITH sentiment AS (
    SELECT {FQ}.analyze_call_sentiment(transcription) AS raw
  ),
  topics AS (
    SELECT {FQ}.extract_topics_and_intent(transcription) AS raw
  ),
  category AS (
    SELECT {FQ}.classify_call_category(transcription) AS raw
  ),
  rubric AS (
    SELECT {FQ}.assess_rubric_rag(transcription) AS raw
  )
  SELECT to_json(named_struct(
    'sentiment', COALESCE(try_parse_json((SELECT raw FROM sentiment)):sentiment::STRING, 'Unknown'),
    'sentiment_confidence', COALESCE(try_parse_json((SELECT raw FROM sentiment)):confidence::DOUBLE, 0.0),
    'topics', COALESCE(try_parse_json((SELECT raw FROM topics)):topics::STRING, '[]'),
    'intent', COALESCE(try_parse_json((SELECT raw FROM topics)):intent::STRING, 'Unknown'),
    'call_category', COALESCE((SELECT raw FROM category), 'Other'),
    'rubric_score', COALESCE(try_parse_json((SELECT raw FROM rubric)):overall_score::INT, 0),
    'rubric_assessment', COALESCE(try_parse_json((SELECT raw FROM rubric)):assessment::STRING, 'N/A'),
    'criterion_scores', COALESCE(try_parse_json((SELECT raw FROM rubric)):criterion_scores::STRING, '[]'),
    'improvement_areas', COALESCE(try_parse_json((SELECT raw FROM topics)):improvement_areas::STRING, '[]')
  ))
)
""")
print("Registered: enrich_single_call")

# COMMAND ----------

# DBTITLE 1,Verify All Registered Functions

spark.sql(f"USE CATALOG {CATALOG}")
funcs = spark.sql(f"SHOW USER FUNCTIONS IN {FQ}").collect()
print(f"\nAll UC Functions in {FQ}:")
for f in funcs:
    print(f"  - {f[0]}")

expected = {
    "find_audio_file", "find_all_audio_files", "read_audio_base64", "transcribe_audio",
    "classify_call_category", "analyze_call_sentiment", "extract_topics_and_intent",
    "assess_rubric_rag", "transcribe_and_save_to_silver", "process_all_audio_to_silver",
    "enrich_silver_to_gold", "enrich_single_call",
}
registered = {f[0].split(".")[-1] for f in funcs}
missing = expected - registered
if missing:
    print(f"\nWARNING: Missing functions: {missing}")
else:
    print(f"\nAll {len(expected)} functions registered successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC All 12 UC functions registered (all pure SQL):
# MAGIC
# MAGIC | # | Function | Purpose |
# MAGIC |---|----------|---------|
# MAGIC | 1 | `find_audio_file` | Find specific audio file by speaker |
# MAGIC | 2 | `find_all_audio_files` | List all audio files in Volume |
# MAGIC | 3 | `read_audio_base64` | Read audio file as base64 for Whisper |
# MAGIC | 4 | `transcribe_audio` | Transcribe via `ai_query` + Whisper |
# MAGIC | 5 | `classify_call_category` | Classify call into Higher Ed categories |
# MAGIC | 6 | `analyze_call_sentiment` | Sentiment + confidence via LLM |
# MAGIC | 7 | `extract_topics_and_intent` | Extract topics, intent, improvement areas |
# MAGIC | 8 | `assess_rubric_rag` | RAG rubric assessment against advisor criteria |
# MAGIC | 9 | `transcribe_and_save_to_silver` | Transcribe single file (returns JSON) |
# MAGIC | 10 | `process_all_audio_to_silver` | Check transcription pipeline status |
# MAGIC | 11 | `enrich_silver_to_gold` | Check enrichment pipeline status |
# MAGIC | 12 | `enrich_single_call` | Full AI enrichment in one call |