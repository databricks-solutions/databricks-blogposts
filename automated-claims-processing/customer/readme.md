# 🧠 AI-Powered Call Centre Analytics on Databricks

This solution accelerator accompanies the blog **[Transforming Claims Processing with AI: From Call Transcripts to Actionable Insights on Databricks](#)**. It demonstrates how to leverage the **Databricks Intelligence Platform** to transform raw audio call recordings into structured insights, summaries, and even customer-ready emails using **end-to-end AI-powered workflows**.

---

## 📐 Architecture Overview: Medallion Model

The accelerator follows the **Medallion Architecture**, breaking down the process into three layers:

- **Bronze** → Ingest and catalog raw audio files
- **Silver** → Convert audio, extract metadata, and transcribe calls
- **Gold** → Apply AI to generate insights, classifications, and follow-up actions

Each notebook corresponds to a layer in the pipeline.

---

## 📁 Notebook 1: `00 ETL Bronze Layer`

### 🎯 Objective
Ingest raw `.m4a` call recordings into a structured Delta Lake table, establishing the **foundation for all downstream processing**.

### 🔧 Key Steps
- Configure Unity Catalog paths
- Create audio volume path if it doesn't exist
- Load raw file metadata into Spark DataFrame
- Save file reference table to the **Bronze Layer**

### 🗂 Output
**Table**: `recordings_file_reference_bronze`  
Contains metadata (filename, path, timestamps) for each raw audio file.

---

## 🔄 Notebook 2: `01 ETL Silver Layer`

### 🎯 Objective
Convert `.m4a` files to `.mp3`, calculate call duration, and use **OpenAI Whisper** to transcribe calls into text.

### 🔧 Key Steps
- Install required Python libraries (`pydub`, `mutagen`, `whisper`)
- Convert audio format to `.mp3` for transcription compatibility
- Calculate call duration using `mutagen`
- Transcribe call content using the **Whisper** model
- Extract metadata from filenames (e.g., call ID, agent ID, timestamp)

### 🗂 Output
**Table**: `transcriptions_silver`  
Includes:
- Audio duration
- Full transcription text
- Call metadata (agent ID, timestamp, etc.)

---

## ✨ Notebook 3: `02 ETL Gold Layer`

### 🎯 Objective
Apply Databricks-native **AI Functions** and **LLM-based email generation** to extract insights from call transcriptions and prepare follow-up actions.

### 🔧 Key AI Functions Used
| Function | Purpose |
|---------|---------|
| `ai_analyze_sentiment` | Detects customer sentiment |
| `ai_summarize` | Summarizes call content |
| `ai_classify` | Categorizes call intent (e.g., claims, complaints) |
| `ai_extract` | Performs NER (e.g., customer name, DOB, policy #) |
| `ai_query` | Uses LLM to generate a structured follow-up email |
| `ai_mask` | Masks sensitive data for compliant sharing |

### 📬 Email Generation
A detailed prompt and response schema instruct the LLM to generate:
- A clear subject line
- Personalized greeting
- Summary of the call
- Next steps for the customer
- Contact and closing information

### 🛡 Compliance
Uses `ai_mask` to redact personal identifiers in summaries, supporting responsible data sharing.

### 🗂 Output
**Table**: `analysis_gold`  
Includes:
- AI-powered summaries and classifications
- Named entities (customer info)
- Structured follow-up emails (JSON format)
- Masked summaries for secure access

---

## 📊 Visualisation & Use Cases

The enriched data can be consumed in:
- **BI Dashboards** for team leaders (call volumes, agent performance, fraud signals)
- **Real-time Alerts** (e.g., critical sentiment, fraud detection)
- **Case Management Systems** (summaries, next steps, auto-generated emails)
- **Compliance Views** with masked content

---

## 🔐 Databricks Platform Benefits

- **Provisionless Batch Inference**: Run LLMs without managing endpoints
- **Unity Catalog**: Enforces fine-grained access control
- **Delta Lake**: Enables scalable, ACID-compliant storage
- **LakeFlow + AI Functions**: Simplify orchestration and AI enrichment

---

## ✅ Summary

This accelerator shows how to automate and scale intelligent call analysis for claims processing using Databricks, Whisper, and foundation models — all while staying cost-effective, secure, and compliant.

---

## 📎 Notebooks Included

| Notebook | Layer | Description |
|----------|-------|-------------|
| `00 ETL Bronze Layer` | Bronze | Ingest raw audio and register metadata |
| `01 ETL Silver Layer` | Silver | Convert, enrich, and transcribe audio |
| `02 ETL Gold Layer` | Gold | Apply AI for insights, classification, and summarization |

---

## 📬 Need Help?

Reach out via your Databricks representative or check the [Databricks Solution Accelerators](https://www.databricks.com/solutions/accelerators) page for more resources.