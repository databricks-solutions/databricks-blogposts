# 🧠 AI-Powered Call Centre Analytics – Solution Accelerator

This repository contains two variations of the **Databricks-powered claims processing accelerator**, showcasing how to transform call center audio recordings into actionable insights using **AI and LLMs on the Databricks Intelligence Platform**.

---

## 🗂 Directory Structure

```
.
├── demo/           # Demo version for internal presentations (simulated transcriptions)
├── customer/       # Shareable version for customers (full pipeline using real transcription)
├── raw_recordings/ # sample audio recordings
├── dashboard.json  # sample dashboard JSON template
└── README.md
```

---

## 🧪 `demo/` – Internal Demo Version

> ⚠️ Intended **only for internal demo purposes**, not for customer distribution.

This version demonstrates the **end-to-end analytics capabilities at scale**, simulating transcription output to:
- Showcase AI enrichment (sentiment, NER, classification, summarization)
- Visualize patterns across **larger volumes of call data**
- Power the **front-end dashboard** with meaningful insights

### 🔧 Key Notes:
- Includes **sample audio files** in the Bronze layer.
- Uses **`resources/generate_data.py`** to create a **Silver layer** with **simulated transcriptions**.
- Supports **bulk application** of Databricks AI Functions (sentiment, summarization, topic classification, etc.).
- Ideal for **live demos** and showcasing **dashboard interactivity**.

### 🧩 Use Case:
Great for illustrating how insights scale when applying AI functions across calls in a customer service environment.

---

## 🤝 `customer/` – Shareable Version

> ✅ This is the version meant to be **shared directly with customers**.

The `customer/` directory contains the **clean version of the solution accelerator**, which:
- Includes **sample `.m4a` audio files** for ingestion
- Walks through the **complete, realistic pipeline**:
  - Bronze Layer: Raw ingestion of audio
  - Silver Layer: Format conversion, duration calculation, transcription using Whisper
  - Gold Layer: AI enrichment via Databricks AI Functions and LLMs

### 🔧 Key Notes:
- No simulated data — all transcriptions are generated from real sample audio using **OpenAI Whisper**.
- Ensures full **transparency and reproducibility**.
- Designed to show **how customers can adopt the pipeline** with their own audio sources and extend the AI use cases.

---

## 🔍 Resources

- `demo/resources/generate_data.py`: Generates simulated transcription data for demo use
- Notebooks are modular and follow **Medallion Architecture (Bronze → Silver → Gold)**

---

## 🧭 Suggested Usage

| Directory | Audience | Purpose |
|----------|----------|---------|
| `demo/` | Internal teams | Live demos and showcasing dashboards at scale |
| `customer/` | Customers, prospects | Deployable reference pipeline with real transcription and AI insights |

---

## 📎 Notebooks (Included in Both Versions)

| Notebook | Layer | Description |
|----------|-------|-------------|
| `00 ETL Bronze Layer` | Bronze | Ingest raw audio and register file metadata |
| `01 ETL Silver Layer` | Silver | Convert audio, extract metadata, transcribe |
| `02 ETL Gold Layer` | Gold | Apply AI Functions for sentiment, classification, summarization, NER, and generate follow-up emails |

---

## 📊 Visualisation

Use the outputs from the Gold layer to power:
- Agent & Manager dashboards
- Sentiment trends
- Fraud alerts
- Case summaries and auto-generated follow-up communications

---

## 📊 Dashboard JSON Template

A sample **Databricks dashboard JSON** is included in this repository for quick deployment of visualizations powered by the Gold Layer data.

### 📄 File:
- `dashboard.json`

### 📝 Instructions:
- Import this JSON into your Databricks workspace using the dashboard import UI.
- After import, **update the SQL `SELECT` statements** in the *Data* section of each dashboard tile to point to your actual schema and table (e.g., `samantha_wise.ai_claims_processing_clean.analysis_gold`).
- Ensure you have permission to access the underlying Delta tables via Unity Catalog.

> ⚠️ Note: This JSON is a template. It assumes table names and paths consistent with this accelerator. If you modified the table names or schema paths, you'll need to adjust the SQL accordingly after import.

---

## ✅ Summary

This accelerator shows how insurance and call center operations can:
- **Reduce manual effort** through automation
- **Accelerate response times** with real-time transcription and AI
- **Improve CX** with personalized, AI-generated follow-ups
- **Gain insights** from unstructured voice data at scale

---

## 🚀 Ready to Try It?

To get started:
1. Clone this repo.
2. Choose either the `demo/` or `customer/` variation based on your audience.
3. Follow the steps in each notebook to ingest, process, enrich, and visualize your call center audio.

For questions or customization requests, reach out to your Databricks contact or visit our [Solution Accelerators page](https://www.databricks.com/solutions/accelerators).

## 📄 OSS License Review

Below is a list of open source libraries used in this solution accelerator, along with their licenses and usage context.

- **pydub**
  - **License:** MIT License
  - **Purpose:** Used for audio format conversion (e.g., `.m4a` to `.mp3`) and basic audio manipulation.

- **mutagen**
  - **License:** GNU Lesser General Public License v2.1 (LGPL-2.1)
  - **Purpose:** Extracts audio metadata such as duration from `.mp3` files.

- **openai-whisper**
  - **License:** MIT License
  - **Purpose:** Performs transcription of audio using OpenAI's automatic speech recognition (ASR) model.

- **numpy** (>=1.24)
  - **License:** BSD 3-Clause License
  - **Purpose:** Fundamental numerical computing library, required as a dependency for Whisper and general data processing.

---

> ✅ All listed libraries are open source and compatible with commercial use.  
> ⚠️ The LGPL license for `mutagen` may require dynamic linking or disclosure of modifications if redistributed.

