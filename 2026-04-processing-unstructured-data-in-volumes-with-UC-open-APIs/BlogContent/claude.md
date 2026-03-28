# Blog Content - Claude.md

## Project: Processing Unstructured Data in Volumes with Unity Catalog Open APIs

### JIRA Ticket
- [TLC-983](https://databricks.atlassian.net/browse/TLC-983) — Status: IN DRAFT

### Source Materials
- **Draft V2 tab**: [Google Doc](https://docs.google.com/document/d/1JpzQZi5d62-L0t66zA5jNz91Rlc8Bw0ov5hW5rBf8ZQ/edit?tab=t.et6zecaaxea)
- **Reference blog (format guide)**: [Integrating Apache Spark with UC Assets via Open APIs](https://community.databricks.com/t5/technical-blog/integrating-apache-spark-with-databricks-unity-catalog-assets/ba-p/97533)
- **Reference code**: [Google Drive folder](https://drive.google.com/drive/u/0/folders/1c0rj6N7zVGcpGTmFRc3kL0QcOiHOPAx7)

### Output
- **Final Blog Google Doc**: [Processing Unstructured Data in Volumes with Unity Catalog Open APIs](https://docs.google.com/document/d/1RyMNUFimgpo746ukOaa7UzUzaV2bjn-PeGdID16GkJQ/edit)
- Located in: [BlogContent Drive folder](https://drive.google.com/drive/u/0/folders/17CEXy_5Jcwywv0WDNFV5sgAMJ6mNE0Hh)

### What Was Done
1. Read JIRA ticket TLC-983 for context on the use case (credential vending for unstructured data in UC Volumes)
2. Read the Draft V2 tab from the source Google Doc
3. Analyzed the published Databricks community blog for formatting conventions (heading hierarchy, code blocks, step-by-step walkthrough pattern, screenshot placement)
4. Read all reference code files (get_temp_vol_cred.py, query_volume_with_daft.py, query_volume_with_duckdb.py, query_volume_with_ray.py, process_images_with_huggingface.py, run_download_and_process.py, etc.)
5. Created a new Google Doc combining the Draft V2 content with the published blog's formatting style, incorporating actual code from the reference repository
6. Moved the doc to the target Drive folder without overwriting existing files

### Blog Structure
- Introduction with announcement of Volumes Credential Vending public preview
- **Section 1**: UC extends governance to unstructured data (context/problem)
- **Section 2**: UC Open APIs extend to Volumes (how credential vending works)
- **Section 3**: End-to-end walkthrough with Daft + HuggingFace (step-by-step with code)
- **Section 4**: Under the hood — direct API usage
- **Section 5**: Other engines (DuckDB, Ray)
- **Section 6**: Prerequisites and setup
- **Conclusion**

### Key Reference Code Files
- `get_temp_vol_cred.py` — Core credential vending module (UC REST API calls)
- `query_volume_with_daft.py` — Downloads files from UC Volumes using Daft
- `query_volume_with_duckdb.py` — Queries parquet files using DuckDB with vended credentials
- `query_volume_with_ray.py` — Processes images using Ray Data with vended credentials
- `process_images_with_huggingface.py` — HuggingFace image classification and captioning
- `run_download_and_process.py` — Orchestrator script (Daft download + HuggingFace processing)

### Notes
- The blog draft mentions images (architecture diagram, sample output) that should be manually inserted into the final doc
- SME reviewers listed in the draft: Jason Reid, Michelle Leon, Adriana Ispas, Kelly Albano, Alex Tsarkov
- Draft comments suggest adding sections about: benefits of volumes, agents on Databricks, AI_PARSE, explanation of external tools, why users can't use the Files API
