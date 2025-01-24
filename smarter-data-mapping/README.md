# Project: Leveraging Vector Search & LLMs for Smarter Data Mapping

This repository contains four notebooks that showcase how to create and analyze data for taxonomy consolidation and entity resolution use cases. By using a combination of **regex-based methods**, **vector search**, and **LLM function calling**, this project demonstrates end-to-end workflows for improving data quality and consistency.

---

## Notebooks Overview

1. **Create Data**  
   - Creates a **simulated dataset** that can be used for demonstrating the taxonomy consolidation workflows outlined in the other notebooks.  
   - Contains examples of data transformation, random sampling, and basic data cleaning techniques.

2. **Regex Extraction**  
   - Illustrates how **regular expressions** can be applied to standardize and normalize entity names.  
   - Demonstrates common regex patterns and best practices for entity resolution and data cleaning.

3. **Vector Search for Supplier Identification**  
   - Uses **vector embeddings** to align similar entity names (e.g., supplier names) across multiple data sources, leveraging the [Databricks Vector Search](https://docs.databricks.com/machine-learning/feature-store/vector-search.html) capabilities.  
   - Explains how to build, index, and query a vector store to find semantically similar items despite differences in spelling or abbreviations.  
   - Highlights steps such as data cleaning, embedding creation, vector index building, and similarity search.

4. **LLM Extraction**  
   - Demonstrates how to use **function calling** (or _tool use_) APIs to force Large Language Models (LLMs) to produce structured outputs.  
   - Integrates with the **OpenAI SDK** and **Foundation Model APIs** to parse natural language inputs and convert them into machine-readable schemas.  
   - Shows how LLMs can fit into more complex data processing or Agent workflows by adhering to well-defined function signatures.

---

## Getting Started

### Prerequisites

- A **Databricks** workspace (or comparable environment) with:
  - **Databricks Runtime** 13.x (or higher) for ML recommended
  - Permissions to create and run notebooks
- The following libraries installed (either in the cluster or via `%pip install`):
  - `openai`  
  - `tenacity`  
  - `tqdm`  
  - `databricks-sdk`  
  - `databricks-vectorsearch`  
  - `pandas`  
  - `pyspark`  
  - `mlflow`  

### Setup

1. **Clone or Download** this repository into your Databricks workspace (e.g., via Databricks Repos).
2. **Attach** each notebook to a cluster that has the necessary libraries installed (listed in **Prerequisites**).
3. **Configure** any relevant credentials or tokens for the OpenAI SDK, if you plan to run the LLM function-calling notebook.

### Config File

A `config` file is provided with key variables used across the notebooks, such as:

- `VECTOR_SEARCH_ENDPOINT_NAME`
- `catalog`
- `db`
- `cleaned_table_name`
- `conformed_table_name`
- `embedding_model_endpoint_name`
- `primary_key`
- `embedding_source_column`
- `regex_table_name`
- `llm_table_name`
- `llm_model`
  
Feel free to **modify these variables** according to your Databricks environment, catalog, schema, and any custom naming conventions. Adjusting these settings in the config file ensures a seamless experience when running the notebooks and avoids hardcoding environment-specific information.

### Usage

1. **Simulated Data Generation**  
   - Open the `1. Create Data` notebook.  
   - Run all cells to create your synthetic dataset in a Delta table.

2. **Regex-Based Taxonomy Consolidation**  
   - Open the `2. Regex Extraction` notebook.  
   - Adjust regex patterns or cleaning rules as needed to match your data.  
   - Validate the cleaning results against your synthetic data or real data sources.

3. **Vector Search for Supplier Identification**  
   - Open the `3. Create Vector Search` notebook.  
   - Ensure the conformed table is available, and update paths or table names as required.  
   - Run all cells to see how vector embeddings are created, indexed, and queried.

4. **LLM Function Calling & Tool Use**  
   - Open the `4. LLM Extraction` notebook.  
   - Update any environment variables or credentials needed  
   - Run the notebook cells to observe how function calling enforces a schema on the LLM outputs.

---

## Key Concepts and Benefits

- **Regex-based Normalization**  
  Provides a quick way to handle formatting issues, remove special characters, and unify naming conventions in data.

- **Vector Search**  
  Uses semantic embeddings to accurately match entities that are similar in meaning—even when they differ in punctuation, abbreviations, or minor spelling changes.

- **LLM Function Calling**  
  Allows structured data extraction from LLM outputs, reducing guesswork and manual parsing when building AI-driven applications.

- **End-to-End Workflow**  
  These notebooks can serve as an **end-to-end demonstration** of data generation, standardization, semantic matching, and AI-based parsing—helpful for teams designing robust data pipelines or advanced ML applications.

---

## License Information

Below is a list of primary libraries and their licenses:

- **openai**: MIT License  
- **tenacity**: Apache License 2.0  
- **tqdm**: MIT License  
- **databricks-sdk**: Apache License 2.0  
- **databricks-vectorsearch**: Apache License 2.0  
- **pandas**: BSD 3-Clause License  
- **pyspark**: Apache License 2.0  
- **mlflow**: Apache License 2.0  

Refer to each library’s repository for the most up-to-date license details.

---

## Contributing

If you’d like to contribute enhancements or bug fixes:
1. Fork the repository.
2. Create a new branch for your changes.
3. Submit a pull request detailing your modifications.

Please ensure that all code contributions follow the relevant guidelines for style and documentation.

---

## Support

For issues or questions:
- This content is community based and not supported by Databricks.
- For open-source library inquiries, please check the respective library’s GitHub issues or documentation.  

**Happy Consolidating!**
