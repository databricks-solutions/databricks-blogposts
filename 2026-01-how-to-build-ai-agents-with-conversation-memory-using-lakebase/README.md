# Lakebase Memory Accelerator

A comprehensive solution accelerator demonstrating how to build stateful AI agents using Databricks Lakebase (PostgreSQL) and LangGraph for persistent conversation memory and state management.

## Overview

This accelerator showcases how to build conversational AI agents that maintain context across multiple interactions using Databricks Lakebase as a checkpoint store. Unlike stateless LLM calls, these agents preserve conversation history and can resume from any point in time using thread IDs.

### Key Features

- **Persistent Memory**: Conversation state stored in Databricks Lakebase (PostgreSQL)
- **Thread-based Sessions**: Each conversation tracked with unique thread IDs
- **Resumable Conversations**: Pick up where you left off in any conversation
- **Unity Catalog Integration**: Leverage UC functions as agent tools
- **Production-ready Deployment**: Complete MLflow model registration and serving
- **Interactive Chat Interface**: Streamlit-based web application

## Architecture

The solution uses:
- **Lakebase**: Managed PostgreSQL for durable agent state storage
- **LangGraph**: State graph framework with PostgreSQL checkpointer
- **MLflow**: Model tracking, registration, and deployment
- **Unity Catalog**: Function toolkit for agent tools
- **Databricks Model Serving**: Production deployment platform

## Directory Structure

```
lakebase-memory-accelerator/
├── README.md                                    # This file
├── 00-data-uc-function-setup.ipynb            # Unity Catalog functions setup
├── 01-lakebase-instance-setup.ipynb           # Lakebase PostgreSQL instance creation
├── 02-lakebase-langgraph-checkpointer-agent.ipynb  # Main agent implementation
├── agent.py                                   # LangGraph agent class implementation
├── checkpoints-example-query.dbquery.ipynb   # Example checkpoint queries
├── data/                                      # Sample datasets
│   ├── cyber_threat_detection.snappy.parquet # Cybersecurity threat data
│   └── user_info.snappy.parquet              # User information data
├── databricks_apps/                          # Streamlit web application
│   ├── LICENSE
│   ├── NOTICE  
│   ├── README.md
│   └── streamlit-chatbot-app/
│       ├── app.py                            # Streamlit chat interface
│       ├── app.yaml                          # App configuration
│       ├── model_serving_utils.py            # Model serving utilities
│       └── requirements.txt                  # Python dependencies
└── resources/                                # Databricks bundle configurations
    ├── lakebase_instance.yml                 # Example DABs Lakebase instance config
    ├── short_term_memory_agent_job.yml       # Example DABs Job deployment config
    └── short_term_memory_app.yml             # Example DABs App deployment config
```

## Getting Started

### Prerequisites

1. **Databricks Workspace** with Unity Catalog enabled
2. **Lakebase Instance** - Create via SQL Warehouses → Lakebase Postgres → Create database instance
3. **Model Serving Permissions** for agent deployment
4. **Secret Scope** for storing credentials (default: `dbdemos`)

### Setup Instructions

#### Step 1: Data and Functions Setup
Run `00-data-uc-function-setup.ipynb` to:
- Create sample datasets in Unity Catalog
- Set up Unity Catalog functions as agent tools
- Configure cybersecurity threat detection functions

#### Step 2: Lakebase Instance Setup  
Run `01-lakebase-instance-setup.ipynb` to:
- Create Lakebase PostgreSQL instance
- Configure database roles and permissions
- Set up database catalog integration

#### Step 3: Agent Development and Deployment
Run `02-lakebase-langgraph-checkpointer-agent.ipynb` to:
- Build the stateful LangGraph agent
- Configure databricks-langchain checkpointer
- Test agent locally with conversation threads
- Register model to Unity Catalog
- Deploy to Databricks Model Serving

#### Step 4: Web Application Deployment
Deploy the Streamlit chat interface:
- Configure thread ID management in sidebar
- Connect to deployed agent endpoint
- Enable persistent conversation sessions

## Core Components

### LangGraphChatAgent Class

The main agent implementation (`agent.py`) features:
- **Conversation Checkpointing**: State persistence after each agent step
- **Tool Integration**: Unity Catalog tools

### Available Tools

- `get_cyber_threat_info`: Retrieve cybersecurity threat information
- `get_user_info`: Get user details from threat source IPs
- Optional: Vector Search retrieval tools

## Usage Examples

### Basic Agent Interaction

```python
from agent import AGENT

response = AGENT.predict({
    "messages": [{"role": "user", "content": "Who committed the latest malware threat?"}],
    "custom_inputs": {"thread_id": "conversation-123"}
})
```

### Resuming Conversations

```python
# Continue previous conversation using same thread_id
response = AGENT.predict({
    "messages": [{"role": "user", "content": "What was their IP address?"}],
    "custom_inputs": {"thread_id": "conversation-123"}  # Same thread ID
})
```

### Streamlit App Usage

1. Open the deployed Databricks App
2. Configure thread ID in sidebar (auto-generated or custom)
3. Start conversation with cybersecurity queries
4. Agent maintains context across multiple messages

## Deployment Options

### 1. Databricks Model Serving
- Automatic scaling and high availability
- Built-in authentication and authorization
- Integrated monitoring and logging

### 2. Databricks Apps
- Interactive web interface
- Custom thread ID management
- Real-time conversation experience

### 3. Job Scheduling
- Automated agent training/updates
- Batch processing capabilities
- Resource optimization

## Monitoring and Observability

### Conversation Queries
Use `checkpoints-example-query.dbquery.ipynb` to:
- Analyze conversation patterns
- Debug agent behavior
- Monitor checkpoint storage

### MLflow Tracking
- Model versioning and lineage
- Performance metrics
- Experiment comparison

## Security and Governance

- **Unity Catalog Integration**: Data governance and permissions
- **OAuth Authentication**: Secure Lakebase connections
- **Secret Management**: Encrypted credential storage
- **Audit Logging**: Complete conversation tracking

## Customization

### Adding New Tools
1. Create Unity Catalog functions
2. Add to `uc_tool_names` list in `agent.py`
3. Update system prompt to include tool usage

### Modifying Agent Behavior
1. Update `SYSTEM_PROMPT` in configuration
2. Adjust tool selection logic
3. Customize conversation flow in LangGraph

## Troubleshooting

### Common Issues
1. **Thread Management**: Verify thread_id persistence in application state
2. **Tool Permissions**: Check Unity Catalog function access rights
3. **Model Serving**: Validate endpoint deployment and health

### Debug Resources
- MLflow experiment tracking for model behavior
- Lakebase query logs for connection issues
- Databricks job logs for deployment problems

## Next Steps

1. **Production Hardening**: Implement monitoring, alerting, and backup strategies
2. **Advanced Tools**: Add vector search, external APIs, or custom functions
3. **Multi-tenant Support**: Implement user-specific thread isolation
4. **Performance Optimization**: Fine-tune connection pooling and caching

## Documentation Links

- [Databricks AI Agent Memory Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-framework/stateful-agents#example-notebook)
- [LangGraph Checkpoint Documentation](https://langchain-ai.github.io/langgraph/concepts/persistence/)
- [Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/)
- [Unity Catalog Functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions.html)