# AI Agent Transport

A specialized travel assistant agent built with Databricks' Mosaic AI Agent Framework, designed to help users plan train journeys in Switzerland by accessing real-time train connections and station information through the Transport OpenData API.


There is a blog about Buidling an AI agent transportation applications with Databricks that is based on this implementation and can be found [here](https://docs.google.com/document/d/1VgcfIYbvuIa_cjqyEnL6nlWsqLAd4_boWNjf2OuHmNw/edit?tab=t.0) 

## Project Overview

This agent provides:
- Real-time train connection queries
- Station departure/arrival board information
- Support for multi-stop journey planning
- Real-time updates and platform information

## Repository Structure

### Core Files
- `driver.py` - Main implementation notebook with agent testing, evaluation, and deployment
- `system_prompt.txt` - Defines agent behavior and interaction parameters
- `function_implementations.ipynb` - Unity Catalog function implementations
- `README.md` - Project documentation

### Unity Catalog Functions

Located in `travel_agents.train_agent` schema:

1. **get_connections**
   ```python
   def get_connections(
       from_station: str,  # Departure station
       to_station: str,    # Arrival station
       via_station: str    # Optional intermediate stop
   ) -> str:              # Returns JSON with connection details
   ```

2. **get_station_board**
   ```python
   def get_station_board(
       station: str,               # Target station
       arrival_or_departure: str   # Board type selector
   ) -> str:                      # Returns JSON with schedule data
   ```

## Setup & Installation

### Prerequisites
- Databricks workspace with MLflow access
- Python 3.8+
- Meta-Llama 3 70B Instruct model endpoint

### Installation
```bash
%pip install -U -qqqq mlflow langchain langgraph==0.3.4 \
    databricks-langchain pydantic databricks-agents \
    unitycatalog-langchain[databricks] uv
```

## Usage Examples

### Basic Queries

```python
from agent import AGENT

# Find next connection
response = AGENT.predict({
    "messages": [{
        "role": "user",
        "content": "When is the next train from Zurich to Bern?"
    }]
})

# Stream response for station board
for chunk in AGENT.predict_stream({
    "messages": [{
        "role": "user",
        "content": "Show me departures from Zurich HB"
    }]
}):
    print(chunk)
```

### Sample Queries
- "What are the train connections from Zurich to Bern today?"
- "Show me the station board for Zurich HB"
- "When is the next train leaving from Zurich HB?"
- "I want to go from Zurich to Bern via Basel. What's available?"

## Development & Deployment

### Evaluation Setup
```python
eval_examples = [
    {
        "request": {
            "messages": [{
                "role": "user",
                "content": "Find the next train from Zurich to Bern?"
            }]
        },
        "expected_response": None
    }
]
```

### Model Registration
```python
mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = "travel_agents.train_agent.train_travel_agent"
```

### Deployment
```python
from databricks import agents
agents.deploy(UC_MODEL_NAME, model_version, tags={"endpointSource": "playground"})
```

## Technical Details

### System Design
- Uses LangGraph for agent orchestration
- Implements MLflow's ChatAgent interface
- Leverages Unity Catalog for function registration
- Integrates with transport.opendata.ch API

### Limitations
- Swiss train network only
- No ticket booking capability
- Maximum 15 entries per station board query
- Real-time data subject to API availability

### Error Handling
- Clear messages for API failures
- Informative responses for invalid stations
- Graceful handling of missing connections
- Real-time data availability notifications

## API Integration

Uses [Transport OpenData CH](https://transport.opendata.ch/) for:
- Train timetables
- Real-time departure/arrival information
- Platform updates
- Connection details

## Contributing

When adding features:
1. Update system prompt for new capabilities
2. Add corresponding Unity Catalog functions
3. Include evaluation examples
4. Update documentation

## License

Refer to project license file for terms of use.
