## Backend 
# Database Query API with FastAPI and Databricks Connect

A REST API service for executing database queries using FastAPI framework and Databricks Connect. The API supports query operations including filters, aggregations, grouping, and ordering through a JSON-based query structure.

## Features

- SQL query execution via REST API
- Support for complex query operations:
 - Column selection
 - Filtering conditions
 - Aggregations (SUM, COUNT, AVG, etc.)
 - Group By operations
 - Order By clauses
 - Result limiting

## Setup and Installation

```bash
./run.sh
```

## Json to build query
```json
{
    "table_name": "sales",
    "columns": ["category", "product"],
    "filters": [
        {
            "column": "amount",
            "operator": ">",
            "value": 1000
        }
    ],
    "aggregations": [
        {
            "function": "SUM",
            "column": "amount",
            "alias": "total_sales"
        }
    ],
    "group_by": ["category", "product"],
    "order_by": [
        {
            "column": "total_sales",
            "order": "DESC"
        }
    ],
    "limit": 10
}
```
## Response

```json
{
    "data": [
        {
            "category": "Electronics",
            "product": "Laptop",
            "total_sales": 50000
        }
    ],
    "count": 1,
    "query": "SELECT category, product, SUM(amount) as total_sales..."
}
```

## Example
```python
import requests

def execute_query(query_json):
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/query",
            json=query_json
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Error: {e.response.json()}")
        return None

# Example usage
query = {
    "table_name": "sales",
    "columns": ["category"],
    "aggregations": [
        {
            "function": "SUM",
            "column": "amount",
            "alias": "total_sales"
        }
    ],
    "group_by": ["category"]
}

result = execute_query(query)
```

dbconnect_ak_local_1 (SP) needs to have permissions across