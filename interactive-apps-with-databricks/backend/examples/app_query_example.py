import requests

query_request = {
    "table_name": "sales",
    "columns": ["category"],
    "aggregations": [
        {
            "function": "SUM",
            "column": "amount",
            "alias": "total_sales"
        }
    ],
    "filters": [
        {
            "column": "date",
            "operator": ">=",
            "value": "2024-01-01"
        }
    ],
    "group_by": ["category"],
    "order_by": [
        {
            "column": "total_sales",
            "order": "DESC"
        }
    ]
}

response = requests.post(
    "http://localhost:8000/api/v1/query",
    json=query_request
)

result = response.json()
print("Generated Query:", result["query"])
print("Results:", result["data"])