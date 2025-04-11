import os
import sys

from dotenv import load_dotenv
from typing import List, Dict, Any
from pydantic import BaseModel
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from DataSource import DataSource
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

import logging

app = FastAPI(title="Databricks Query API")
logger = logging.getLogger('uvicorn.error')

# Load .env file
load_dotenv()


# Environment configuration
DEPLOYMENT_MODE = os.getenv("NEXT_PUBLIC_DEPLOYMENT_MODE", "standalone")  # "standalone" or "integrated"
STATIC_FILES_DIR = os.getenv("STATIC_FILES_DIR", "static")

# DB Connect Spark Session connection
datasource = DataSource()


class QueryResponse(BaseModel):
    data: List[Dict]
    count: int


def build_query(query_json: Dict[str, Any]) -> str:
    """Build SQL query from JSON structure"""
    try:
        # Get base components
        table_name = query_json.get('table_name')
        if not table_name:
            raise ValueError("table_name is required")

        # Handle SELECT clause
        columns = query_json.get('columns', ['*'])
        aggregations = query_json.get('aggregations', [])

        select_parts = []

        # Add regular columns if specified
        if columns != ['*'] or not aggregations:
            select_parts.extend(columns)

        # Add aggregations
        for agg in aggregations:
            agg_str = f"{agg['function']}({agg['column']})"
            if 'alias' in agg:
                agg_str += f" as {agg['alias']}"
            select_parts.append(agg_str)

        select_clause = ", ".join(select_parts) if select_parts else "*"

        # Build base query
        query = f"SELECT {select_clause} FROM {table_name}"

        # Handle WHERE clause
        filters = query_json.get('filters', [])
        if filters:
            conditions = []
            for filter in filters:
                column = filter['column']
                operator = filter['operator']
                value = filter['value']

                if isinstance(value, list):
                    # Handle IN operator
                    values = ", ".join(f"'{v}'" if isinstance(v, str) else str(v)
                                       for v in value)
                    conditions.append(f"{column} {operator} ({values})")
                else:
                    # Handle other operators
                    value_str = f"'{value}'" if isinstance(value, str) else str(value)
                    conditions.append(f"{column} {operator} {value_str}")

            query += " WHERE " + " AND ".join(conditions)

        # Handle GROUP BY
        group_by = query_json.get('group_by', [])
        if group_by:
            query += " GROUP BY " + ", ".join(group_by)

        # Handle ORDER BY
        order_by = query_json.get('order_by', [])
        if order_by:
            order_terms = [
                f"{item['column']} {item.get('order', 'ASC')}"
                for item in order_by
            ]
            query += " ORDER BY " + ", ".join(order_terms)

        # Handle LIMIT
        limit = query_json.get('limit')
        if limit:
            query += f" LIMIT {limit}"

        return query

    except Exception as e:
        raise ValueError(f"Error building query: {str(e)}")
    
# Create API router for the /api/v1 routes
api_app = FastAPI()

@api_app.post("/query")
async def run_query(query_json: Dict[str, Any]):
    for attempt in range(2):  # Try twice at most

        try:
            # Build the query
            query = build_query(query_json)
            print(query)

            # Execute query using your datasource
            logger.debug(f"connected to {datasource.databricks_host}")
            df = datasource.session.sql(query)
            results = [row.asDict() for row in df.collect()]

            return {
                "data": results,
                "count": len(results),
                "query": query
            }

        except ValueError as e:
            raise HTTPException(status_code=400, detail={
                "error": "Unexpected Error",
                "message": str(e),
                "query_json": query
            })

        except Exception as e:
            if attempt == 0:  # only runs once; after the first failure try to re-initialise the datasource
                datasource.reset()

            else:
                raise HTTPException(status_code=500, detail={
                    "error": "Unexpected Error",
                    "message": str(e),
                    "query_json": query
                })


@api_app.get("/tables")
async def list_tables(catalog: str = None, database: str = None):
    for attempt in range(2):  # Try twice at most
        try:
            tables = datasource.session.sql(f"SHOW TABLES in {catalog}.{database}").collect()
            return [row.tableName for row in tables]
        except Exception as e:
            if attempt == 0:  # only runs once; after the first failure try to re-initialise the datasource
                datasource.reset()

            else:
                raise HTTPException(status_code=500, detail=str(e))


@api_app.get("/table/schema")
async def table_schema(catalog: str = None, database: str = None, table: str = None):
    for attempt in range(2):  # Try twice at most
        try:
            sql_stmt = f"DESCRIBE {catalog}.{database}.{table}"
            print(sql_stmt)
            schema = datasource.session.sql(sql_stmt).collect()
            return [row.asDict() for row in schema]
        except Exception as e:
            if attempt == 0:  # only runs once; after the first failure try to re-initialise the datasource
                datasource.reset()

            else:
                raise HTTPException(status_code=500, detail=str(e))


# Configure CORS middleware for all modes
origins = [
    "http://localhost:8000",  # Backend
    "http://localhost:3000",  # Frontend dev server
    "*"  # Allow all origins - you might want to restrict this in production
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info(f"CORS middleware configured with origins: {origins}")

# Mount the API under /api/v1
app.mount("/api/v1", api_app)

# In integrated mode (Databricks app), mount static files
if DEPLOYMENT_MODE.lower() == "integrated":
    logger.info(f"Running in integrated mode, mounting static files from {STATIC_FILES_DIR}")
    app.mount("/", StaticFiles(directory=STATIC_FILES_DIR, html=True), name="static")
else:
    logger.info("Running in standalone mode, API only")
    
    @app.get("/")
    async def read_root():
        return {
            "status": "running",
            "mode": "standalone",
            "api": "/api/v1"
        }