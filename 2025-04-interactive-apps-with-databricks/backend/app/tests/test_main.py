import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


from backend.app.main import app, build_query

# Create test client
client = TestClient(app)
client.headers = {
    "Authorization": "Bearer test_token"
}

@pytest.fixture
def mock_spark_session():
    """Create a mock for a Spark session"""
    # Create the mock session
    mock_session = MagicMock()
    
    # Set up common methods
    def mock_sql(query):
        """Mock the SQL execution method"""
        mock_df = MagicMock()
        
        # Set up different mock behaviors based on the SQL query
        if "SHOW TABLES" in query:
            # For listing tables
            mock_row1 = MagicMock()
            mock_row1.tableName = "customers"
            mock_row2 = MagicMock()
            mock_row2.tableName = "orders"
            mock_df.collect.return_value = [mock_row1, mock_row2]
        elif "DESCRIBE" in query:
            # For schema queries
            mock_row1 = MagicMock()
            mock_row1.asDict.return_value = {"col_name": "id", "data_type": "INTEGER", "comment": "Primary key"}
            mock_row2 = MagicMock()
            mock_row2.asDict.return_value = {"col_name": "name", "data_type": "VARCHAR", "comment": "User name"}
            mock_df.collect.return_value = [mock_row1, mock_row2]
        else:
            # For regular queries
            mock_row1 = MagicMock()
            mock_row1.asDict.return_value = {"id": 1, "name": "Test Product", "price": 99.99}
            mock_row2 = MagicMock()
            mock_row2.asDict.return_value = {"id": 2, "name": "Another Product", "price": 49.99}
            mock_df.collect.return_value = [mock_row1, mock_row2]
            
        return mock_df
    
    # Assign the mock_sql function to the sql method
    mock_session.sql = mock_sql
    
    # Add other common Spark session methods if needed
    mock_session.table = MagicMock()
    mock_session.read = MagicMock()
    mock_session.createDataFrame = MagicMock()
    
    return mock_session


class TestQueryBuilder:
    def test_basic_query(self):
        """Test building a simple query with only table_name"""
        query_json = {"table_name": "customers"}
        expected = "SELECT * FROM customers"
        assert build_query(query_json) == expected
    
    def test_missing_table_name(self):
        """Test error when table_name is missing"""
        query_json = {"columns": ["id", "name"]}
        with pytest.raises(ValueError) as excinfo:
            build_query(query_json)
        assert "table_name is required" in str(excinfo.value)
    
    def test_columns_selection(self):
        """Test query with specific columns"""
        query_json = {
            "table_name": "customers",
            "columns": ["id", "name", "email"]
        }
        expected = "SELECT id, name, email FROM customers"
        assert build_query(query_json) == expected
    
    def test_where_clause(self):
        """Test query with WHERE clause"""
        query_json = {
            "table_name": "customers",
            "columns": ["id", "name"],
            "filters": [
                {"column": "status", "operator": "=", "value": "active"},
                {"column": "age", "operator": ">", "value": 18}
            ]
        }
        expected = "SELECT id, name FROM customers WHERE status = 'active' AND age > 18"
        assert build_query(query_json) == expected
    
    def test_where_in_operator(self):
        """Test query with IN operator in WHERE clause"""
        query_json = {
            "table_name": "customers",
            "filters": [
                {"column": "country", "operator": "IN", "value": ["USA", "Canada", "Mexico"]}
            ]
        }
        expected = "SELECT * FROM customers WHERE country IN ('USA', 'Canada', 'Mexico')"
        assert build_query(query_json) == expected
    
    def test_aggregations(self):
        """Test query with aggregation functions"""
        query_json = {
            "table_name": "orders",
            "aggregations": [
                {"function": "SUM", "column": "amount", "alias": "total_sales"},
                {"function": "COUNT", "column": "id", "alias": "order_count"}
            ],
            "group_by": ["customer_id", "product_id"]
        }
        expected = "SELECT SUM(amount) as total_sales, COUNT(id) as order_count FROM orders GROUP BY customer_id, product_id"
        assert build_query(query_json) == expected
    
    def test_order_by(self):
        """Test query with ORDER BY clause"""
        query_json = {
            "table_name": "products",
            "columns": ["id", "name", "price"],
            "order_by": [
                {"column": "price", "order": "DESC"},
                {"column": "name", "order": "ASC"}
            ]
        }
        expected = "SELECT id, name, price FROM products ORDER BY price DESC, name ASC"
        assert build_query(query_json) == expected
    
    def test_limit(self):
        """Test query with LIMIT clause"""
        query_json = {
            "table_name": "logs",
            "limit": 100
        }
        expected = "SELECT * FROM logs LIMIT 100"
        assert build_query(query_json) == expected
    
    def test_complex_query(self):
        """Test building a complex query with multiple components"""
        query_json = {
            "table_name": "sales",
            "columns": ["region"],
            "aggregations": [
                {"function": "SUM", "column": "revenue", "alias": "total_revenue"},
                {"function": "AVG", "column": "margin", "alias": "avg_margin"}
            ],
            "filters": [
                {"column": "date", "operator": ">=", "value": "2023-01-01"},
                {"column": "product_category", "operator": "IN", "value": ["Electronics", "Furniture"]}
            ],
            "group_by": ["region"],
            "order_by": [{"column": "total_revenue", "order": "DESC"}],
            "limit": 10
        }
        expected = "SELECT region, SUM(revenue) as total_revenue, AVG(margin) as avg_margin FROM sales WHERE date >= '2023-01-01' AND product_category IN ('Electronics', 'Furniture') GROUP BY region ORDER BY total_revenue DESC LIMIT 10"
        assert build_query(query_json) == expected

@pytest.fixture(autouse=True)
def setup_mock_datasource(mock_spark_session):
    """Setup the mock datasource for all tests"""
    with patch('backend.app.main.datasource') as mock_datasource:
        # Assign our mock spark session to the datasource
        mock_datasource.session = mock_spark_session
        yield mock_datasource
        
def test_query_endpoint():
        """Test the query endpoint with our mocked Spark session"""
        query_json = {
            "table_name": "products",
            "columns": ["id", "name", "price"],
            "filters": [{"column": "price", "operator": ">", "value": 10}]
        }
        
        response = client.post("/api/v1/query", json=query_json)
        
        # Print for debugging
        if response.status_code != 200:
            print(f"Response status: {response.status_code}")
            print(f"Response detail: {response.json() if response.content else 'No content'}")
        
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 2
        assert data["data"][0]["id"] == 1
        assert data["count"] == 2
        
def test_list_tables_endpoint():
    """Test the tables endpoint with our mocked Spark session"""
    response = client.get("/api/v1/tables?catalog=test_catalog&database=test_db")
    
    assert response.status_code == 200
    assert response.json() == ["customers", "orders"]

def test_table_schema_endpoint():
    """Test the schema endpoint with our mocked Spark session"""
    response = client.get("/api/v1/table/schema?catalog=test_catalog&database=test_db&table=customers")
    print(response.json())
    
    assert response.status_code == 200
    schema = response.json()
    assert len(schema) == 2
    assert schema[0]["col_name"] == "id"
    assert schema[1]["data_type"] == "VARCHAR"
        
if __name__ == "__main__":
    pytest.main()