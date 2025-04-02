import type { Dataset, Column, QueryResult, QueryConfig } from "@/types/data"

// Mock data for fallback
const mockDatasets: Dataset[] = [
  { id: "sales", name: "Sales Data" },
  { id: "inventory", name: "Inventory" },
  { id: "customers", name: "Customer Data" },
]

const mockColumns: Column[] = [
  { name: "date", type: "date" },
  { name: "product", type: "string" },
  { name: "category", type: "string" },
  { name: "quantity", type: "number" },
  { name: "revenue", type: "number" },
  { name: "cost", type: "number" },
  { name: "customer_id", type: "string" },
  { name: "region", type: "string" },
]

// Need to fix this
// having issue if reading from env file. When deployed in databricks apps frontend not able to talk to backend api
const DEPLOYMENT_TYPE = process.env.NEXT_PUBLIC_DEPLOYMENT_MODE || 'standalone';

// Determine API base URL based on deployment type
const API_BASE_URL = DEPLOYMENT_TYPE === 'integrated'
  ? '/api/v1'  // Integrated mode (Databricks App) - relative URL
  : process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api/v1';  // Standalone mode - absolute URL

console.log("Deployment type:", DEPLOYMENT_TYPE);
console.log("Using API URL:", API_BASE_URL);

console.log("Current environment:", process.env.NODE_ENV);

// Still read these from environment variables
const CATALOG = process.env.NEXT_PUBLIC_CATALOG
const DATABASE = process.env.NEXT_PUBLIC_DATABASE

console.log("Using API:", API_BASE_URL);
console.log("Using catalog:", CATALOG);
console.log("Using database:", DATABASE);

export async function fetchDatasets(): Promise<Dataset[]> {
  // Ensure we're running in the browser for static export
  if (typeof window === 'undefined') {
    console.warn('fetchDatasets called during server rendering');
    return mockDatasets;
  }
  
  try {
    const url = `${API_BASE_URL}/tables?catalog=${CATALOG}&database=${DATABASE}`;
    console.log("Fetching datasets from URL:", url);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'accept': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Error fetching datasets: ${response.status} ${response.statusText}`);
    }
    
    const data: string[] = await response.json();
    return data.map((name) => ({ id: name, name }));
  } catch (error) {
    console.error('Error fetching datasets:', error);
    return mockDatasets; // Fallback to mock data
  }
}

export async function fetchColumns(datasetId: string): Promise<Column[]> {
  // Ensure we're running in the browser for static export
  if (typeof window === 'undefined') {
    console.warn(`fetchColumns called during server rendering for dataset ${datasetId}`);
    return mockColumns;
  }
  
  try {
    const url = `${API_BASE_URL}/table/schema?catalog=${CATALOG}&database=${DATABASE}&table=${datasetId}`;
    console.log(`Fetching columns from URL:`, url);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'accept': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Error fetching columns: ${response.status} ${response.statusText}`);
    }
    
    const data: { col_name: string, data_type: string, comment: string | null }[] = await response.json();
    return data.map((col) => ({ name: col.col_name, type: col.data_type as "string" | "number" | "date" }));
  } catch (error) {
    console.error(`Error fetching columns for dataset ${datasetId}:`, error);
    return mockColumns; // Fallback to mock data
  }
}

export async function executeQuery(config: QueryConfig): Promise<QueryResult> {
  // Ensure we're running in the browser for static export
  if (typeof window === 'undefined') {
    console.warn('executeQuery called during server rendering', config);
    return { columns: [], rows: [] };
  }
  
  try {
    const filters = config.filters.map(filter => ({
      column: filter.column,
      operator: filter.operator,
      value: filter.value
    }));

    const aggregations = config.groupBy.map(group => ({
      function: group.aggregation.toUpperCase(),
      column: group.column,
      alias: `${group.aggregation}_${group.column}`
    }));

    const groupBy = config.groupBy.map(group => group.column);

    const payload = {
      table_name: `${CATALOG}.${DATABASE}.${config.dataset}`,
      columns: config.availableColumns,
      filters,
      aggregations,
      group_by: groupBy,
      order_by: [], // Add order_by if needed
      limit: 10 // Add limit if needed
    };

    const response = await fetch(`${API_BASE_URL}/query`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      console.error("Error executing query", payload, response);
      throw new Error(`Error executing query: ${response.statusText}`);
    }

    const data = await response.json();
    const rows = data.data;
    const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

    return {
      columns,
      rows
    };
  } catch (error) {
    console.error("Error executing query:", error);
    return { columns: [], rows: [] }; // Return empty result on error
  }
}