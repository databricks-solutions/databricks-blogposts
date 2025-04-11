// Removed "use server" directive
import { QueryConfig } from "@/types/data"
import { fetchColumns, executeQuery } from "@/lib/api"

// Client-side implementation that uses our updated api.ts functions
export async function getColumns(datasetId: string) {
  return fetchColumns(datasetId);
}

export async function runQuery(config: QueryConfig) {
  return executeQuery(config);
}