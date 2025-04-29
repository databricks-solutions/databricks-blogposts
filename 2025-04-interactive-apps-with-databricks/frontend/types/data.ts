export interface Dataset {
  id: string
  name: string
  description?: string
}

export interface Column {
  name: string
  type: "string" | "number" | "date"
}

export interface Filter {
  column: string
  operator: ">" | "=" | "<" | "<>"
  value: string | number | [number, number]
}

export interface GroupBy {
  column: string
  aggregation: "sum" | "avg" | "count" | "min" | "max"
}

export interface QueryConfig {
  dataset: string
  filters: Filter[]
  groupBy: GroupBy[]
  availableColumns: string[] // Add availableColumns to QueryConfig
}

export interface QueryResult {
  columns: string[]
  rows: Record<string, any>[]
}

