"use client"

import { useState } from "react"
import { AnalysisPanel } from "./analysis-panel"
import { ResultsTable } from "./results-table"
import { ExplorerHeader } from "./explorer-header"
import { SkeletonTable } from "./skeleton-table"
import { getColumns, runQuery } from "@/app/actions"
import type { Dataset, Column, QueryConfig, QueryResult } from "@/types/data"

interface DataExplorerProps {
  initialDatasets: Dataset[]
}

export function DataExplorer({ initialDatasets }: DataExplorerProps) {
  const [datasets] = useState<Dataset[]>(initialDatasets)
  const [columns, setColumns] = useState<Column[]>([])
  const [selectedDataset, setSelectedDataset] = useState<string>("")
  const [queryConfig, setQueryConfig] = useState<QueryConfig>({
    dataset: "",
    filters: [],
    groupBy: [],
    availableColumns: [],
  })
  const [results, setResults] = useState<QueryResult | null>(null)
  const [isLoading, setIsLoading] = useState(false)

  const handleDatasetSelect = async (datasetId: string) => {
    setSelectedDataset(datasetId)
    // clear query config
    setQueryConfig((prev) => ({ ...prev, dataset: datasetId, filters: [], groupBy: [] , availableColumns: [] }))
    setIsLoading(true)
    try {
      const fetchedColumns = await getColumns(datasetId)
      setColumns(fetchedColumns)
      setQueryConfig((prev) => ({ ...prev, dataset: datasetId, availableColumns: fetchedColumns.map(col => col.name) }))
      const results = await runQuery({ ...queryConfig, dataset: datasetId, availableColumns: fetchedColumns.map(col => col.name) })
      setResults(results)
    } catch (error) {
      console.error("Error fetching data:", error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleExecute = async () => {
    setIsLoading(true)
    try {
      const results = await runQuery({ ...queryConfig, availableColumns: columns.map(col => col.name) })
      setResults(results)
    } catch (error) {
      console.error("Query execution failed:", error)
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="h-screen flex flex-col">
      <ExplorerHeader />
      <div className="flex-1 flex min-h-0">
        {/* Left Sidebar */}
        <div className="w-80 flex-shrink-0 border-r overflow-hidden flex flex-col">
          <AnalysisPanel
            datasets={datasets}
            columns={columns}
            onQueryChange={setQueryConfig}
            onDatasetSelect={handleDatasetSelect}
            isLoading={isLoading}
            onExecute={handleExecute}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0 bg-background">
          <div className="flex-none p-4 border-b bg-[hsl(var(--muted))]">
            <h2 className="text-lg font-semibold">Query Results</h2>
          </div>
          <div className="flex-1 overflow-auto">
            {isLoading ? <SkeletonTable /> : <ResultsTable results={results} isLoading={isLoading} />}
          </div>
        </div>
      </div>
    </div>
  )
}

