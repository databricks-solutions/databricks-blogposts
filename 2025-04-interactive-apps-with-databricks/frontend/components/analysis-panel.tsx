"use client"

import { useState, useEffect } from "react"
import { Plus, X, Info, Filter as LucideFilter, Group, Play, Loader2, Database } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Badge } from "@/components/ui/badge"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { Separator } from "@/components/ui/separator"
import type { Dataset, Column, Filter, GroupBy, QueryConfig } from "@/types/data"

interface AnalysisPanelProps {
  datasets: Dataset[]
  columns: Column[]
  onQueryChange: (config: QueryConfig) => void
  onDatasetSelect: (datasetId: string) => void
  isLoading?: boolean
  onExecute: () => void
}

export function AnalysisPanel({
  datasets,
  columns,
  onQueryChange,
  onDatasetSelect,
  isLoading = false,
  onExecute,
}: AnalysisPanelProps) {
  const [selectedDataset, setSelectedDataset] = useState<string>("")
  const [filters, setFilters] = useState<Filter[]>([])
  const [groupBy, setGroupBy] = useState<GroupBy[]>([])

  useEffect(() => {
    setFilters([])
    setGroupBy([])
  }, [selectedDataset])

  const updateQuery = (newDataset?: string, newFilters?: Filter[], newGroupBy?: GroupBy[]) => {
    onQueryChange({
      dataset: newDataset ?? selectedDataset,
      filters: newFilters ?? filters,
      groupBy: newGroupBy ?? groupBy,
      availableColumns: columns.map(column => column.name),
    })
  }

  const addFilter = () => {
    const newFilter = {
      column: columns[0]?.name ?? "",
      operator: "=" as const,
      value: "",
    }
    setFilters([...filters, newFilter])
    updateQuery(undefined, [...filters, newFilter])
  }

  const addGroupBy = () => {
    const newGroupBy = {
      column: columns[0]?.name ?? "",
      aggregation: "sum" as const,
    }
    setGroupBy([...groupBy, newGroupBy])
    updateQuery(undefined, undefined, [...groupBy, newGroupBy])
  }

  const removeFilter = (index: number) => {
    const newFilters = filters.filter((_, i) => i !== index)
    setFilters(newFilters)
    updateQuery(undefined, newFilters)
  }

  const removeGroupBy = (index: number) => {
    const newGroupBy = groupBy.filter((_, i) => i !== index)
    setGroupBy(newGroupBy)
    updateQuery(undefined, undefined, newGroupBy)
  }

  const updateFilter = (index: number, updates: Partial<Filter>) => {
    const newFilters = filters.map((filter, i) => (i === index ? { ...filter, ...updates } : filter))
    setFilters(newFilters)
    updateQuery(undefined, newFilters)
  }

  const updateGroupBy = (index: number, updates: Partial<GroupBy>) => {
    const newGroupBy = groupBy.map((group, i) => (i === index ? { ...group, ...updates } : group))
    setGroupBy(newGroupBy)
    updateQuery(undefined, undefined, newGroupBy)
  }

  return (
    <div className="flex flex-col h-full bg-white">
      <ScrollArea className="flex-1">
        <div className="p-6 space-y-8">
          {/* Dataset Configuration Section */}
          <div>
            <h2 className="text-lg font-semibold mb-4 flex items-center">
              <Database className="w-5 h-5 mr-2" />
              Dataset Configuration
            </h2>
            <div className="space-y-4">
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <label className="text-sm font-medium">Select Dataset</label>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Info className="w-4 h-4 text-muted-foreground" />
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>Choose a dataset to begin your analysis</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </div>
                <Select
                  value={selectedDataset}
                  onValueChange={(value) => {
                    setSelectedDataset(value)
                    onDatasetSelect(value)
                    updateQuery(value)
                  }}
                  disabled={isLoading}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder="Select a dataset to begin" />
                  </SelectTrigger>
                  <SelectContent>
                    {datasets.map((dataset) => (
                      <SelectItem key={dataset.id} value={dataset.id}>
                        {dataset.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {selectedDataset && (
                <div>
                  <div className="flex items-center gap-2 mb-2">
                    <label className="text-sm font-medium">Available Columns</label>
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>
                          <Info className="w-4 h-4 text-muted-foreground" />
                        </TooltipTrigger>
                        <TooltipContent>
                          <p>These are the columns you can use in your analysis</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </div>
                  <div className="flex flex-wrap gap-1.5 p-3 bg-muted/50 rounded-lg">
                    {columns.map((column) => (
                      <Badge key={column.name} variant="secondary" className="px-2 py-1">
                        {column.name}
                        <span className="ml-1 text-xs opacity-70">({column.type})</span>
                      </Badge>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

          <Separator className="my-6" />

          {/* Filters Section */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold flex items-center">
                <LucideFilter className="w-5 h-5 mr-2" />
                Filters
              </h2>
              <Button
                onClick={addFilter}
                variant="outline"
                size="sm"
                className="bg-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))] hover:bg-[hsl(var(--primary))/90]"
              >
                <Plus className="h-4 w-4 mr-1" />
                Add Filter
              </Button>
            </div>
            <div className="space-y-3">
              {filters.length === 0 ? (
                <div className="text-sm text-muted-foreground text-center py-4 bg-muted/50 rounded-lg">
                  No filters added yet. Click "Add Filter" to start filtering your data.
                </div>
              ) : (
                filters.map((filter, index) => (
                  <div key={index} className="grid gap-2 bg-muted/50 p-3 rounded-lg">
                    <div className="flex items-center gap-2">
                      <Select value={filter.column} onValueChange={(value) => updateFilter(index, { column: value })}>
                        <SelectTrigger className="flex-1">
                          <SelectValue placeholder="Column" />
                        </SelectTrigger>
                        <SelectContent>
                          {columns.map((column) => (
                            <SelectItem key={column.name} value={column.name}>
                              {column.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => removeFilter(index)}
                        className="hover:bg-destructive/10 hover:text-destructive"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                    <div className="flex gap-2">
                      <Select
                        value={filter.operator}
                        onValueChange={(value: Filter["operator"]) => updateFilter(index, { operator: value })}
                      >
                        <SelectTrigger className="w-[140px]">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="=">equals</SelectItem>
                          <SelectItem value="in">contains</SelectItem>
                          <SelectItem value=">">greater than</SelectItem>
                          <SelectItem value="<">less than</SelectItem>
                        </SelectContent>
                      </Select>
                      <Input
                        className="flex-1"
                        value={filter.value as string}
                        onChange={(e) => updateFilter(index, { value: e.target.value })}
                        placeholder="Value"
                      />
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          <Separator className="my-6" />

          {/* Group By Section */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold flex items-center">
                <Group className="w-5 h-5 mr-2" />
                Group By
              </h2>
              <Button
                onClick={addGroupBy}
                variant="outline"
                size="sm"
                className="bg-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))] hover:bg-[hsl(var(--primary))/90]"
              >
                <Plus className="h-4 w-4 mr-1" />
                Add Group
              </Button>
            </div>
            <div className="space-y-3">
              {groupBy.length === 0 ? (
                <div className="text-sm text-muted-foreground text-center py-4 bg-muted/50 rounded-lg">
                  No groups added yet. Click "Add Group" to start grouping your data.
                </div>
              ) : (
                groupBy.map((group, index) => (
                  <div key={index} className="grid gap-2 bg-muted/50 p-3 rounded-lg">
                    <div className="flex items-center gap-2">
                      <Select value={group.column} onValueChange={(value) => updateGroupBy(index, { column: value })}>
                        <SelectTrigger className="flex-1">
                          <SelectValue placeholder="Column" />
                        </SelectTrigger>
                        <SelectContent>
                          {columns.map((column) => (
                            <SelectItem key={column.name} value={column.name}>
                              {column.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => removeGroupBy(index)}
                        className="hover:bg-destructive/10 hover:text-destructive"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                    <Select
                      value={group.aggregation}
                      onValueChange={(value: GroupBy["aggregation"]) => updateGroupBy(index, { aggregation: value })}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select aggregation" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="sum">Sum</SelectItem>
                        <SelectItem value="avg">Average</SelectItem>
                        <SelectItem value="count">Count</SelectItem>
                        <SelectItem value="min">Min</SelectItem>
                        <SelectItem value="max">Max</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </ScrollArea>

      {/* Execute Query Button */}
      <div className="sticky bottom-0 p-4 bg-white border-t">
        <Button
          onClick={onExecute}
          disabled={isLoading}
          className="w-full bg-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))]"
        >
          {isLoading ? (
            <>
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              Executing...
            </>
          ) : (
            <>
              <Play className="w-4 h-4 mr-2" />
              Execute Query
            </>
          )}
        </Button>
      </div>
    </div>
  )
}

