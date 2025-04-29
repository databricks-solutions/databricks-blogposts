import { Database } from "lucide-react"

export function ExplorerHeader() {
  return (
    <div className="bg-[hsl(var(--header-bg))] text-[hsl(var(--header-fg))] p-4 flex items-center gap-2">
      <Database className="w-6 h-6" />
      <div>
        <h1 className="text-lg font-semibold">Data Explorer</h1>
        <p className="text-sm opacity-80">Explore and analyze your data with ease</p>
      </div>
    </div>
  )
}

