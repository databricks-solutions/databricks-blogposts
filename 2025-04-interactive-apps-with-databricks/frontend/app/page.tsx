"use client";

import { useState, useEffect } from "react";
import { DataExplorer } from "@/components/data-explorer";
import { fetchDatasets } from "@/lib/api";
import type { Dataset } from "@/types/data";

export default function Page() {
  const [initialDatasets, setInitialDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadData = async () => {
      try {
        const datasets = await fetchDatasets();
        setInitialDatasets(datasets);
      } catch (error) {
        console.error("Error loading datasets:", error);
      } finally {
        setLoading(false);
      }
    };

    loadData();
  }, []);

  if (loading) {
    return <div className="flex items-center justify-center h-screen">Loading datasets...</div>;
  }

  return <DataExplorer initialDatasets={initialDatasets} />;
}