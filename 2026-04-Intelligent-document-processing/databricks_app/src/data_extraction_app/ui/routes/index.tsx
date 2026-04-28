import { createFileRoute } from "@tanstack/react-router";
import { useState, useCallback, useRef, useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import Navbar from "@/components/apx/navbar";
import {
  Upload,
  FileText,
  RefreshCw,
  Trash2,
  AlertCircle,
  CheckCircle,
  Play,
  Table2,
  MessageSquare,
  Send,
} from "lucide-react";
import { triggerJobRun, getJobRun, useGetAppAiQuery, chat, ApiError, type JobRunOut } from "@/lib/api";

export const Route = createFileRoute("/")({
  component: () => <UploadPage />,
});

interface FileInfo {
  name: string;
  path: string;
  size: number;
  modified_at: string | null;
}

interface FileListResponse {
  files: FileInfo[];
}

function formatFileSize(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function formatDate(dateString: string | null): string {
  if (!dateString) return "-";
  return new Date(dateString).toLocaleString();
}

function formatDurationMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const sec = Math.floor(ms / 1000);
  const min = Math.floor(sec / 60);
  if (min === 0) return `${sec}s`;
  const s = sec % 60;
  if (s === 0) return `${min}m`;
  return `${min}m ${s}s`;
}

/** Renders assistant message content with simple markdown (bold, paragraphs, tables). */
function ChatMessageContent({ content }: { content: string }) {
  const lines = content.split("\n");
  const rows: React.ReactNode[] = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();
    // Markdown table: line contains | and looks like a table row
    if (trimmed.startsWith("|") && trimmed.endsWith("|")) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith("|")) {
        tableLines.push(lines[i].trim());
        i++;
      }
      const parseRow = (row: string) =>
        row
          .split("|")
          .slice(1, -1)
          .map((c) => c.trim());
      const isSeparator = (row: string) => /^\|[\s\-:]+\|$/.test(row);
      const header = tableLines[0];
      const sepIdx = tableLines.findIndex(isSeparator);
      const bodyStart = sepIdx >= 0 ? sepIdx + 1 : 1;
      const headerCells = parseRow(header);
      const bodyRows = tableLines.slice(bodyStart).filter((r) => !isSeparator(r));
      rows.push(
        <table key={rows.length} className="my-2 w-full border-collapse text-left text-sm">
          <thead>
            <tr>
              {headerCells.map((cell, j) => (
                <th key={j} className="border border-border px-2 py-1 font-medium">
                  {cell}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {bodyRows.map((row, ri) => (
              <tr key={ri}>
                {parseRow(row).map((cell, j) => (
                  <td key={j} className="border border-border px-2 py-1">
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>,
      );
      continue;
    }
    // Paragraph: render line with **bold** support
    const parts: React.ReactNode[] = [];
    let rest = line;
    let key = 0;
    while (rest.length > 0) {
      const bold = /\*\*(.+?)\*\*/.exec(rest);
      if (bold) {
        if (bold.index > 0) {
          parts.push(<span key={key++}>{rest.slice(0, bold.index)}</span>);
        }
        parts.push(<strong key={key++}>{bold[1]}</strong>);
        rest = rest.slice(bold.index + bold[0].length);
      } else {
        parts.push(<span key={key++}>{rest}</span>);
        break;
      }
    }
    rows.push(
      <p key={rows.length} className="mb-1 last:mb-0">
        {parts.length ? parts : rest || "\u00A0"}
      </p>,
    );
    i++;
  }
  return <div className="space-y-1 whitespace-pre-wrap break-words">{rows}</div>;
}

const JOB_POLL_INTERVAL_MS = 2500;
const TERMINAL_LIFE_CYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"];
const TERMINAL_RESULT_STATES = ["SUCCESS", "FAILED", "CANCELED", "TIMEOUT"];
function isJobRunTerminal(status: JobRunOut): boolean {
  if (TERMINAL_LIFE_CYCLE_STATES.includes(status.life_cycle_state)) return true;
  const rs = status.result_state ?? "";
  if (rs && TERMINAL_RESULT_STATES.includes(rs)) return true;
  return false;
}

function AiQueryResultsCard() {
  const { data, isLoading, error, refetch } = useGetAppAiQuery();
  const rows = data?.data?.rows ?? [];
  const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Table2 className="h-5 w-5" />
              Extraction results
            </CardTitle>
            <CardDescription>
              The results from ai_extract() function
            </CardDescription>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => refetch()}
            disabled={isLoading}
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`} />
            Refresh
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : error ? (
          <div className="flex items-center gap-2 text-destructive py-4">
            <AlertCircle className="h-4 w-4" />
            <span className="text-sm">Failed to load query results</span>
          </div>
        ) : rows.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b">
                  {columns.map((col) => (
                    <th
                      key={col}
                      className="text-left py-3 px-2 text-sm font-medium text-muted-foreground"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, idx) => (
                  <tr key={idx} className="border-b last:border-0 hover:bg-muted/50">
                    {columns.map((col) => (
                      <td key={col} className="py-3 px-2 text-sm">
                        {row[col] != null ? String(row[col]) : ""}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-center py-8 text-muted-foreground">
            <Table2 className="h-12 w-12 mx-auto mb-2 opacity-50" />
            <p>No extraction results yet</p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

type ChatMessage = { role: "user" | "assistant"; content: string };

function UploadPage() {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const [runId, setRunId] = useState<number | null>(null);
  const [runStatus, setRunStatus] = useState<JobRunOut | null>(null);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [chatError, setChatError] = useState<string | null>(null);
  const chatListRef = useRef<HTMLDivElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const pollIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const queryClient = useQueryClient();

  // Fetch existing files
  const {
    data: filesData,
    isLoading,
    error,
    refetch,
  } = useQuery<FileListResponse>({
    queryKey: ["files"],
    queryFn: async () => {
      const response = await fetch("/api/files", { credentials: "include" });
      if (!response.ok) {
        throw new Error("Failed to fetch files");
      }
      return response.json();
    },
  });

  // Helper to convert file to base64
  const fileToBase64 = (file: File): Promise<string> => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.readAsDataURL(file);
      reader.onload = () => {
        const result = reader.result as string;
        const base64 = result.split(",")[1];
        resolve(base64);
      };
      reader.onerror = (error) => reject(error);
    });
  };

  // Upload mutation
  const uploadMutation = useMutation({
    mutationFn: async (files: File[]) => {
      console.log("[UPLOAD] Starting upload for", files.length, "files");
      
      console.log("[UPLOAD] Converting files to base64...");
      const filesData = await Promise.all(
        files.map(async (file) => {
          console.log("[UPLOAD] Converting:", file.name, "size:", file.size);
          const base64 = await fileToBase64(file);
          console.log("[UPLOAD] Converted:", file.name, "base64 length:", base64.length);
          return {
            name: file.name,
            content_base64: base64,
          };
        })
      );

      const requestBody = { files: filesData };
      console.log("[UPLOAD] Request body prepared, total files:", filesData.length);
      console.log("[UPLOAD] Request body size:", JSON.stringify(requestBody).length, "bytes");

      console.log("[UPLOAD] Sending POST to /api/files...");

      let response: Response;
      try {
        response = await fetch("/api/files", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(requestBody),
          credentials: "include",
        });
      } catch (networkError) {
        const msg = networkError instanceof Error ? networkError.message : String(networkError);
        console.error("[UPLOAD] Network error:", msg);
        throw new Error(
          msg.includes("fetch") || msg.includes("Network")
            ? "Network error: could not reach the server. On Databricks, open the app from the Apps launcher and retry. If it persists, the request may be too large (try a smaller file)."
            : msg,
        );
      }

      if (response.status === 502 && typeof window !== "undefined" && window.location?.hostname === "localhost") {
        console.log("[UPLOAD] Proxy failed with 502, trying direct backend call...");
        try {
          const directResponse = await fetch("http://localhost:8517/api/files", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(requestBody),
          });
          response = directResponse;
        } catch {
          // keep original response if direct call also fails
        }
      }

      console.log("[UPLOAD] Response status:", response.status);
      console.log("[UPLOAD] Response headers:", Object.fromEntries(response.headers.entries()));

      if (!response.ok) {
        let errorMessage = `Upload failed (${response.status})`;
        try {
          const errorData = await response.json();
          console.log("[UPLOAD] Error response JSON:", errorData);
          if (errorData.detail) {
            const d = errorData.detail;
            if (typeof d === "string") {
              errorMessage = d;
            } else if (d && typeof d === "object") {
              const parts = [
                d.message,
                d.hint,
                response.status === 401 ? d.fix_databricks : null,
                response.status === 401 ? d.fix_local : null,
                response.status === 403 ? d.fix : null,
              ].filter(Boolean);
              errorMessage = parts.join(" ");
            } else {
              errorMessage = JSON.stringify(d);
            }
          } else if (errorData.message) {
            errorMessage = errorData.message;
          }
        } catch (e) {
          console.log("[UPLOAD] Could not parse error as JSON:", e);
          try {
            const text = await response.text();
            console.log("[UPLOAD] Error response text:", text);
            if (text) errorMessage = text;
          } catch (e2) {
            console.log("[UPLOAD] Could not read error text:", e2);
          }
        }
        console.log("[UPLOAD] Final error message:", errorMessage);
        throw new Error(errorMessage);
      }

      const result = await response.json();
      console.log("[UPLOAD] Success response:", result);
      return result;
    },
    onSuccess: () => {
      console.log("[UPLOAD] Upload completed successfully");
      setSelectedFiles([]);
      queryClient.invalidateQueries({ queryKey: ["files"] });
    },
    onError: (error) => {
      console.error("[UPLOAD] Upload failed with error:", error);
    },
  });

  const triggerJobMutation = useMutation({
    mutationFn: () => triggerJobRun().then((r) => r.data),
    onSuccess: (data) => {
      setRunId(data.run_id);
      setRunStatus(null);
    },
    onError: () => {},
  });

  useEffect(() => {
    if (runId == null) return;
    const poll = async () => {
      try {
        const { data } = await getJobRun({ run_id: runId });
        setRunStatus(data);
        const terminal = isJobRunTerminal(data);
        if (terminal) {
          if (pollIntervalRef.current) {
            clearInterval(pollIntervalRef.current);
            pollIntervalRef.current = null;
          }
        }
      } catch (err) {
        // keep polling on transient errors
      }
    };
    poll();
    pollIntervalRef.current = setInterval(poll, JOB_POLL_INTERVAL_MS);
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    };
  }, [runId]);

  const isJobRunning =
    runId != null && runStatus != null && !isJobRunTerminal(runStatus);
  const jobFinished =
    runId != null && runStatus != null && isJobRunTerminal(runStatus);

  const handleExecuteProcessing = useCallback(() => {
    triggerJobMutation.mutate();
  }, [triggerJobMutation]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);

    const droppedFiles = Array.from(e.dataTransfer.files).filter((file) =>
      file.name.toLowerCase().endsWith(".pdf")
    );

    if (droppedFiles.length > 0) {
      setSelectedFiles((prev) => [...prev, ...droppedFiles]);
    }
  }, []);

  const handleFileSelect = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const files = e.target.files;
      if (files) {
        const pdfFiles = Array.from(files).filter((file) =>
          file.name.toLowerCase().endsWith(".pdf")
        );
        setSelectedFiles((prev) => [...prev, ...pdfFiles]);
      }
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    },
    []
  );

  const removeSelectedFile = useCallback((index: number) => {
    setSelectedFiles((prev) => prev.filter((_, i) => i !== index));
  }, []);

  const handleUpload = useCallback(() => {
    if (selectedFiles.length > 0) {
      uploadMutation.mutate(selectedFiles);
    }
  }, [selectedFiles, uploadMutation]);

  const handleSendChat = useCallback(async () => {
    const text = chatInput.trim();
    if (!text || chatLoading) return;
    const userMsg: ChatMessage = { role: "user", content: text };
    setChatInput("");
    setChatError(null);
    setChatMessages((prev) => [...prev, userMsg]);
    setChatLoading(true);
    try {
      const messagesForApi = [...chatMessages, userMsg].map((m) => ({
        role: m.role as "user" | "assistant" | "system",
        content: m.content,
      }));
      const { data } = await chat({ messages: messagesForApi });
      setChatMessages((prev) => [...prev, { role: "assistant", content: data.message.content }]);
    } catch (err) {
      let message: string;
      if (err instanceof ApiError) {
        message = `${err.message}\n(HTTP ${err.status})`;
      } else if (err instanceof Error) {
        message = err.message;
      } else {
        message = "Agent unavailable";
      }
      setChatError(message);
    } finally {
      setChatLoading(false);
    }
  }, [chatInput, chatLoading, chatMessages]);

  useEffect(() => {
    chatListRef.current?.scrollTo({ top: chatListRef.current.scrollHeight, behavior: "smooth" });
  }, [chatMessages]);

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Navbar />

      <main className="flex-1 container mx-auto px-4 py-8 max-w-4xl">
        <div className="space-y-6">
          {/* Header */}
          <div>
            <h1 className="text-3xl font-bold tracking-tight">PDF Upload</h1>
            <p className="text-muted-foreground mt-1">
              Upload PDF documents to your Databricks Volume
            </p>
          </div>

          {/* Upload Card */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Upload className="h-5 w-5" />
                Upload PDFs
              </CardTitle>
              <CardDescription>
                Drag and drop PDF files or click to browse
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Drop Zone */}
              <div
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                className={`
                  border-2 border-dashed rounded-lg p-8 text-center cursor-pointer
                  transition-colors duration-200
                  ${
                    isDragging
                      ? "border-primary bg-primary/5"
                      : "border-muted-foreground/25 hover:border-primary/50 hover:bg-muted/50"
                  }
                `}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".pdf"
                  multiple
                  onChange={handleFileSelect}
                  className="hidden"
                />
                <FileText className="h-12 w-12 mx-auto text-muted-foreground mb-4" />
                <p className="text-lg font-medium">
                  {isDragging
                    ? "Drop PDF files here"
                    : "Drag & drop PDF files here"}
                </p>
                <p className="text-sm text-muted-foreground mt-1">
                  or click to browse
                </p>
              </div>

              {/* Selected Files */}
              {selectedFiles.length > 0 && (
                <div className="space-y-2">
                  <p className="text-sm font-medium">
                    Selected files ({selectedFiles.length}):
                  </p>
                  <div className="space-y-1">
                    {selectedFiles.map((file, index) => (
                      <div
                        key={`${file.name}-${index}`}
                        className="flex items-center justify-between p-2 bg-muted rounded-md"
                      >
                        <div className="flex items-center gap-2 min-w-0">
                          <FileText className="h-4 w-4 flex-shrink-0 text-muted-foreground" />
                          <span className="text-sm truncate">{file.name}</span>
                          <span className="text-xs text-muted-foreground flex-shrink-0">
                            ({formatFileSize(file.size)})
                          </span>
                        </div>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 flex-shrink-0"
                          onClick={(e) => {
                            e.stopPropagation();
                            removeSelectedFile(index);
                          }}
                        >
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Upload Status */}
              {uploadMutation.isSuccess && (
                <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                  <CheckCircle className="h-4 w-4" />
                  <span className="text-sm">Files uploaded successfully!</span>
                </div>
              )}

              {uploadMutation.isError && (
                <div className="p-3 bg-destructive/10 border border-destructive/20 rounded-md">
                  <div className="flex items-start gap-2 text-destructive">
                    <AlertCircle className="h-4 w-4 mt-0.5 flex-shrink-0" />
                    <div className="text-sm space-y-1">
                      <p className="font-medium">Upload failed</p>
                      <p className="text-destructive/80 break-all">
                        {uploadMutation.error instanceof Error
                          ? uploadMutation.error.message
                          : "Unknown error occurred"}
                      </p>
                      {(uploadMutation.error instanceof Error &&
                        (uploadMutation.error.message.includes("Authentication required") ||
                          uploadMutation.error.message.includes("x-forwarded-access-token"))) && (
                        <p className="text-muted-foreground mt-2">
                          On Databricks: open this app from the <strong>Apps launcher</strong> (Apps menu or workspace app link), not a direct URL, so the platform can authenticate your requests.
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              )}

              {/* Upload Button */}
              <Button
                onClick={handleUpload}
                disabled={selectedFiles.length === 0 || uploadMutation.isPending}
                className="w-full"
              >
                {uploadMutation.isPending ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4" />
                    Upload {selectedFiles.length > 0 && `(${selectedFiles.length})`}
                  </>
                )}
              </Button>
            </CardContent>
          </Card>

          {/* Processing Card */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Play className="h-5 w-5" />
                Execute processing
              </CardTitle>
              <CardDescription>
                Run the configured Databricks job to process uploaded PDFs
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <Button
                onClick={handleExecuteProcessing}
                disabled={isJobRunning || triggerJobMutation.isPending}
                className="w-full"
              >
                {triggerJobMutation.isPending ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    Starting...
                  </>
                ) : isJobRunning ? (
                  <>
                    <RefreshCw className="h-4 w-4 animate-spin" />
                    Running...
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4" />
                    Execute processing
                  </>
                )}
              </Button>

              {isJobRunning && (
                <div className="space-y-2">
                  <p className="text-sm text-muted-foreground">Job running...</p>
                  <Progress indeterminate />
                </div>
              )}

              {jobFinished && runStatus && (
                <div className="space-y-2">
                  {runStatus.result_state === "SUCCESS" ? (
                    <div className="flex items-center gap-2 text-green-600 dark:text-green-400">
                      <CheckCircle className="h-4 w-4" />
                      <span className="text-sm font-medium">Completed</span>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 text-amber-600 dark:text-amber-400">
                      <AlertCircle className="h-4 w-4" />
                      <span className="text-sm font-medium">
                        {runStatus.result_state ?? runStatus.life_cycle_state}
                      </span>
                    </div>
                  )}
                  {runStatus.execution_duration_ms != null && (
                    <p className="text-sm text-muted-foreground">
                      Execution time: {formatDurationMs(runStatus.execution_duration_ms)}
                    </p>
                  )}
                </div>
              )}

              {triggerJobMutation.isError && (
                <div className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/5 p-3 text-destructive text-sm">
                  <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
                  <div className="min-w-0 space-y-1">
                    <p className="font-medium">Could not start processing job</p>
                    <p className="whitespace-pre-wrap break-words text-muted-foreground">
                      {triggerJobMutation.error instanceof ApiError
                        ? triggerJobMutation.error.message
                        : triggerJobMutation.error instanceof Error
                          ? triggerJobMutation.error.message
                          : "Failed to start job"}
                    </p>
                    {triggerJobMutation.error instanceof ApiError && (
                      <p className="text-xs text-muted-foreground">HTTP {triggerJobMutation.error.status}</p>
                    )}
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Files Table Card */}
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="flex items-center gap-2">
                    <FileText className="h-5 w-5" />
                    Existing PDFs
                  </CardTitle>
                  <CardDescription>
                    PDF files currently in the volume
                  </CardDescription>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => refetch()}
                  disabled={isLoading}
                >
                  <RefreshCw
                    className={`h-4 w-4 ${isLoading ? "animate-spin" : ""}`}
                  />
                  Refresh
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {isLoading ? (
                <div className="space-y-2">
                  {[1, 2, 3].map((i) => (
                    <Skeleton key={i} className="h-12 w-full" />
                  ))}
                </div>
              ) : error ? (
                <div className="flex items-center gap-2 text-destructive py-4">
                  <AlertCircle className="h-4 w-4" />
                  <span className="text-sm">Failed to load files</span>
                </div>
              ) : filesData?.files && filesData.files.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                          Name
                        </th>
                        <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                          Size
                        </th>
                        <th className="text-left py-3 px-2 text-sm font-medium text-muted-foreground">
                          Modified
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {filesData.files.map((file) => (
                        <tr
                          key={file.path}
                          className="border-b last:border-0 hover:bg-muted/50"
                        >
                          <td className="py-3 px-2">
                            <div className="flex items-center gap-2">
                              <FileText className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                              <span className="text-sm truncate max-w-[200px]">
                                {file.name}
                              </span>
                            </div>
                          </td>
                          <td className="py-3 px-2 text-sm text-muted-foreground">
                            {formatFileSize(file.size)}
                          </td>
                          <td className="py-3 px-2 text-sm text-muted-foreground">
                            {formatDate(file.modified_at)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center py-8 text-muted-foreground">
                  <FileText className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>No PDF files in the volume yet</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Extraction results */}
          <AiQueryResultsCard />

          {/* Chat with agent endpoint */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <MessageSquare className="h-5 w-5" />
                Chat
              </CardTitle>
              <CardDescription>
                Talk to the Databricks agent endpoint
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div
                ref={chatListRef}
                className="min-h-[200px] max-h-[320px] overflow-y-auto rounded-lg border bg-muted/30 p-3 space-y-3"
              >
                {chatMessages.length === 0 && (
                  <p className="text-sm text-muted-foreground text-center py-4">
                    Send a message to start the conversation.
                  </p>
                )}
                {chatMessages.map((msg, idx) => (
                  <div
                    key={idx}
                    className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                  >
                    <div
                      className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
                        msg.role === "user"
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted border"
                      }`}
                    >
                      {msg.role === "assistant" ? (
                        <ChatMessageContent content={msg.content} />
                      ) : (
                        msg.content
                      )}
                    </div>
                  </div>
                ))}
              </div>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSendChat()}
                  placeholder="Type a message..."
                  className="flex-1 rounded-md border bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:opacity-50"
                  disabled={chatLoading}
                />
                <Button
                  onClick={handleSendChat}
                  disabled={!chatInput.trim() || chatLoading}
                  size="icon"
                  className="shrink-0"
                >
                  {chatLoading ? (
                    <RefreshCw className="h-4 w-4 animate-spin" />
                  ) : (
                    <Send className="h-4 w-4" />
                  )}
                </Button>
              </div>
              {chatError && (
                <div className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/5 p-3 text-destructive text-sm">
                  <AlertCircle className="h-4 w-4 flex-shrink-0 mt-0.5" />
                  <div className="min-w-0 space-y-1">
                    <p className="font-medium">Agent request failed</p>
                    <p className="whitespace-pre-wrap break-words text-muted-foreground">{chatError}</p>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
}
