import { useRef, useState, useEffect, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { FileText, Loader2, Trash2, Upload, X } from "lucide-react";
import { api, type Document, type ProcessingResult, type DetectedParty } from "@/lib/api";
import { formatDate, plural } from "@/lib/format";
import { cn } from "@/lib/utils";
import ConfirmDialog from "@/components/ConfirmDialog";
import { Button, Card, IconButton, Pill } from "@/components/ui";

const ACCEPT = ".pdf,.doc,.docx,.txt,.png,.jpg,.jpeg,.pptx";

function formatSize(bytes: number | null): string {
  if (!bytes) return "—";
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

export default function DocumentManager({ matterId }: { matterId: string }) {
  const queryClient = useQueryClient();
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [processing, setProcessing] = useState<string | null>(null);
  const [processingStep, setProcessingStep] = useState<string>("");
  const [processingError, setProcessingError] = useState<string | null>(null);
  const [processingResult, setProcessingResult] = useState<ProcessingResult | null>(null);
  const [dragging, setDragging] = useState(false);
  const [toDelete, setToDelete] = useState<Document | null>(null);

  // Poll processing status
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const startPolling = useCallback(
    (docId: string) => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = setInterval(async () => {
        try {
          const status = await api.getProcessingStatus(matterId, docId);
          setProcessingStep(status.step || status.status);

          if (status.status === "completed") {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setProcessing(null);
            setProcessingStep("");
            if (status.result) setProcessingResult(status.result);
            for (const key of ["documents", "issues", "evidence", "timeline", "parties"]) {
              queryClient.invalidateQueries({ queryKey: [key, matterId] });
            }
          } else if (status.status === "error") {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setProcessing(null);
            setProcessingStep("");
            setProcessingError(status.error || "Processing failed.");
          }
        } catch {
          // ignore polling errors
        }
      }, 2000);
    },
    [matterId, queryClient],
  );

  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, []);

  const { data: docs = [], isLoading } = useQuery({
    queryKey: ["documents", matterId],
    queryFn: () => api.listDocuments(matterId),
  });

  const uploadMut = useMutation({
    mutationFn: (file: File) => api.uploadDocument(matterId, file),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["documents", matterId] });
      queryClient.invalidateQueries({ queryKey: ["matters"] });
    },
  });

  const processMut = useMutation({
    mutationFn: (docId: string) => {
      setProcessing(docId);
      setProcessingStep("starting");
      setProcessingError(null);
      return api.processDocument(matterId, docId);
    },
    onSuccess: (_result, docId) => startPolling(docId),
    onError: (e) => {
      setProcessing(null);
      setProcessingStep("");
      setProcessingError(e.message);
    },
  });

  const deleteMut = useMutation({
    mutationFn: (docId: string) => api.deleteDocument(matterId, docId),
    onSuccess: (_, docId) => {
      queryClient.setQueryData<Document[]>(["documents", matterId], (old = []) =>
        old.filter((d) => d.document_id !== docId),
      );
      queryClient.invalidateQueries({ queryKey: ["documents", matterId] });
      queryClient.invalidateQueries({ queryKey: ["matters"] });
      setToDelete(null);
    },
  });

  const upload = (files: FileList | null | undefined) => {
    const file = files?.[0];
    if (file) uploadMut.mutate(file);
  };

  const dropProps = {
    onDragOver: (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(true);
    },
    onDragLeave: () => setDragging(false),
    onDrop: (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      upload(e.dataTransfer.files);
    },
  };

  const detected = processingResult?.detected_parties ?? [];

  return (
    <div className="space-y-4 px-4 py-6 md:px-8" {...dropProps}>
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-semibold text-foreground">Documents</h2>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Stored in a Unity Catalog volume. Processing extracts issues, evidence, a timeline and the
            parties.
          </p>
        </div>
        <Button variant="primary" onClick={() => fileInputRef.current?.click()} disabled={uploadMut.isPending}>
          {uploadMut.isPending ? (
            <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
          ) : (
            <Upload className="h-4 w-4" aria-hidden="true" />
          )}
          {uploadMut.isPending ? "Uploading…" : "Upload Document"}
        </Button>
        <input
          ref={fileInputRef}
          type="file"
          className="hidden"
          accept={ACCEPT}
          onChange={(e) => upload(e.target.files)}
        />
      </div>

      <div aria-live="polite" className="space-y-3">
        {uploadMut.isError && (
          <p role="alert" className="text-sm text-destructive">
            Upload failed: {uploadMut.error.message}
          </p>
        )}
        {processingError && (
          <p role="alert" className="text-sm text-destructive">
            Processing failed: {processingError}
          </p>
        )}
        {processingResult && processingResult.status === "processed" && (
          <Card className="px-5 py-4">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-sm font-medium text-foreground">
                  Processed into {plural(processingResult.chunks, "chunk")},{" "}
                  {plural(processingResult.issues, "issue")}, {plural(processingResult.evidence, "evidence item")}{" "}
                  and {plural(processingResult.timeline_events, "timeline event")}.
                </p>
                {detected.length > 0 && (
                  <p className="mt-1 text-sm text-muted-foreground">
                    {plural(detected.length, "party", "parties")} found and added to the Parties tab:{" "}
                    {detected.map((p: DetectedParty) => p.name).join(", ")}.
                  </p>
                )}
              </div>
              <IconButton label="Dismiss" onClick={() => setProcessingResult(null)}>
                <X className="h-4 w-4" aria-hidden="true" />
              </IconButton>
            </div>
          </Card>
        )}
      </div>

      {toDelete && (
        <ConfirmDialog
          title="Delete document?"
          message={
            <p>
              <span className="font-medium text-foreground">{toDelete.file_name}</span> will be
              deleted from the volume, with its extracted text and search chunks. Issues, evidence
              and parties already extracted from it stay.
            </p>
          }
          confirmLabel="Delete Document"
          pending={deleteMut.isPending}
          error={deleteMut.error?.message}
          onConfirm={() => deleteMut.mutate(toDelete.document_id)}
          onCancel={() => {
            setToDelete(null);
            deleteMut.reset();
          }}
        />
      )}

      {!isLoading && docs.length === 0 ? (
        <button
          type="button"
          onClick={() => fileInputRef.current?.click()}
          className={cn(
            "flex w-full flex-col items-center gap-2 rounded-lg border border-dashed bg-card px-6 py-12 text-center transition-colors duration-100",
            dragging ? "border-primary bg-raised" : "border-input/60 hover:border-input",
          )}
        >
          <Upload className="h-6 w-6 text-muted-foreground" aria-hidden="true" />
          <span className="text-sm font-medium text-foreground">Drop a file here, or choose one</span>
          <span className="text-sm text-muted-foreground">PDF, Word, PowerPoint, text or an image</span>
        </button>
      ) : (
        <Card className={cn(dragging && "border-primary")}>
          <div
            className="hidden h-10 grid-cols-[minmax(0,1fr)_120px_96px_120px_176px] items-center gap-4 border-b border-border px-5 text-xs text-muted-foreground md:grid"
            aria-hidden="true"
          >
            <span>Document</span>
            <span>Type</span>
            <span className="text-right">Size</span>
            <span className="text-right">Uploaded</span>
            <span />
          </div>
          <ul>
            {docs.map((doc: Document) => {
              const isProcessing = processing === doc.document_id;
              return (
                <li
                  key={doc.document_id}
                  className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-4 gap-y-1 border-b border-rule px-5 py-3 last:border-b-0 md:grid-cols-[minmax(0,1fr)_120px_96px_120px_176px]"
                >
                  <div className="flex min-w-0 items-center gap-3">
                    <FileText className="h-4 w-4 flex-none text-muted-foreground" aria-hidden="true" />
                    <div className="min-w-0">
                      <p className="truncate font-medium text-foreground" title={doc.file_name}>
                        {doc.file_name}
                      </p>
                      <p className="text-sm text-muted-foreground md:hidden">
                        {(doc.document_type || "other").replace("_", " ")} · {formatSize(doc.file_size)}
                      </p>
                    </div>
                  </div>
                  <span className="hidden text-sm text-muted-foreground capitalize md:block">
                    {(doc.document_type || "other").replace("_", " ")}
                  </span>
                  <span className="num hidden text-right text-sm md:block">{formatSize(doc.file_size)}</span>
                  <span className="num hidden text-right text-sm text-muted-foreground md:block">
                    {formatDate(doc.uploaded_at)}
                  </span>
                  <div className="flex items-center justify-end gap-2">
                    {doc.processed_at ? (
                      <Pill tone="positive">Processed</Pill>
                    ) : isProcessing ? (
                      <span className="flex items-center gap-1.5 text-sm text-muted-foreground" aria-live="polite">
                        <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
                        <span className="capitalize">{(processingStep || "processing").replace(/_/g, " ")}…</span>
                      </span>
                    ) : (
                      <Button size="sm" onClick={() => processMut.mutate(doc.document_id)} disabled={!!processing}>
                        Process
                      </Button>
                    )}
                    <IconButton label={`Delete ${doc.file_name}`} tone="danger" onClick={() => setToDelete(doc)}>
                      <Trash2 className="h-4 w-4" aria-hidden="true" />
                    </IconButton>
                  </div>
                </li>
              );
            })}
          </ul>
        </Card>
      )}
    </div>
  );
}
