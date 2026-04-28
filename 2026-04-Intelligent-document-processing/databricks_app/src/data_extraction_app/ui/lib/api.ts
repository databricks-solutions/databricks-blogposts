import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";

export interface AppAiQueryOut {
  columns?: string[];
  rows: Record<string, unknown>[];
}

export interface ChatIn {
  messages: ChatMessageIn[];
}

export interface ChatMessageIn {
  content: string;
  role: string;
}

export interface ChatMessageOut {
  content: string;
  role: string;
}

export interface ChatOut {
  message: ChatMessageOut;
}

export interface ComplexValue {
  display?: string | null;
  primary?: boolean | null;
  ref?: string | null;
  type?: string | null;
  value?: string | null;
}

export interface FileInfo {
  modified_at: string | null;
  name: string;
  path: string;
  size: number;
}

export interface FileListOut {
  files: FileInfo[];
}

export interface FileUploadIn {
  content_base64: string;
  name: string;
}

export interface FilesUploadIn {
  files: FileUploadIn[];
}

export interface HTTPValidationError {
  detail?: ValidationError[];
}

export interface JobRunOut {
  end_time?: number | null;
  execution_duration_ms?: number | null;
  life_cycle_state: string;
  result_state?: string | null;
  run_id: number;
  start_time?: number | null;
}

export interface JobRunTriggerOut {
  job_id?: string | null;
  run_id: number;
}

export interface Name {
  family_name?: string | null;
  given_name?: string | null;
}

export interface User {
  active?: boolean | null;
  display_name?: string | null;
  emails?: ComplexValue[] | null;
  entitlements?: ComplexValue[] | null;
  external_id?: string | null;
  groups?: ComplexValue[] | null;
  id?: string | null;
  name?: Name | null;
  roles?: ComplexValue[] | null;
  schemas?: UserSchema[] | null;
  user_name?: string | null;
}

export const UserSchema = {
  "urn:ietf:params:scim:schemas:core:2.0:User": "urn:ietf:params:scim:schemas:core:2.0:User",
  "urn:ietf:params:scim:schemas:extension:workspace:2.0:User": "urn:ietf:params:scim:schemas:extension:workspace:2.0:User",
} as const;

export type UserSchema = (typeof UserSchema)[keyof typeof UserSchema];

export interface ValidationError {
  ctx?: Record<string, unknown>;
  input?: unknown;
  loc: (string | number)[];
  msg: string;
  type: string;
}

export interface VersionOut {
  version: string;
}

export interface GetJobRunParams {
  run_id: number;
}

export class ApiError extends Error {
  status: number;
  statusText: string;
  body: unknown;

  constructor(status: number, statusText: string, body: unknown) {
    super(`HTTP ${status}: ${statusText}`);
    this.name = "ApiError";
    this.status = status;
    this.statusText = statusText;
    this.body = body;
  }
}

export const authDiagnostic = async (options?: RequestInit): Promise<{ data: unknown }> => {
  const res = await fetch("/api/auth/diagnostic", { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const authDiagnosticKey = () => {
  return ["/api/auth/diagnostic"] as const;
};

export function useAuthDiagnostic<TData = { data: unknown }>(options?: { query?: Omit<UseQueryOptions<{ data: unknown }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: authDiagnosticKey(), queryFn: () => authDiagnostic(), ...options?.query });
}

export function useAuthDiagnosticSuspense<TData = { data: unknown }>(options?: { query?: Omit<UseSuspenseQueryOptions<{ data: unknown }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: authDiagnosticKey(), queryFn: () => authDiagnostic(), ...options?.query });
}

export const chat = async (data: ChatIn, options?: RequestInit): Promise<{ data: ChatOut }> => {
  const res = await fetch("/api/chat", { ...options, method: "POST", headers: { "Content-Type": "application/json", ...options?.headers }, body: JSON.stringify(data) });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export function useChat(options?: { mutation?: UseMutationOptions<{ data: ChatOut }, ApiError, ChatIn> }) {
  return useMutation({ mutationFn: (data) => chat(data), ...options?.mutation });
}

export const currentUser = async (options?: RequestInit): Promise<{ data: User }> => {
  const res = await fetch("/api/current-user", { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const currentUserKey = () => {
  return ["/api/current-user"] as const;
};

export function useCurrentUser<TData = { data: User }>(options?: { query?: Omit<UseQueryOptions<{ data: User }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: currentUserKey(), queryFn: () => currentUser(), ...options?.query });
}

export function useCurrentUserSuspense<TData = { data: User }>(options?: { query?: Omit<UseSuspenseQueryOptions<{ data: User }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: currentUserKey(), queryFn: () => currentUser(), ...options?.query });
}

export const listFiles = async (options?: RequestInit): Promise<{ data: FileListOut }> => {
  const res = await fetch("/api/files", { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const listFilesKey = () => {
  return ["/api/files"] as const;
};

export function useListFiles<TData = { data: FileListOut }>(options?: { query?: Omit<UseQueryOptions<{ data: FileListOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: listFilesKey(), queryFn: () => listFiles(), ...options?.query });
}

export function useListFilesSuspense<TData = { data: FileListOut }>(options?: { query?: Omit<UseSuspenseQueryOptions<{ data: FileListOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: listFilesKey(), queryFn: () => listFiles(), ...options?.query });
}

export const uploadFiles = async (data: FilesUploadIn, options?: RequestInit): Promise<{ data: FileListOut }> => {
  const res = await fetch("/api/files", { ...options, method: "POST", headers: { "Content-Type": "application/json", ...options?.headers }, body: JSON.stringify(data) });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export function useUploadFiles(options?: { mutation?: UseMutationOptions<{ data: FileListOut }, ApiError, FilesUploadIn> }) {
  return useMutation({ mutationFn: (data) => uploadFiles(data), ...options?.mutation });
}

export const triggerJobRun = async (options?: RequestInit): Promise<{ data: JobRunTriggerOut }> => {
  const res = await fetch("/api/jobs/run", { ...options, method: "POST" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export function useTriggerJobRun(options?: { mutation?: UseMutationOptions<{ data: JobRunTriggerOut }, ApiError, void> }) {
  return useMutation({ mutationFn: () => triggerJobRun(), ...options?.mutation });
}

export const getJobRun = async (params: GetJobRunParams, options?: RequestInit): Promise<{ data: JobRunOut }> => {
  const res = await fetch(`/api/jobs/runs/${params.run_id}`, { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const getJobRunKey = (params?: GetJobRunParams) => {
  return ["/api/jobs/runs/{run_id}", params] as const;
};

export function useGetJobRun<TData = { data: JobRunOut }>(options: { params: GetJobRunParams; query?: Omit<UseQueryOptions<{ data: JobRunOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: getJobRunKey(options.params), queryFn: () => getJobRun(options.params), ...options?.query });
}

export function useGetJobRunSuspense<TData = { data: JobRunOut }>(options: { params: GetJobRunParams; query?: Omit<UseSuspenseQueryOptions<{ data: JobRunOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: getJobRunKey(options.params), queryFn: () => getJobRun(options.params), ...options?.query });
}

export const getAppAiQuery = async (options?: RequestInit): Promise<{ data: AppAiQueryOut }> => {
  const res = await fetch("/api/query/app_ai_query", { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const getAppAiQueryKey = () => {
  return ["/api/query/app_ai_query"] as const;
};

export function useGetAppAiQuery<TData = { data: AppAiQueryOut }>(options?: { query?: Omit<UseQueryOptions<{ data: AppAiQueryOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: getAppAiQueryKey(), queryFn: () => getAppAiQuery(), ...options?.query });
}

export function useGetAppAiQuerySuspense<TData = { data: AppAiQueryOut }>(options?: { query?: Omit<UseSuspenseQueryOptions<{ data: AppAiQueryOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: getAppAiQueryKey(), queryFn: () => getAppAiQuery(), ...options?.query });
}

export const version = async (options?: RequestInit): Promise<{ data: VersionOut }> => {
  const res = await fetch("/api/version", { ...options, method: "GET" });
  if (!res.ok) {
    const body = await res.text();
    let parsed: unknown;
    try { parsed = JSON.parse(body); } catch { parsed = body; }
    throw new ApiError(res.status, res.statusText, parsed);
  }
  return { data: await res.json() };
};

export const versionKey = () => {
  return ["/api/version"] as const;
};

export function useVersion<TData = { data: VersionOut }>(options?: { query?: Omit<UseQueryOptions<{ data: VersionOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useQuery({ queryKey: versionKey(), queryFn: () => version(), ...options?.query });
}

export function useVersionSuspense<TData = { data: VersionOut }>(options?: { query?: Omit<UseSuspenseQueryOptions<{ data: VersionOut }, ApiError, TData>, "queryKey" | "queryFn"> }) {
  return useSuspenseQuery({ queryKey: versionKey(), queryFn: () => version(), ...options?.query });
}

