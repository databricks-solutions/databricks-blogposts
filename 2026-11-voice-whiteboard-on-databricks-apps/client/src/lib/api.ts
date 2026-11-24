// REST calls to the app's backend.

import type { AccessView, BoardList } from './types'

/**
 * In local development a `?as=Name` in the page URL simulates being someone
 * else; it has to accompany API calls too. (Deployed, identity comes from the
 * workspace sign-in and this adds nothing.)
 */
function url(path: string): string {
  const as = new URLSearchParams(location.search).get('as')
  return as ? `${path}${path.includes('?') ? '&' : '?'}as=${encodeURIComponent(as)}` : path
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url(path), {
    ...init,
    headers: init?.body ? { 'Content-Type': 'application/json' } : undefined,
  })
  if (!res.ok) {
    let detail = `HTTP ${res.status}`
    try {
      detail = ((await res.json()) as { detail?: string }).detail ?? detail
    } catch {
      // not JSON: keep the status
    }
    throw new Error(detail)
  }
  return (await res.json()) as T
}

export const api = {
  me: () => call<{ name: string; email: string }>('/api/me'),
  boards: () => call<BoardList>('/api/boards'),
  create: () => call<{ id: string }>('/api/boards', { method: 'POST', body: '{}' }),
  remove: (id: string) => call<{ deleted: string }>(`/api/boards/${id}`, { method: 'DELETE' }),
  share: (id: string, change: { link?: string; add?: string[]; remove?: string[] }) =>
    call<AccessView>(`/api/boards/${id}/access`, { method: 'POST', body: JSON.stringify(change) }),
}
