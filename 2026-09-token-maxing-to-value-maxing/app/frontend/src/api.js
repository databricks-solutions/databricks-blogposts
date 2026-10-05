// GatewayIQ API client + shared helpers

// ---- Identity ------------------------------------------------------------
// The chosen persona (demo picker / switcher). Sent on every API call so the
// backend scopes the data. Real SSO identity is used automatically when this
// is unset and the logged-in user is in the roster.
const AS_KEY = 'gatewayiq_as'
export const getIdentity = () => localStorage.getItem(AS_KEY) || ''
export function setIdentity(email) {
  if (email) localStorage.setItem(AS_KEY, email)
  else localStorage.removeItem(AS_KEY)
  _cache = {} // scope changed → drop lazy-dataset cache
}

function h() {
  const as = getIdentity()
  return as ? { 'X-GatewayIQ-As': as } : {}
}

export async function fetchMe() {
  const as = getIdentity()
  const r = await fetch('/api/me' + (as ? `?as=${encodeURIComponent(as)}` : ''), { headers: h() })
  if (!r.ok) throw new Error('Failed to resolve identity')
  return r.json()
}

export async function login(email, password) {
  const r = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
  })
  if (r.status === 401) throw new Error('Incorrect email or password.')
  if (!r.ok) throw new Error('Sign-in failed. Please try again.')
  return r.json() // { persona }
}

export async function fetchBundle() {
  const r = await fetch('/api/bundle', { headers: h() })
  if (r.status === 401) throw new Error('no-identity')
  if (!r.ok) throw new Error('Failed to load data bundle')
  return r.json() // { ds_name: {columns, rows}, ... }
}

let _cache = {}
export async function fetchDataset(name) {
  if (_cache[name]) return _cache[name]
  const r = await fetch(`/api/data/${name}`, { headers: h() })
  if (!r.ok) throw new Error(`Failed to load ${name}`)
  const j = await r.json()
  _cache[name] = j.rows
  return j.rows
}

// ---- User administration (admins & managers) -----------------------------
const USER_ERRORS = {
  'not-a-manager': 'You don’t have permission to manage users.',
  'bad-email': 'Please enter a valid email address.',
  'missing-name': 'Please enter the person’s name.',
  'bad-role': 'Please choose a role.',
  'user-needs-manager': 'A User must be assigned to a manager. Pick a manager, or set the role to Manager/Admin.',
  'manager-out-of-scope': 'You can only assign users under a manager on your own team.',
  'cannot-remove-self': 'You can’t remove your own account.',
  'out-of-scope': 'That user isn’t on your team.',
  'bad-target': 'That user no longer exists.',
}
async function _usersReq(path, body) {
  const r = await fetch(path, {
    method: body ? 'POST' : 'GET',
    headers: body ? { ...h(), 'Content-Type': 'application/json' } : h(),
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!r.ok) {
    let detail = ''
    try { detail = (await r.json()).detail } catch {}
    throw new Error(USER_ERRORS[detail] || 'User update failed. Please try again.')
  }
  return r.json()
}
export const fetchUsers = () => _usersReq('/api/users')
export const saveUser = (user) => _usersReq('/api/users/save', user)
export const removeUser = (email) => _usersReq('/api/users/remove', { email })

// ---- Word cloud ----------------------------------------------------------
export async function wordcloudOptions() {
  const r = await fetch('/api/wordcloud/options', { headers: h() })
  if (!r.ok) throw new Error('Failed to load options')
  return r.json()
}
export async function wordcloudLast() {
  const r = await fetch('/api/wordcloud/last', { headers: h() })
  if (!r.ok) throw new Error('Failed to load word cloud')
  return r.json()
}
// ---- Cost Forecast & My Usage --------------------------------------------
export async function fetchForecast() {
  const r = await fetch('/api/forecast', { headers: h() })
  if (!r.ok) throw new Error('Failed to load forecast')
  return r.json()
}
export async function myusageOptions() {
  const r = await fetch('/api/myusage/options', { headers: h() })
  if (!r.ok) throw new Error('Failed to load options')
  return r.json()
}
export async function fetchMyUsage(body) {
  const r = await fetch('/api/myusage', {
    method: 'POST',
    headers: { ...h(), 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error('Failed to load usage')
  return r.json()
}

export async function wordcloudGenerate(body) {
  const r = await fetch('/api/wordcloud/generate', {
    method: 'POST',
    headers: { ...h(), 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) {
    let d = ''
    try { d = (await r.json()).detail } catch {}
    throw new Error(d || 'Generation failed')
  }
  return r.json()
}

// ---- Series color palette (matches theme.css categorical vars) ----
export const PALETTE = ['#35D6BE', '#4C8DFF', '#A98CFF', '#FFC15A', '#FF7AA8', '#57D98A', '#46C7FF', '#FF6B6B']
export const SEV_COLORS = {
  CRITICAL: '#FF5C5C', HIGH: '#FF9E4D', MEDIUM: '#FFD24C', LOW: '#57D98A', NORMAL: '#6B7688',
}
export const colorFor = (i) => PALETTE[i % PALETTE.length]

// ---- Formatters ----
export const fmtInt = (n) => (n == null || isNaN(n) ? '—' : Math.round(+n).toLocaleString('en-US'))
export const fmtNum = (n, d = 1) => (n == null || isNaN(n) ? '—' : (+n).toLocaleString('en-US', { maximumFractionDigits: d }))
export function fmtCompact(n) {
  if (n == null || isNaN(n)) return '—'
  n = +n
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return Math.round(n).toString()
}
export const fmtUsd = (n, compact = false) =>
  n == null || isNaN(n) ? '—' : '$' + (compact ? fmtCompact(n) : (+n).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }))
// Axis-friendly USD: keeps one decimal for small fractional values (e.g. daily
// per-user cost ~$0–2) so ticks don't collapse to duplicate whole dollars.
export function fmtUsdAxis(n) {
  if (n == null || isNaN(n)) return '—'
  n = +n
  if (Math.abs(n) >= 1000) return fmtUsd(n, true)
  return '$' + n.toLocaleString('en-US', { maximumFractionDigits: Math.abs(n) < 10 ? 1 : 0 })
}
export function fmtDate(s) {
  if (!s) return '—'
  const d = new Date(s)
  if (isNaN(d)) return String(s).slice(0, 10)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}
export const shortDate = (s) => (s ? String(s).slice(0, 10) : '—')

// group + aggregate helper
export function groupSum(rows, key, valKey) {
  const m = new Map()
  for (const r of rows) {
    const k = r[key]
    m.set(k, (m.get(k) || 0) + (+r[valKey] || 0))
  }
  return [...m.entries()].map(([k, v]) => ({ [key]: k, [valKey]: v }))
}
