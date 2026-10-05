import React, { useEffect, useState } from 'react'
import { Mail, Users2, User, CalendarRange, Sparkles, Send, Clock, Link2, Unlink, ShieldCheck } from 'lucide-react'
import { Card, Spinner } from '../components/ui.jsx'

async function post(path, body, headers) {
  const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body) })
  const j = await r.json().catch(() => ({}))
  if (!r.ok) throw new Error(j.detail || 'Request failed')
  return j
}
const asHeader = () => {
  const as = localStorage.getItem('gatewayiq_as')
  return as ? { 'X-GatewayIQ-As': as } : {}
}

export default function Notifications({ persona }) {
  const [opts, setOpts] = useState(null)
  const [scope, setScope] = useState('user')
  const [target, setTarget] = useState('')
  const [teamKey, setTeamKey] = useState('')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [preview, setPreview] = useState(null)
  const [busy, setBusy] = useState(false)
  const [sendMsg, setSendMsg] = useState(null)
  const [err, setErr] = useState(null)
  const [plan, setPlan] = useState(null)      // dry-run recipient list awaiting confirm
  const [sending, setSending] = useState(false)
  const [sendResult, setSendResult] = useState(null)
  const [gmail, setGmail] = useState(null)
  const [oauthCid, setOauthCid] = useState('')
  const [oauthSecret, setOauthSecret] = useState('')
  const [clientMsg, setClientMsg] = useState(null)
  const [connecting, setConnecting] = useState(false)

  const body = () => ({ scope, target_email: target, team_key: teamKey, date_from: from, date_to: to })

  const refreshGmail = () =>
    fetch('/api/notifications/gmail-status', { headers: asHeader() })
      .then((r) => r.json()).then((g) => setGmail(g)).catch(() => {})

  async function connectGmail() {
    setConnecting(true); setErr(null)
    try {
      const { authorize_url } = await (await fetch('/api/gmail/oauth/start', { headers: asHeader() })).json()
        .then((j) => j.authorize_url ? j : Promise.reject(new Error(j.detail || 'Could not start Gmail connect')))
      // Open Google's consent screen in a popup; poll status until it closes.
      const popup = window.open(authorize_url, 'gatewayiq_gmail', 'width=520,height=680')
      const timer = setInterval(async () => {
        if (popup && popup.closed) {
          clearInterval(timer); setConnecting(false); refreshGmail()
        }
      }, 1000)
    } catch (e) { setErr(e.message); setConnecting(false) }
  }

  async function disconnectGmail() {
    try { await post('/api/gmail/oauth/disconnect', {}, asHeader()); refreshGmail() }
    catch (e) { setErr(e.message) }
  }

  async function saveOauthClient() {
    setClientMsg(null)
    try {
      const r = await post('/api/gmail/oauth/client', { client_id: oauthCid, client_secret: oauthSecret }, asHeader())
      setClientMsg({ ok: true, text: `Saved. Register this redirect URI on the OAuth client: ${r.redirect_uri}` })
      setOauthSecret(''); refreshGmail()
    } catch (e) { setClientMsg({ ok: false, text: e.message }) }
  }

  useEffect(() => {
    let alive = true
    fetch('/api/notifications/options', { headers: asHeader() })
      .then((r) => r.json())
      .then((o) => {
        if (!alive) return
        setOpts(o)
        if (o.members?.length) setTarget(o.members[0].email)
        if (o.teams?.length) setTeamKey(o.teams[0].key)
        // default to the last 7 days of data
        const max = o.date_max
        setTo(max || '')
        if (max) { const d = new Date(max); d.setDate(d.getDate() - 6); setFrom(d.toISOString().slice(0, 10)) }
        else setFrom(o.date_min || '')
      })
      .catch((e) => alive && setErr(e.message))
    fetch('/api/notifications/gmail-status', { headers: asHeader() })
      .then((r) => r.json()).then((g) => alive && setGmail(g)).catch(() => {})
    return () => { alive = false }
  }, [])

  async function generate() {
    setBusy(true); setErr(null); setSendMsg(null)
    try {
      const body = { scope, target_email: target, team_key: teamKey, date_from: from, date_to: to }
      setPreview(await post('/api/notifications/preview', body, asHeader()))
    } catch (e) { setErr(e.message) } finally { setBusy(false) }
  }

  async function sendTest() {
    setSendMsg(null); setErr(null)
    try {
      const r = await post('/api/notifications/send-test', { ...body(), subject: preview?.subject }, asHeader())
      setSendMsg({ ok: true, text: `Sent to ${r.to}.` })
    } catch (e) { setSendMsg({ ok: false, text: e.message }) }
  }

  async function planSend() {
    setErr(null); setSendResult(null); setSendMsg(null)
    try {
      setPlan(await post('/api/notifications/send', { ...body(), dry_run: true }, asHeader()))
    } catch (e) { setErr(e.message) }
  }

  async function confirmSend() {
    setSending(true); setErr(null)
    try {
      const r = await post('/api/notifications/send', body(), asHeader())
      setSendResult(r); setPlan(null)
    } catch (e) { setErr(e.message) } finally { setSending(false) }
  }

  if (err && !opts) return <div className="loading"><div>⚠️ {err}</div></div>
  if (!opts) return <Spinner label="Loading…" />

  const teams = opts.teams || []

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Notifications</h1>
        <div className="page-desc">
          Preview the weekly AI Gateway report emails. Every user gets their own summary; managers also get a team digest —
          each with KPIs, this week’s highlights, and a prompt word cloud.
        </div>
      </div>

      <div className="scope-ribbon scope-team" style={{ borderRadius: 12, border: '1px solid var(--border)', marginBottom: 16 }}>
        <Clock size={14} />
        <span><b>Weekly schedule</b> — reports are generated every Monday for the prior 7 days. Personal reports go to each user; team reports go to their manager. Use this screen to preview exactly what recipients will see.</span>
      </div>

      {/* Admin-only: set the org Google OAuth client once, so managers can connect. */}
      {gmail && gmail.is_admin && !gmail.client_ready && (
        <div className="scope-ribbon" style={{ borderRadius: 12, border: '1px solid rgba(255,158,77,0.35)',
          background: 'rgba(255,158,77,0.08)', marginBottom: 16, display: 'block' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
            <ShieldCheck size={14} style={{ color: 'var(--sev-med)' }} />
            <b>One-time setup — connect your organization's Google account</b>
          </div>
          <div style={{ fontSize: 12.5, color: 'var(--text-dim)', marginBottom: 10 }}>
            Paste a Google <b>Web application</b> OAuth client (id + secret). Managers then click
            “Connect Gmail” to send from their own mailbox. No Databricks secret scope needed.
          </div>
          <div className="controls" style={{ gap: 8, flexWrap: 'wrap' }}>
            <input className="input" placeholder="OAuth client id" value={oauthCid} onChange={(e) => setOauthCid(e.target.value)} style={{ flex: 1, minWidth: 220 }} />
            <input className="input" type="password" placeholder="OAuth client secret" value={oauthSecret} onChange={(e) => setOauthSecret(e.target.value)} style={{ flex: 1, minWidth: 220 }} />
            <button className="pw-submit" style={{ padding: '9px 16px' }} onClick={saveOauthClient} disabled={!oauthCid || !oauthSecret}>Save</button>
          </div>
          {clientMsg && <div style={{ marginTop: 8, fontSize: 12, color: clientMsg.ok ? '#8ef0b4' : '#ffb4b4' }}>{clientMsg.ok ? '✓ ' : '⚠️ '}{clientMsg.text}</div>}
        </div>
      )}

      {gmail && (
        <div className="scope-ribbon" style={{ borderRadius: 12, border: '1px solid var(--border)', marginBottom: 16,
          background: gmail.connected && gmail.ok ? 'rgba(87,217,138,0.08)' : 'rgba(255,158,77,0.08)' }}>
          <Mail size={14} style={{ color: gmail.connected && gmail.ok ? 'var(--sev-low)' : 'var(--sev-high)' }} />
          {gmail.connected && gmail.ok ? (
            <span style={{ flex: 1 }}><b>Gmail connected</b> — your reports send from <b>{gmail.sender}</b>.</span>
          ) : gmail.connected ? (
            <span style={{ flex: 1 }}><b>Gmail error</b> — {gmail.error || 'token invalid'}. Reconnect below.</span>
          ) : !gmail.client_ready ? (
            <span style={{ flex: 1 }}><b>Email not set up yet</b> — an admin needs to connect the organization's Google account first (above).</span>
          ) : (
            <span style={{ flex: 1 }}><b>Connect your mailbox</b> to send reports from your own address. Preview works either way.</span>
          )}
          {gmail.client_ready && (gmail.connected
            ? <button className="btn-ghost" onClick={disconnectGmail}><Unlink size={14} /> Disconnect</button>
            : <button className="pw-submit" style={{ padding: '7px 14px' }} onClick={connectGmail} disabled={connecting}>
                <Link2 size={14} /> {connecting ? 'Connecting…' : 'Connect Gmail'}
              </button>)}
        </div>
      )}

      <div className="wc-grid">
        <Card title="Report" hint="pick who & when, then preview">
          <div className="wc-label"><Users2 size={13} /> Report type</div>
          <div className="chips" style={{ marginBottom: 14 }}>
            <button className={`chip ${scope === 'user' ? 'active' : ''}`} onClick={() => setScope('user')}>Individual user</button>
            <button className={`chip ${scope === 'team' ? 'active' : ''}`} onClick={() => setScope('team')}>Team digest</button>
          </div>

          {scope === 'user' && (
            <div style={{ marginBottom: 14 }}>
              <div className="wc-label"><User size={13} /> User</div>
              <select className="select" style={{ width: '100%' }} value={target} onChange={(e) => setTarget(e.target.value)}>
                {(opts.members || []).map((m) => <option key={m.email} value={m.email}>{m.name}</option>)}
              </select>
            </div>
          )}
          {scope === 'team' && (
            <div style={{ marginBottom: 14 }}>
              <div className="wc-label"><Users2 size={13} /> Team</div>
              {teams.length > 1 ? (
                <select className="select" style={{ width: '100%' }} value={teamKey} onChange={(e) => setTeamKey(e.target.value)}>
                  {teams.map((t) => <option key={t.key} value={t.key}>{t.label} ({t.count})</option>)}
                </select>
              ) : <div className="badge badge-soft">{teams[0]?.label} · {teams[0]?.count}</div>}
            </div>
          )}

          <div className="wc-label"><CalendarRange size={13} /> Date range</div>
          <div className="controls" style={{ marginBottom: 18 }}>
            <input className="input" type="date" value={from} min={opts.date_min} max={opts.date_max} onChange={(e) => setFrom(e.target.value)} />
            <span className="field-label">to</span>
            <input className="input" type="date" value={to} min={opts.date_min} max={opts.date_max} onChange={(e) => setTo(e.target.value)} />
          </div>

          <button className="pw-submit" style={{ width: '100%' }} onClick={generate} disabled={busy}>
            <Sparkles size={16} /> {busy ? 'Building preview…' : 'Generate preview'}
          </button>
          <button className="btn-ghost" style={{ width: '100%', justifyContent: 'center', marginTop: 10 }} onClick={sendTest} disabled={!preview}>
            <Send size={14} /> Send test to myself
          </button>
          <button className="pw-submit" style={{ width: '100%', marginTop: 10, background: 'linear-gradient(135deg,#FF9E4D,#FF7AA8)' }} onClick={planSend} disabled={sending}>
            <Users2 size={16} /> {scope === 'team' ? 'Send to team…' : 'Send to this user…'}
          </button>

          {plan && (
            <div className="login-sso-note" style={{ marginTop: 12, background: 'rgba(255,158,77,0.1)', borderColor: 'rgba(255,158,77,0.35)', color: '#ffca92' }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>⚠️ Send real emails to {plan.count} recipient{plan.count !== 1 ? 's' : ''}?</div>
              <div style={{ maxHeight: 130, overflowY: 'auto', fontSize: 12, color: 'var(--text-dim)', marginBottom: 10 }}>
                {plan.recipients.map((r) => <div key={r.email}>{r.name} · {r.email}</div>)}
              </div>
              <div style={{ display: 'flex', gap: 8 }}>
                <button className="pw-submit" style={{ flex: 1, padding: '9px' }} onClick={confirmSend} disabled={sending}>
                  {sending ? 'Sending…' : `Confirm & send (${plan.count})`}
                </button>
                <button className="btn-ghost" onClick={() => setPlan(null)} disabled={sending}>Cancel</button>
              </div>
            </div>
          )}
          {sendResult && <div className="login-sso-note" style={{ marginTop: 10, background: 'rgba(87,217,138,0.1)', borderColor: 'rgba(87,217,138,0.3)', color: '#8ef0b4' }}>✓ Sent {sendResult.sent} email{sendResult.sent !== 1 ? 's' : ''}{sendResult.failed ? ` · ${sendResult.failed} failed` : ''}.</div>}
          {sendMsg && <div className="login-sso-note" style={{ marginTop: 10, background: sendMsg.ok ? 'rgba(87,217,138,0.1)' : 'rgba(255,158,77,0.1)', borderColor: sendMsg.ok ? 'rgba(87,217,138,0.3)' : 'rgba(255,158,77,0.3)', color: sendMsg.ok ? '#8ef0b4' : '#ffca92' }}>{sendMsg.ok ? '✓ ' : 'ℹ️ '}{sendMsg.text}</div>}
          {err && <div className="login-sso-note" style={{ marginTop: 10, background: 'rgba(255,92,92,0.1)', borderColor: 'rgba(255,92,92,0.3)', color: '#ffb4b4' }}>⚠️ {err}</div>}
        </Card>

        <Card title="Email preview" hint={preview ? preview.subject : 'nothing generated yet'}
              right={preview ? <span className="card-hint">To: {preview.recipient}</span> : null}>
          {busy ? (
            <div className="wc-canvas"><div className="spinner" /><div className="muted" style={{ marginTop: 12 }}>Rendering the email…</div></div>
          ) : preview ? (
            <iframe title="email-preview" srcDoc={preview.html} sandbox="allow-same-origin"
                    style={{ width: '100%', height: 860, border: '1px solid var(--border)', borderRadius: 10, background: '#0A0D14' }} />
          ) : (
            <div className="wc-canvas">
              <Mail size={34} className="faint" />
              <div className="muted" style={{ marginTop: 12, fontWeight: 600 }}>No preview yet</div>
              <div className="faint" style={{ marginTop: 4, fontSize: 12.5 }}>Choose a user or team and date range, then hit <b>Generate preview</b>.</div>
            </div>
          )}
        </Card>
      </div>
    </>
  )
}
