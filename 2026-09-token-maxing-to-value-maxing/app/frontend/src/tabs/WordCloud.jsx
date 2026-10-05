import React, { useEffect, useMemo, useState } from 'react'
import { Cloud, Sparkles, User, Users2, CalendarRange, Cpu, ImageOff } from 'lucide-react'
import { Card } from '../components/ui.jsx'
import { wordcloudOptions, wordcloudLast, wordcloudGenerate } from '../api'

export default function WordCloud({ persona }) {
  const [opts, setOpts] = useState(null)
  const [current, setCurrent] = useState(null)     // {image, meta...} or {empty} or null
  const [loading, setLoading] = useState(true)     // initial load
  const [generating, setGenerating] = useState(false)
  const [err, setErr] = useState(null)

  const isMgr = persona.is_manager
  const isAdmin = persona.is_admin

  // filter state
  const [scope, setScope] = useState(isMgr ? 'team' : 'self')
  const [targetEmail, setTargetEmail] = useState('')
  const [teamKey, setTeamKey] = useState('')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [models, setModels] = useState(new Set())

  useEffect(() => {
    let alive = true
    Promise.all([wordcloudOptions(), wordcloudLast()])
      .then(([o, last]) => {
        if (!alive) return
        setOpts(o)
        setFrom(o.date_min || ''); setTo(o.date_max || '')
        setModels(new Set(o.models))
        if (o.members?.length) setTargetEmail(o.members[0].email)
        if (o.teams?.length) setTeamKey(o.teams[0].key)
        if (last?.exists) setCurrent(last)
      })
      .catch((e) => alive && setErr(e.message))
      .finally(() => alive && setLoading(false))
    return () => { alive = false }
  }, [])

  const toggleModel = (m) => setModels((s) => {
    const n = new Set(s); n.has(m) ? n.delete(m) : n.add(m); return n
  })

  async function generate() {
    setGenerating(true); setErr(null)
    try {
      const body = {
        scope, target_email: targetEmail, team_key: teamKey,
        date_from: from, date_to: to, models: [...models],
      }
      const res = await wordcloudGenerate(body)
      setCurrent(res)
    } catch (e) { setErr(e.message) } finally { setGenerating(false) }
  }

  if (loading) return <div className="empty">Loading word cloud…</div>
  if (err && !opts) return <div className="loading"><div>⚠️ {err}</div></div>

  const teams = opts.teams || []

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Prompt Word Cloud</h1>
        <div className="page-desc">
          The most frequent terms across {isMgr ? 'your team’s' : 'your'} AI Gateway prompts.
          Choose a period and models, then generate. Your last cloud is saved and shown by default.
        </div>
      </div>

      <div className="wc-grid">
        {/* ---- Filters ---- */}
        <Card title="Filters" hint="tune the corpus, then generate">
          {isMgr && (
            <>
              <div className="wc-label"><Users2 size={13} /> Scope</div>
              <div className="chips" style={{ marginBottom: 14 }}>
                <button className={`chip ${scope === 'team' ? 'active' : ''}`} onClick={() => setScope('team')}>Team</button>
                <button className={`chip ${scope === 'user' ? 'active' : ''}`} onClick={() => setScope('user')}>Individual user</button>
                {!isAdmin && (
                  <button className={`chip ${scope === 'self' ? 'active' : ''}`} onClick={() => setScope('self')}>Just me</button>
                )}
              </div>
              {scope === 'team' && (
                <div style={{ marginBottom: 14 }}>
                  <div className="wc-label"><Users2 size={13} /> Which team</div>
                  {teams.length > 1 ? (
                    <select className="select" style={{ width: '100%' }} value={teamKey} onChange={(e) => setTeamKey(e.target.value)}>
                      {teams.map((t) => <option key={t.key} value={t.key}>{t.label} ({t.count})</option>)}
                    </select>
                  ) : (
                    <div className="badge badge-soft">{teams[0]?.label || persona.team}{teams[0] ? ` · ${teams[0].count}` : ''}</div>
                  )}
                </div>
              )}
              {scope === 'user' && (
                <div style={{ marginBottom: 14 }}>
                  <div className="wc-label"><User size={13} /> User</div>
                  <select className="select" style={{ width: '100%' }} value={targetEmail} onChange={(e) => setTargetEmail(e.target.value)}>
                    {(opts.members || []).map((m) => <option key={m.email} value={m.email}>{m.name}</option>)}
                  </select>
                </div>
              )}
            </>
          )}

          <div className="wc-label"><CalendarRange size={13} /> Time period</div>
          <div className="controls" style={{ marginBottom: 14 }}>
            <input className="input" type="date" value={from} min={opts.date_min} max={opts.date_max} onChange={(e) => setFrom(e.target.value)} />
            <span className="field-label">to</span>
            <input className="input" type="date" value={to} min={opts.date_min} max={opts.date_max} onChange={(e) => setTo(e.target.value)} />
          </div>

          <div className="wc-label"><Cpu size={13} /> Models</div>
          <div className="chips" style={{ marginBottom: 18 }}>
            {(opts.models || []).map((m) => (
              <button key={m} className={`chip ${models.has(m) ? 'active' : ''}`} onClick={() => toggleModel(m)}>{m}</button>
            ))}
          </div>

          <button className="pw-submit" style={{ width: '100%' }} onClick={generate} disabled={generating || (scope === 'user' && !targetEmail)}>
            <Sparkles size={16} /> {generating ? 'Generating…' : 'Generate word cloud'}
          </button>
          {err && <div className="login-sso-note" style={{ marginTop: 12, background: 'rgba(255,92,92,0.1)', borderColor: 'rgba(255,92,92,0.3)', color: '#ffb4b4' }}>⚠️ {err}</div>}
        </Card>

        {/* ---- Display ---- */}
        <Card
          title="Word cloud"
          hint={current && current.image ? `${current.target_label} · ${current.prompt_count} prompts · ${current.models} · ${current.date_from} → ${current.date_to}` : 'nothing generated yet'}
          right={current?.created_at ? <span className="card-hint">generated {String(current.created_at).replace('T', ' ').replace('Z', ' UTC')}</span> : null}
        >
          {generating ? (
            <div className="wc-canvas"><div className="spinner" /><div className="muted" style={{ marginTop: 12 }}>Analysing prompts &amp; rendering…</div></div>
          ) : current && current.image ? (
            <div className="wc-canvas">
              <img className="wc-img" src={`data:image/png;base64,${current.image}`} alt="Prompt word cloud" />
            </div>
          ) : current && current.empty ? (
            <div className="wc-canvas"><ImageOff size={30} className="faint" /><div className="muted" style={{ marginTop: 10 }}>No prompts match these filters.</div></div>
          ) : (
            <div className="wc-canvas">
              <Cloud size={34} className="faint" />
              <div className="muted" style={{ marginTop: 12, fontWeight: 600 }}>No word cloud generated</div>
              <div className="faint" style={{ marginTop: 4, fontSize: 12.5 }}>Pick your filters on the left and hit <b>Generate word cloud</b>.</div>
            </div>
          )}
        </Card>
      </div>
    </>
  )
}
