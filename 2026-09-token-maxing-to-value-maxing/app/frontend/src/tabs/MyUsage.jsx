import React, { useEffect, useState } from 'react'
import { Coins, Activity, Cpu, Gauge, User, CalendarRange } from 'lucide-react'
import { Card, Kpi, Spinner } from '../components/ui.jsx'
import { TrendChart, HBar } from '../components/charts.jsx'
import DataTable from '../components/DataTable.jsx'
import { myusageOptions, fetchMyUsage, fmtInt, fmtCompact, fmtUsd, fmtUsdAxis, fmtNum, fmtDate } from '../api'

export default function MyUsage({ persona }) {
  const [opts, setOpts] = useState(null)
  const [usage, setUsage] = useState(null)
  const [target, setTarget] = useState('')
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState(null)

  useEffect(() => {
    let alive = true
    myusageOptions().then((o) => {
      if (!alive) return
      setOpts(o); setTarget(o.self_email); setFrom(o.date_min || ''); setTo(o.date_max || '')
    }).catch((e) => alive && setErr(e.message))
    return () => { alive = false }
  }, [])

  useEffect(() => {
    if (!opts || !target) return
    let alive = true
    setLoading(true)
    fetchMyUsage({ user_email: target, date_from: from, date_to: to })
      .then((u) => alive && setUsage(u)).catch((e) => alive && setErr(e.message))
      .finally(() => alive && setLoading(false))
    return () => { alive = false }
  }, [opts, target, from, to])

  if (err) return <div className="loading"><div>⚠️ {err}</div></div>
  if (!opts) return <Spinner label="Loading…" />

  const k = usage?.kpis || {}
  const isMgr = opts.is_manager
  const daily = usage?.daily || []

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">My Usage</h1>
        <div className="page-desc">
          {isMgr ? 'Per-user' : 'Your personal'} AI Gateway activity — spend, tokens, rate-limit status and model mix.
        </div>
      </div>

      {/* Controls */}
      <div className="card" style={{ marginBottom: 16, padding: '14px 18px' }}>
        <div className="controls">
          {isMgr && (
            <>
              <span className="field-label"><User size={12} style={{ verticalAlign: '-1px' }} /> User</span>
              <select className="select" value={target} onChange={(e) => setTarget(e.target.value)}>
                <option value={opts.self_email}>{opts.self_name} (me)</option>
                {(opts.members || []).filter((m) => m.email !== opts.self_email)
                  .map((m) => <option key={m.email} value={m.email}>{m.name}</option>)}
              </select>
            </>
          )}
          <span className="field-label"><CalendarRange size={12} style={{ verticalAlign: '-1px' }} /> From</span>
          <input className="input" type="date" value={from} min={opts.date_min} max={opts.date_max} onChange={(e) => setFrom(e.target.value)} />
          <span className="field-label">To</span>
          <input className="input" type="date" value={to} min={opts.date_min} max={opts.date_max} onChange={(e) => setTo(e.target.value)} />
          {usage && <span className="pill" style={{ marginLeft: 'auto' }}>{usage.target_name}</span>}
        </div>
      </div>

      {loading && !usage ? <Spinner label="Loading usage…" /> : (
        <>
          <div className="grid cols-4" style={{ marginBottom: 16 }}>
            <Kpi label="My Spend" value={fmtUsd(k.my_spend, true)} icon={Coins} accent="var(--c4)" foot="attributed cost" />
            <Kpi label="My Requests" value={fmtInt(k.my_requests)} icon={Activity} accent="var(--c1)" foot="gateway calls" />
            <Kpi label="My Tokens" value={fmtCompact(k.my_tokens)} icon={Cpu} accent="var(--c2)" foot="input + output" />
            <Kpi label="Rate Limit Hits" value={fmtInt(k.my_rate_limit_hits)} icon={Gauge} accent="var(--sev-high)" foot="quota / budget" />
          </div>

          <div className="grid cols-2" style={{ marginBottom: 16 }}>
            <Card title="My Daily Usage Trend" hint="attributed spend per day">
              <TrendChart data={daily} x="request_date" area height={300}
                series={[{ key: 'daily_cost_usd', name: 'Cost', color: '#35D6BE' }]}
                tickFmt={fmtDate} valueFmt={fmtUsdAxis} />
            </Card>
            <Card title="Tokens by Model" hint="token volume by destination model">
              <HBar data={usage?.tokens_by_model || []} category="destination_model" value="total_tokens"
                valueFmt={fmtCompact} height={300} single="#4C8DFF" colorByIndex={false} />
            </Card>
          </div>

          <Card title="Rate Limit Status" hint="enforcement hits by limit type">
            <DataTable rows={usage?.rate_limits || []} maxHeight={280} initialSort={{ key: 'total_rate_limit_hits', dir: 'desc' }}
              cols={[
                { key: 'rate_limit_type', label: 'Limit Type', render: (r) => <span className="badge badge-soft">{r.rate_limit_type}</span> },
                { key: 'total_rate_limit_hits', label: 'Hits', num: true, render: (r) => fmtInt(r.total_rate_limit_hits) },
                { key: 'avg_block_rate_pct', label: 'Avg Block %', num: true, render: (r) => fmtNum(r.avg_block_rate_pct, 1) },
                { key: 'max_tokens_day', label: 'Peak Tokens/day', num: true, render: (r) => fmtCompact(r.max_tokens_day) },
              ]} />
          </Card>
        </>
      )}
    </>
  )
}
