import React, { useEffect, useMemo, useState } from 'react'
import { AlertTriangle, Flame, UserX, Boxes } from 'lucide-react'
import { Card, Kpi, SeverityBadge } from '../components/ui.jsx'
import { TrendChart, HBar, StackedHBar } from '../components/charts.jsx'
import DataTable from '../components/DataTable.jsx'
import { fetchDataset, fmtInt, fmtDate, SEV_COLORS } from '../api'

const SEV_ORDER = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'NORMAL']

export default function AnomalyDetection({ data }) {
  const catalog = data.ds_anomaly_catalog?.rows || []
  const incidents = data.ds_anomaly_incidents?.rows || []
  const [selected, setSelected] = useState('all')
  const [requests, setRequests] = useState(null)
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')

  useEffect(() => {
    fetchDataset('ds_anomaly_requests').then((rows) => {
      setRequests(rows)
      const dates = rows.map((r) => r.request_date).filter(Boolean).sort()
      if (dates.length) { setDateFrom(dates[0]); setDateTo(dates[dates.length - 1]) }
    }).catch(() => setRequests([]))
  }, [])

  const codes = useMemo(() => Array.from(new Set(incidents.map((r) => r.anomaly_code))).sort(), [incidents])
  const match = (code) => selected === 'all' || code === selected

  const incFiltered = useMemo(() => incidents.filter((r) => match(r.anomaly_code)), [incidents, selected])

  // KPIs (respect selection)
  const kUsers = new Set(incFiltered.map((r) => r.requester)).size
  const kTypes = new Set(incFiltered.map((r) => r.anomaly_code)).size
  const kCritHigh = incFiltered.filter((r) => ['CRITICAL', 'HIGH'].includes((r.severity || '').toUpperCase())).length

  // Incidents by anomaly type, stacked by severity (global)
  const byType = useMemo(() => {
    const m = {}
    for (const r of incidents) {
      const c = r.anomaly_code
      m[c] = m[c] || { anomaly_code: c, total: 0 }
      const s = (r.severity || 'NORMAL').toUpperCase()
      m[c][s] = (m[c][s] || 0) + 1
      m[c].total += 1
    }
    return Object.values(m).sort((a, b) => b.total - a.total)
  }, [incidents])

  // Flagged users for selection
  const flagged = useMemo(() => {
    const m = new Map()
    for (const r of incFiltered) m.set(r.requester, (m.get(r.requester) || 0) + 1)
    return [...m.entries()].map(([requester, incidents]) => ({ requester, incidents }))
      .sort((a, b) => b.incidents - a.incidents).slice(0, 12)
  }, [incFiltered])

  // Trend for selection (count by date)
  const trend = useMemo(() => {
    const m = new Map()
    for (const r of incFiltered) m.set(r.request_date, (m.get(r.request_date) || 0) + 1)
    return [...m.entries()].map(([request_date, incidents]) => ({ request_date, incidents }))
      .sort((a, b) => a.request_date.localeCompare(b.request_date))
  }, [incFiltered])

  // Request-level detail (selection + date range)
  const reqFiltered = useMemo(() => {
    if (!requests) return null
    return requests.filter((r) => match(r.anomaly_code) &&
      (!dateFrom || r.request_date >= dateFrom) && (!dateTo || r.request_date <= dateTo))
  }, [requests, selected, dateFrom, dateTo])

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Anomaly Detection</h1>
        <div className="page-desc">Behavioral and security anomalies detected across gateway traffic. Select an anomaly to drill in.</div>
      </div>

      {/* Selector */}
      <div className="card" style={{ marginBottom: 16, padding: '14px 18px' }}>
        <div className="controls">
          <span className="field-label">Selected anomaly</span>
          <div className="chips">
            <button className={`chip ${selected === 'all' ? 'active' : ''}`} onClick={() => setSelected('all')}>All types</button>
            {codes.map((c) => (
              <button key={c} className={`chip ${selected === c ? 'active' : ''}`} onClick={() => setSelected(c)}>{c}</button>
            ))}
          </div>
        </div>
      </div>

      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <Kpi label="Total Incidents" value={fmtInt(incFiltered.length)} icon={AlertTriangle} accent="var(--sev-high)" foot={selected === 'all' ? 'all anomaly types' : selected} />
        <Kpi label="Critical / High" value={fmtInt(kCritHigh)} icon={Flame} accent="var(--sev-critical)" foot="severe incidents" />
        <Kpi label="Users Flagged" value={fmtInt(kUsers)} icon={UserX} accent="var(--c3)" foot="distinct requesters" />
        <Kpi label="Anomaly Types" value={fmtInt(kTypes)} icon={Boxes} accent="var(--c2)" foot="distinct detectors" />
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Incidents by Anomaly Type" hint="stacked by severity (all types)">
          <StackedHBar data={byType} category="anomaly_code" height={300}
            keys={SEV_ORDER.map((s) => ({ key: s, name: s, color: SEV_COLORS[s] }))} />
        </Card>
        <Card title="Trend for Selected Anomaly" hint={selected === 'all' ? 'daily incidents, all types' : `daily incidents · ${selected}`}>
          <TrendChart data={trend} x="request_date" area height={300}
            series={[{ key: 'incidents', name: 'Incidents', color: '#FF9E4D' }]}
            tickFmt={fmtDate} valueFmt={fmtInt} />
        </Card>
      </div>

      <div className="grid cols-3" style={{ marginBottom: 16 }}>
        <Card title="Flagged Users" hint="top users for current selection" span="1">
          <HBar data={flagged} category="requester" value="incidents" valueFmt={fmtInt} height={320} single="#A98CFF" colorByIndex={false} />
        </Card>
        <Card title="Anomaly Definition & Context" hint="detector catalog" span="2">
          <DataTable rows={catalog} maxHeight={320} initialSort={{ key: 'incidents', dir: 'desc' }}
            cols={[
              { key: 'anomaly_type', label: 'Anomaly', render: (r) => <b>{r.anomaly_type}</b> },
              { key: 'what_it_detects', label: 'What it detects', clip: true },
              { key: 'detection_method', label: 'Detection method', clip: true },
              { key: 'incidents', label: 'Incidents', num: true },
              { key: 'users_affected', label: 'Users', num: true },
            ]} />
        </Card>
      </div>

      <Card title="User-Level Incident Detail" hint={`${incFiltered.length} incidents`} className="span-6" >
        <DataTable rows={incFiltered} maxHeight={380} cap={500} initialSort={{ key: 'request_date', dir: 'desc' }}
          cols={[
            { key: 'anomaly_code', label: 'Code', mono: true },
            { key: 'request_date', label: 'Date', render: (r) => String(r.request_date).slice(0, 10) },
            { key: 'requester', label: 'User' },
            { key: 'team', label: 'Team', render: (r) => <span className="badge badge-soft">{r.team}</span> },
            { key: 'severity', label: 'Severity', render: (r) => <SeverityBadge value={r.severity} /> },
            { key: 'metric_summary', label: 'Summary', clip: true },
            { key: 'explanation', label: 'Explanation', clip: true },
          ]} />
      </Card>

      <Card title="Actionable Insights &amp; Recommendations" hint="what to do about each anomaly type" style={{ marginTop: 16 }}>
        <DataTable rows={data.ds_anomaly_insights?.rows || []} maxHeight={340} initialSort={{ key: 'total_incidents', dir: 'desc' }}
          cols={[
            { key: 'anomaly_category', label: 'Issue Type', render: (r) => <b>{r.anomaly_category}</b> },
            { key: 'users_affected', label: 'Users Affected', num: true },
            { key: 'total_incidents', label: 'Incidents', num: true },
            { key: 'critical_high_count', label: 'Critical/High', num: true, render: (r) => (r.critical_high_count > 0 ? <span className="badge sev-HIGH">{r.critical_high_count}</span> : '—') },
            { key: 'recommendation', label: 'AI-Driven Recommendation', clip: true },
          ]} />
      </Card>

      <Card
        style={{ marginTop: 16 }}
        title="Request-Level Detail"
        hint={reqFiltered ? `${reqFiltered.length.toLocaleString()} requests` : 'loading…'}
        right={
          <div className="controls">
            <span className="field-label">From</span>
            <input className="input" type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
            <span className="field-label">To</span>
            <input className="input" type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
          </div>
        }
      >
        {!reqFiltered ? <div className="empty">Loading request-level detail…</div> : (
          <DataTable rows={reqFiltered} maxHeight={480} cap={600} initialSort={{ key: 'event_time', dir: 'desc' }}
            cols={[
              { key: 'anomaly_code', label: 'Code', mono: true },
              { key: 'event_time', label: 'Time', mono: true, render: (r) => String(r.event_time || '').replace('T', ' ').slice(0, 19) },
              { key: 'requester', label: 'User' },
              { key: 'severity', label: 'Severity', render: (r) => <SeverityBadge value={r.severity} /> },
              { key: 'destination_model', label: 'Model', mono: true },
              { key: 'total_tokens', label: 'Tokens', num: true, render: (r) => fmtInt(r.total_tokens) },
              { key: 'latency_ms', label: 'ms', num: true, render: (r) => fmtInt(r.latency_ms) },
              { key: 'user_prompt', label: 'Prompt', clip: true },
              { key: 'anomaly_context', label: 'Context', clip: true },
            ]} />
        )}
      </Card>
    </>
  )
}
