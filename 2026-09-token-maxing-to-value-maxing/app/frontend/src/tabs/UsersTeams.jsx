import React, { useEffect, useMemo, useState } from 'react'
import { Card } from '../components/ui.jsx'
import { HBar } from '../components/charts.jsx'
import DataTable from '../components/DataTable.jsx'
import { fetchDataset, fmtInt, fmtUsd, fmtCompact, fmtNum } from '../api'

const N = (v) => (v == null ? 0 : +v)

export default function UsersTeams({ data }) {
  const L = data.ui_labels || { expensive: 'Premium', cheap: 'Standard' }
  const [detail, setDetail] = useState(null)
  const [useCaseFilter, setUseCaseFilter] = useState('all')

  useEffect(() => {
    fetchDataset('ds_usecase_detail').then(setDetail).catch(() => setDetail([]))
  }, [])

  const useCases = useMemo(
    () => (data.ds_usecase?.rows || []).map((r) => ({ ...r, requests: N(r.requests) })).sort((a, b) => b.requests - a.requests),
    [data]
  )
  const users = data.ds_users?.rows || []

  const detailFiltered = useMemo(() => {
    if (!detail) return null
    if (useCaseFilter === 'all') return detail
    return detail.filter((r) => (r.classified_use_case || '').toLowerCase() === useCaseFilter)
  }, [detail, useCaseFilter])

  const ucOptions = useMemo(
    () => ['all', ...Array.from(new Set((detail || []).map((r) => (r.classified_use_case || '').toLowerCase()).filter(Boolean))).sort()],
    [detail]
  )

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Users &amp; Teams</h1>
        <div className="page-desc">Who is using AI, for what, and at what cost — by use case, user, and team.</div>
      </div>

      <div className="grid cols-3" style={{ marginBottom: 16 }}>
        <Card title="Requests by Use Case" hint="classified intent of gateway traffic" span="1">
          <HBar data={useCases} category="use_case_category" value="requests" valueFmt={fmtCompact} height={320} />
        </Card>
        <Card title="User Consumption" hint="per-user activity, tokens & spend" span="2" bodyClass="">
          <DataTable
            rows={users}
            maxHeight={340}
            initialSort={{ key: 'total_cost_usd', dir: 'desc' }}
            cols={[
              { key: 'requester', label: 'User', render: (r) => <b>{r.requester}</b> },
              { key: 'team', label: 'Team', render: (r) => <span className="badge badge-soft">{r.team}</span> },
              { key: 'role', label: 'Role', mono: true },
              { key: 'total_requests', label: 'Requests', num: true, render: (r) => fmtInt(r.total_requests) },
              { key: 'total_tokens', label: 'Tokens', num: true, render: (r) => fmtCompact(r.total_tokens) },
              { key: 'total_cost_usd', label: 'Cost', num: true, render: (r) => fmtUsd(r.total_cost_usd) },
              { key: 'avg_latency_ms', label: 'Avg ms', num: true, render: (r) => fmtInt(r.avg_latency_ms) },
              { key: 'active_days', label: 'Days', num: true },
            ]}
          />
        </Card>
      </div>

      <Card title="AI-Driven Team Insights" hint="cost, savings & risk per team with recommended actions" style={{ marginBottom: 16 }}>
        <DataTable
          rows={data.ds_team_insights?.rows || []}
          maxHeight={340}
          initialSort={{ key: 'total_cost_usd', dir: 'desc' }}
          cols={[
            { key: 'team', label: 'Team', render: (r) => <span className="badge badge-soft">{r.team}</span> },
            { key: 'active_users', label: 'Users', num: true },
            { key: 'total_cost_usd', label: 'Total Cost', num: true, render: (r) => fmtUsd(r.total_cost_usd) },
            { key: 'potential_savings_usd', label: 'Potential Savings', num: true, render: (r) => fmtUsd(r.potential_savings_usd) },
            { key: 'sonnet_pct', label: `${L.expensive} %`, num: true, render: (r) => fmtNum(r.sonnet_pct, 1) },
            { key: 'cost_per_user', label: '$/User', num: true, render: (r) => fmtUsd(r.cost_per_user) },
            { key: 'anomaly_count', label: 'Anomalies', num: true },
            { key: 'ai_recommendation', label: 'AI-Driven Recommendation', clip: true },
          ]}
        />
      </Card>

      <Card
        title="Request Detail by Use Case"
        hint={detail ? `${(detailFiltered || []).length.toLocaleString()} requests` : 'loading…'}
        right={
          <div className="controls">
            <span className="field-label">Use case</span>
            <select className="select" value={useCaseFilter} onChange={(e) => setUseCaseFilter(e.target.value)}>
              {ucOptions.map((u) => <option key={u} value={u}>{u === 'all' ? 'All use cases' : u}</option>)}
            </select>
          </div>
        }
      >
        {!detail ? (
          <div className="empty">Loading request-level detail…</div>
        ) : (
          <DataTable
            rows={detailFiltered}
            maxHeight={520}
            cap={800}
            initialSort={{ key: 'request_time', dir: 'desc' }}
            cols={[
              { key: 'request_time', label: 'Time', mono: true, render: (r) => String(r.request_time || '').replace('T', ' ').slice(0, 19) },
              { key: 'requester', label: 'User' },
              { key: 'classified_use_case', label: 'Use case', render: (r) => <span className="badge badge-soft">{r.classified_use_case}</span> },
              { key: 'model_requested', label: 'Model', mono: true },
              { key: 'prompt_preview', label: 'Prompt', clip: true },
              { key: 'input_tokens', label: 'In', num: true, render: (r) => fmtInt(r.input_tokens) },
              { key: 'output_tokens', label: 'Out', num: true, render: (r) => fmtInt(r.output_tokens) },
              { key: 'estimated_cost_usd', label: 'Cost', num: true, render: (r) => fmtUsd(r.estimated_cost_usd) },
              { key: 'team', label: 'Team' },
            ]}
          />
        )}
      </Card>
    </>
  )
}
