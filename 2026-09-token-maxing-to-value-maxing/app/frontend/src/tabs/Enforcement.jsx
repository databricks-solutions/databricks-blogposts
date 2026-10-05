import React, { useMemo } from 'react'
import { ShieldX, Gauge, UserX, Percent } from 'lucide-react'
import { Card, Kpi } from '../components/ui.jsx'
import { TrendChart, HBar } from '../components/charts.jsx'
import DataTable from '../components/DataTable.jsx'
import { fmtInt, fmtNum, colorFor } from '../api'

const N = (v) => (v == null ? 0 : +v)

export default function Enforcement({ data }) {
  const ek = (data.ds_enforcement_kpis?.rows || [{}])[0]
  const rk = (data.ds_rl_kpis?.rows || [{}])[0]
  const gDaily = data.ds_guardrail_daily?.rows || []
  const gUsers = data.ds_guardrail_users?.rows || []
  const rlUsers = data.ds_rl_users?.rows || []

  // pivot guardrail_daily → date rows with a column per guardrail_type
  const gTypes = useMemo(() => Array.from(new Set(gDaily.map((r) => r.guardrail_type))).sort(), [gDaily])
  const gTrend = useMemo(() => {
    const m = new Map()
    for (const r of gDaily) {
      const d = m.get(r.request_date) || { request_date: r.request_date }
      d[r.guardrail_type] = (d[r.guardrail_type] || 0) + N(r.blocked_count)
      m.set(r.request_date, d)
    }
    return [...m.values()].sort((a, b) => a.request_date.localeCompare(b.request_date))
  }, [gDaily])

  const blocksByType = useMemo(() => {
    const m = new Map()
    for (const r of gUsers) m.set(r.guardrail_type, (m.get(r.guardrail_type) || 0) + N(r.total_blocks))
    return [...m.entries()].map(([guardrail_type, total_blocks]) => ({ guardrail_type, total_blocks })).sort((a, b) => b.total_blocks - a.total_blocks)
  }, [gUsers])

  const hitsByUser = useMemo(() => {
    const m = new Map()
    for (const r of rlUsers) m.set(r.requester, (m.get(r.requester) || 0) + N(r.total_hits))
    return [...m.entries()].map(([requester, total_hits]) => ({ requester, total_hits })).sort((a, b) => b.total_hits - a.total_hits).slice(0, 12)
  }, [rlUsers])

  const hitsByType = useMemo(() => {
    const m = new Map()
    for (const r of rlUsers) m.set(r.rate_limit_type, (m.get(r.rate_limit_type) || 0) + N(r.total_hits))
    return [...m.entries()].map(([rate_limit_type, total_hits]) => ({ rate_limit_type, total_hits })).sort((a, b) => b.total_hits - a.total_hits)
  }, [rlUsers])

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">AI Gateway Enforcement</h1>
        <div className="page-desc">Guardrail blocks, rate limiting and access controls enforced at the gateway.</div>
      </div>

      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <Kpi label="Guardrail Blocks" value={fmtInt(ek.total_guardrail_blocks)} icon={ShieldX} accent="var(--sev-critical)" foot="prompts blocked" />
        <Kpi label="Rate Limit Hits" value={fmtInt(rk.total_rate_limit_hits)} icon={Gauge} accent="var(--sev-high)" foot="quota / budget hits" />
        <Kpi label="Users Blocked" value={fmtInt(ek.users_with_blocks)} icon={UserX} accent="var(--c3)" foot="with ≥1 block" />
        <Kpi label="Avg Block Rate" value={fmtNum(rk.avg_block_rate_pct, 1)} unit="%" icon={Percent} accent="var(--c4)" foot="of rate-limited traffic" />
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Guardrail Blocks — Daily Trend" hint="blocked prompts per day, by guardrail">
          <TrendChart data={gTrend} x="request_date" height={300}
            series={gTypes.map((t, i) => ({ key: t, name: t, color: colorFor(i) }))}
            valueFmt={fmtInt} />
        </Card>
        <Card title="Blocks by Guardrail Type" hint="total blocks per guardrail">
          <HBar data={blocksByType} category="guardrail_type" value="total_blocks" valueFmt={fmtInt} height={300} />
        </Card>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Rate Limit Hits by User" hint="top users hitting limits">
          <HBar data={hitsByUser} category="requester" value="total_hits" valueFmt={fmtInt} height={300} single="#FF9E4D" colorByIndex={false} />
        </Card>
        <Card title="Hits by Rate Limit Type" hint="quota vs budget enforcement">
          <HBar data={hitsByType} category="rate_limit_type" value="total_hits" valueFmt={fmtInt} height={300} />
        </Card>
      </div>

      <Card title="Blocked Prompts Detail" hint={`${(data.ds_blocked_prompts?.rows || []).length} blocked prompts`} style={{ marginBottom: 16 }}>
        <DataTable rows={data.ds_blocked_prompts?.rows || []} maxHeight={400} cap={400} initialSort={{ key: 'request_date', dir: 'desc' }}
          cols={[
            { key: 'request_date', label: 'Date', render: (r) => String(r.request_date).slice(0, 10) },
            { key: 'requester', label: 'User' },
            { key: 'guardrail_type', label: 'Type', render: (r) => <span className="badge badge-soft">{r.guardrail_type}</span> },
            { key: 'block_reason', label: 'Why It Was Blocked', clip: true },
            { key: 'original_prompt', label: 'Prompt Content', clip: true },
          ]} />
      </Card>

      <div className="grid cols-2">
        <Card title="UC Access Denied Events" hint="Unity Catalog 401/403 — last 30d">
          <DataTable rows={data.ds_uc_access_denied?.rows || []} maxHeight={360} cap={200} initialSort={{ key: 'event_time', dir: 'desc' }}
            cols={[
              { key: 'event_time', label: 'Time', mono: true },
              { key: 'user_email', label: 'User' },
              { key: 'action_name', label: 'Action', mono: true },
              { key: 'resource_name', label: 'Resource', clip: true, mono: true },
              { key: 'securable_type', label: 'Type', render: (r) => <span className="badge badge-soft">{r.securable_type}</span> },
              { key: 'error_message', label: 'Error', clip: true },
            ]} />
        </Card>
        <Card title="Enforcement Type Definitions" hint="guardrail & rate-limit catalog">
          <DataTable rows={data.ds_guardrail_legend?.rows || []} maxHeight={360} initialSort={{ key: 'priority', dir: 'asc' }}
            cols={[
              { key: 'enforcement_type', label: 'Type', render: (r) => <b>{r.enforcement_type}</b> },
              { key: 'category', label: 'Category', render: (r) => <span className="badge badge-soft">{r.category}</span> },
              { key: 'what_it_does', label: 'What it does', clip: true },
              { key: 'example_trigger', label: 'Example trigger', clip: true },
            ]} />
        </Card>
      </div>
    </>
  )
}
