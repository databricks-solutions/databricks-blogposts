import React from 'react'
import { Activity, Coins, Cpu, Users, CheckCircle2, AlertTriangle } from 'lucide-react'
import { Card, Kpi } from '../components/ui.jsx'
import { TrendChart, Donut } from '../components/charts.jsx'
import { fmtCompact, fmtInt, fmtUsd, fmtNum, fmtDate, PALETTE } from '../api'

const N = (v) => (v == null ? 0 : +v)

export default function ExecutiveOverview({ data }) {
  const kpi = (data.ds_kpi?.rows || [{}])[0]
  const daily = (data.ds_daily?.rows || []).map((r) => ({
    request_date: r.request_date,
    total_requests: N(r.total_requests),
    total_cost_usd: N(r.total_cost_usd),
  }))
  const models = (data.ds_model_split?.rows || []).map((r) => ({ ...r, total_tokens: N(r.total_tokens) }))
  const depts = (data.ds_cost_attr?.rows || []).map((r) => ({ ...r, cost_usd: N(r.cost_usd) }))

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Executive Overview</h1>
        <div className="page-desc">Platform-wide AI Gateway activity, spend, and governance posture over the last 30 days.</div>
      </div>

      <div className="grid cols-6" style={{ marginBottom: 16 }}>
        <Kpi label="Total Requests" value={fmtCompact(kpi.total_requests)} foot="last 30 days" icon={Activity} accent="var(--c1)" />
        <Kpi label="Total Tokens" value={fmtCompact(kpi.total_tokens)} foot="input + output" icon={Cpu} accent="var(--c2)" />
        <Kpi label="Total Cost" value={fmtUsd(kpi.total_cost_usd, true)} foot="attributed spend" icon={Coins} accent="var(--c4)" />
        <Kpi label="Active Users" value={fmtInt(kpi.active_users)} foot="unique requesters" icon={Users} accent="var(--c3)" />
        <Kpi label="Success Rate" value={fmtNum(kpi.success_rate_pct, 1)} unit="%" foot="HTTP 200 responses" icon={CheckCircle2} accent="var(--c6)" />
        <Kpi label="Critical Anomalies" value={fmtInt(kpi.critical_anomalies)} foot="critical + high" icon={AlertTriangle} accent="var(--sev-critical)" />
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Daily Request Volume" hint="requests routed through the gateway">
          <TrendChart data={daily} x="request_date" area
            series={[{ key: 'total_requests', name: 'Requests', color: '#35D6BE' }]}
            tickFmt={fmtDate} valueFmt={fmtCompact} />
        </Card>
        <Card title="Daily Cost (USD)" hint="model-serving spend per day">
          <TrendChart data={daily} x="request_date" area
            series={[{ key: 'total_cost_usd', name: 'Cost', color: '#4C8DFF' }]}
            tickFmt={fmtDate} valueFmt={(v) => fmtUsd(v, true)} />
        </Card>
      </div>

      <div className="grid cols-2">
        <Card title="Tokens by Model" hint="destination model share of token volume">
          <Donut data={models} nameKey="destination_model" valueKey="total_tokens" fmt={fmtCompact} />
        </Card>
        <Card title="Cost by Department" hint="attributed spend across business units">
          <Donut data={depts} nameKey="department" valueKey="cost_usd" colors={PALETTE} fmt={(v) => fmtUsd(v, true)} />
        </Card>
      </div>
    </>
  )
}
