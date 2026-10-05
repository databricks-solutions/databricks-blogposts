import React, { useMemo } from 'react'
import { Users, Code2, Bug, Coins, GitCommitHorizontal, GitMerge, Ticket, Clock } from 'lucide-react'
import { Card, Kpi } from '../components/ui.jsx'
import { TrendChart, HBar, VBar, Donut } from '../components/charts.jsx'
import DataTable from '../components/DataTable.jsx'
import { fmtInt, fmtUsd, fmtNum, fmtCompact, fmtDate } from '../api'

const N = (v) => (v == null ? 0 : +v)

export default function DeveloperProductivity({ data }) {
  const L = data.ui_labels || { expensive: 'Premium', cheap: 'Standard' }
  const dev = data.ds_dev_prod?.rows || []
  const mix = (data.ds_dev_model_mix?.rows || []).map((r) => ({ ...r, requests: N(r.requests) }))
  const impact = (data.ds_productivity_impact?.rows || [])
  const pk = Object.fromEntries((data.ds_productivity_kpis?.rows || []).map((r) => [r.metric, r.value]))

  const activeDevs = new Set(dev.map((r) => r.requester)).size
  const coding = dev.reduce((s, r) => s + N(r.coding_requests), 0)
  const debugging = dev.reduce((s, r) => s + N(r.debugging_requests), 0)
  const spend = dev.reduce((s, r) => s + N(r.total_cost_usd), 0)

  // Daily activity per developer (top 8 by total requests)
  const dailyPerDev = useMemo(() => {
    const totals = new Map()
    for (const r of dev) totals.set(r.requester, (totals.get(r.requester) || 0) + N(r.total_requests))
    const top = [...totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8).map((e) => e[0])
    const byDate = new Map()
    for (const r of dev) {
      if (!top.includes(r.requester)) continue
      const d = byDate.get(r.request_date) || { request_date: r.request_date }
      d[r.requester] = (d[r.requester] || 0) + N(r.total_requests)
      byDate.set(r.request_date, d)
    }
    return { rows: [...byDate.values()].sort((a, b) => a.request_date.localeCompare(b.request_date)), devs: top }
  }, [dev])

  const costPerDev = useMemo(() => {
    const m = new Map()
    for (const r of dev) m.set(r.requester, (m.get(r.requester) || 0) + N(r.total_cost_usd))
    return [...m.entries()].map(([requester, total_cost_usd]) => ({ requester, total_cost_usd })).sort((a, b) => b.total_cost_usd - a.total_cost_usd).slice(0, 12)
  }, [dev])

  const tokensPerDev = useMemo(() => {
    const m = new Map()
    for (const r of dev) { const e = m.get(r.requester) || { s: 0, n: 0 }; e.s += N(r.avg_tokens_per_req); e.n += 1; m.set(r.requester, e) }
    return [...m.entries()].map(([requester, e]) => ({ requester, avg_tokens: e.n ? e.s / e.n : 0 })).sort((a, b) => b.avg_tokens - a.avg_tokens).slice(0, 12)
  }, [dev])

  const eng = useMemo(() => impact.filter((r) => r.team_type === 'engineering').map((r) => ({
    week_start: r.week_start, ai_sessions_per_user: N(r.ai_sessions_per_user), commits_per_dev_week: N(r.commits_per_dev_week), avg_merge_days: N(r.avg_merge_days),
  })).sort((a, b) => a.week_start.localeCompare(b.week_start)), [impact])

  const prod = useMemo(() => impact.filter((r) => r.team_type === 'product').map((r) => ({
    week_start: r.week_start, ba_tickets_created: N(r.ba_tickets_created), ba_tickets_deflected_by_ai: N(r.ba_tickets_deflected_by_ai),
  })).sort((a, b) => a.week_start.localeCompare(b.week_start)), [impact])

  const adoption = useMemo(() => {
    const m = new Map()
    for (const r of impact) { const d = m.get(r.week_start) || { week_start: r.week_start }; d[r.team_type] = N(r.ai_adoption_pct); m.set(r.week_start, d) }
    return [...m.values()].sort((a, b) => a.week_start.localeCompare(b.week_start))
  }, [impact])

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Developer Productivity</h1>
        <div className="page-desc">How engineering and product teams use AI — and the measured impact on delivery.</div>
      </div>

      {/* Business impact headline KPIs */}
      <div className="section-label">Business impact</div>
      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <Kpi label="Commit Velocity Lift" value={pk.commit_lift || '—'} icon={GitCommitHorizontal} accent="var(--c1)" foot="vs. pre-AI baseline" />
        <Kpi label="Merge Time Reduction" value={pk.merge_reduction_pct || '—'} icon={GitMerge} accent="var(--c2)" foot="faster time-to-merge" />
        <Kpi label="BA Ticket Deflection" value={pk.ba_deflection_pct || '—'} icon={Ticket} accent="var(--c3)" foot="self-served via AI" />
        <Kpi label="Analyst Hours Freed" value={pk.hours_saved || '—'} icon={Clock} accent="var(--c4)" foot="per week" />
      </div>

      <div className="section-label">Developer AI activity</div>
      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <Kpi label="Active Developers" value={fmtInt(activeDevs)} icon={Users} accent="var(--c2)" foot="using the gateway" />
        <Kpi label="Coding Sessions" value={fmtCompact(coding)} icon={Code2} accent="var(--c1)" foot="coding requests" />
        <Kpi label="Debugging Sessions" value={fmtCompact(debugging)} icon={Bug} accent="var(--c5)" foot="debugging requests" />
        <Kpi label="Dev AI Spend" value={fmtUsd(spend, true)} icon={Coins} accent="var(--c4)" foot="total dev cost" />
      </div>

      <div className="grid cols-3" style={{ marginBottom: 16 }}>
        <Card title="Daily AI Activity per Developer" hint="top 8 developers by request volume" span="2">
          <TrendChart data={dailyPerDev.rows} x="request_date" height={300}
            series={dailyPerDev.devs.map((d, i) => ({ key: d, name: d, color: undefined })).map((s, i) => ({ ...s, color: ['#35D6BE', '#4C8DFF', '#A98CFF', '#FFC15A', '#FF7AA8', '#57D98A', '#46C7FF', '#FF6B6B'][i] }))}
            tickFmt={fmtDate} valueFmt={fmtInt} />
        </Card>
        <Card title={`Model Choice: ${L.expensive} vs ${L.cheap}`} hint="request share by model" span="1">
          <Donut data={mix} nameKey="destination_model" valueKey="requests" fmt={fmtCompact} colors={['#4C8DFF', '#35D6BE']} />
        </Card>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Total AI Cost per Developer" hint="attributed spend">
          <HBar data={costPerDev} category="requester" value="total_cost_usd" valueFmt={(v) => fmtUsd(v, true)} height={320} single="#FFC15A" colorByIndex={false} />
        </Card>
        <Card title="Avg Tokens per Request" hint="prompt complexity by developer">
          <HBar data={tokensPerDev} category="requester" value="avg_tokens" valueFmt={fmtCompact} height={320} single="#A98CFF" colorByIndex={false} />
        </Card>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="Engineering: AI Sessions vs Git Commits" hint="per developer per week">
          <TrendChart data={eng} x="week_start" height={280}
            series={[{ key: 'ai_sessions_per_user', name: 'AI sessions/user', color: '#35D6BE' }, { key: 'commits_per_dev_week', name: 'Commits/dev', color: '#4C8DFF' }]}
            tickFmt={fmtDate} valueFmt={(v) => fmtNum(v, 1)} />
        </Card>
        <Card title="Product: BA Tickets vs AI Self-Serve" hint="tickets created vs deflected by AI">
          <VBar data={prod} x="week_start" height={280}
            keys={[{ key: 'ba_tickets_created', name: 'Tickets created', color: '#4C8DFF' }, { key: 'ba_tickets_deflected_by_ai', name: 'Deflected by AI', color: '#35D6BE' }]}
            tickFmt={fmtDate} valueFmt={fmtInt} />
        </Card>
      </div>

      <div className="grid cols-2" style={{ marginBottom: 16 }}>
        <Card title="AI Adoption Curve by Team" hint="% of team actively using AI">
          <TrendChart data={adoption} x="week_start" height={260}
            series={[{ key: 'engineering', name: 'Engineering', color: '#35D6BE' }, { key: 'product', name: 'Product', color: '#A98CFF' }]}
            tickFmt={fmtDate} valueFmt={(v) => v + '%'} />
        </Card>
        <Card title="Time-to-Merge Trend" hint="avg days to merge (engineering)">
          <TrendChart data={eng} x="week_start" area height={260}
            series={[{ key: 'avg_merge_days', name: 'Avg merge days', color: '#FF9E4D' }]}
            tickFmt={fmtDate} valueFmt={(v) => fmtNum(v, 1)} />
        </Card>
      </div>

      <Card title="Developer Activity Detail" hint={`${dev.length} developer-days`}>
        <DataTable rows={dev} maxHeight={440} cap={500} initialSort={{ key: 'request_date', dir: 'desc' }}
          cols={[
            { key: 'request_date', label: 'Date', render: (r) => String(r.request_date).slice(0, 10) },
            { key: 'requester', label: 'Developer', render: (r) => <b>{r.requester}</b> },
            { key: 'role', label: 'Role', mono: true },
            { key: 'total_requests', label: 'Requests', num: true, render: (r) => fmtInt(r.total_requests) },
            { key: 'coding_requests', label: 'Coding', num: true, render: (r) => fmtInt(r.coding_requests) },
            { key: 'debugging_requests', label: 'Debug', num: true, render: (r) => fmtInt(r.debugging_requests) },
            { key: 'avg_tokens_per_req', label: 'Avg tok/req', num: true, render: (r) => fmtInt(r.avg_tokens_per_req) },
            { key: 'sonnet_requests', label: L.expensive, num: true, render: (r) => fmtInt(r.sonnet_requests) },
            { key: 'haiku_requests', label: L.cheap, num: true, render: (r) => fmtInt(r.haiku_requests) },
            { key: 'total_cost_usd', label: 'Cost', num: true, render: (r) => fmtUsd(r.total_cost_usd) },
          ]} />
      </Card>
    </>
  )
}
