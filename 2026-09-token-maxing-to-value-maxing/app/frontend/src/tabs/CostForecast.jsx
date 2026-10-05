import React, { useEffect, useMemo, useState } from 'react'
import { Coins, TrendingUp, Percent, Gauge } from 'lucide-react'
import { Card, Kpi, Spinner } from '../components/ui.jsx'
import { TrendChart, ForecastChart } from '../components/charts.jsx'
import { fetchForecast, fmtUsd, fmtUsdAxis, fmtDate, colorFor } from '../api'

export default function CostForecast({ persona }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)

  useEffect(() => {
    let alive = true
    fetchForecast().then((d) => alive && setData(d)).catch((e) => alive && setErr(e.message))
    return () => { alive = false }
  }, [])

  // pivot daily_by_team → one row per date, a column per team
  const teamTrend = useMemo(() => {
    if (!data) return []
    const m = new Map()
    for (const r of data.daily_by_team) {
      const d = m.get(r.request_date) || { request_date: r.request_date }
      d[r.team] = (d[r.team] || 0) + r.cost
      m.set(r.request_date, d)
    }
    return [...m.values()].sort((a, b) => a.request_date.localeCompare(b.request_date))
  }, [data])

  if (err) return <div className="loading"><div>⚠️ {err}</div></div>
  if (!data) return <Spinner label="Building forecast…" />

  const k = data.kpis
  const scopeNote = persona.is_admin ? 'all users' : persona.is_manager ? persona.team : 'your usage'
  const growthPos = k.growth_pct >= 0

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Cost Forecast</h1>
        <div className="page-desc">Projected AI Gateway spend for {scopeNote} — next 30 days, modelled on the recent daily run-rate.</div>
      </div>

      <div className="grid cols-4" style={{ marginBottom: 16 }}>
        <Kpi label="Last 30d Spend" value={fmtUsd(k.last_30d_cost_usd, true)} icon={Coins} accent="var(--c1)" foot="actual attributed spend" />
        <Kpi label="Projected Next 30d" value={fmtUsd(k.projected_30d_cost_usd, true)} icon={TrendingUp} accent="var(--c2)" foot="forecast" />
        <Kpi label="Projected Growth" value={`${growthPos ? '+' : ''}${k.growth_pct}`} unit="%" icon={Percent}
             accent={growthPos ? 'var(--sev-high)' : 'var(--sev-low)'} foot="vs. last 30 days" />
        <Kpi label="Daily Run Rate" value={fmtUsd(k.daily_run_rate_usd, true)} icon={Gauge} accent="var(--c4)" foot="avg spend / day" />
      </div>

      <Card title="30-Day Cost Forecast" hint="actual (solid) + projected spend with confidence band" style={{ marginBottom: 16 }}>
        <ForecastChart data={data.series} height={320} tickFmt={fmtDate} valueFmt={fmtUsdAxis} />
      </Card>

      <Card title="Daily Cost by Team" hint="attributed spend per day, by team">
        <TrendChart data={teamTrend} x="request_date" height={300}
          series={(data.teams || []).map((t, i) => ({ key: t, name: t, color: colorFor(i) }))}
          tickFmt={fmtDate} valueFmt={fmtUsdAxis} />
      </Card>
    </>
  )
}
