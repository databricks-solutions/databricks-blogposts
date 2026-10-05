// Recharts wrappers themed for GatewayIQ's dark UI.
import React from 'react'
import {
  ResponsiveContainer, LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, AreaChart, Area, ComposedChart,
} from 'recharts'
import { PALETTE, colorFor } from '../api'

const AXIS = { stroke: 'transparent', tick: { fill: '#606C84', fontSize: 11 }, tickLine: false }
const GRID = { stroke: '#232E43', strokeDasharray: '3 3', vertical: false }

export function TrendChart({ data, x, series, height = 260, area = false, tickFmt, valueFmt }) {
  // series: [{ key, name, color }]
  const Chart = area ? AreaChart : LineChart
  const Series = area ? Area : Line
  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={height}>
        <Chart data={data} margin={{ top: 6, right: 12, left: 4, bottom: 0 }}>
          <defs>
            {series.map((s) => (
              <linearGradient key={s.key} id={`g-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={s.color} stopOpacity={0.35} />
                <stop offset="100%" stopColor={s.color} stopOpacity={0} />
              </linearGradient>
            ))}
          </defs>
          <CartesianGrid {...GRID} />
          <XAxis dataKey={x} {...AXIS} tickFormatter={tickFmt} minTickGap={24} />
          <YAxis {...AXIS} width={44} tickFormatter={valueFmt} />
          <Tooltip formatter={valueFmt ? (v) => valueFmt(v) : undefined} labelFormatter={tickFmt} />
          {series.length > 1 && <Legend iconType="plainline" />}
          {series.map((s) =>
            area ? (
              <Area key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color}
                    strokeWidth={2} fill={`url(#g-${s.key})`} dot={false} activeDot={{ r: 4 }} />
            ) : (
              <Line key={s.key} type="monotone" dataKey={s.key} name={s.name} stroke={s.color}
                    strokeWidth={2.2} dot={false} activeDot={{ r: 4 }} />
            )
          )}
        </Chart>
      </ResponsiveContainer>
    </div>
  )
}

export function HBar({ data, category, value, color, height = 280, valueFmt, colorByIndex = true, single }) {
  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }}>
          <CartesianGrid {...GRID} horizontal={false} />
          <XAxis type="number" {...AXIS} tickFormatter={valueFmt} />
          <YAxis type="category" dataKey={category} {...AXIS} width={category === 'requester' ? 120 : 150}
                 tick={{ fill: '#9CA7BC', fontSize: 11.5 }} />
          <Tooltip formatter={valueFmt ? (v) => valueFmt(v) : undefined} cursor={{ fill: 'rgba(76,141,255,0.06)' }} />
          <Bar dataKey={value} radius={[0, 6, 6, 0]} maxBarSize={26} fill={single || 'var(--c1)'}>
            {colorByIndex && !single && data.map((_, i) => <Cell key={i} fill={colorFor(i)} />)}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export function StackedHBar({ data, category, keys, height = 300, valueFmt, colorMap }) {
  // keys: [{key, name, color}]
  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }}>
          <CartesianGrid {...GRID} horizontal={false} />
          <XAxis type="number" {...AXIS} tickFormatter={valueFmt} />
          <YAxis type="category" dataKey={category} {...AXIS} width={150} tick={{ fill: '#9CA7BC', fontSize: 11.5 }} />
          <Tooltip cursor={{ fill: 'rgba(76,141,255,0.06)' }} />
          <Legend />
          {keys.map((k, i) => (
            <Bar key={k.key} dataKey={k.key} name={k.name} stackId="a" fill={k.color || colorFor(i)}
                 maxBarSize={26} radius={i === keys.length - 1 ? [0, 6, 6, 0] : 0} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export function VBar({ data, x, keys, height = 280, tickFmt, valueFmt }) {
  // keys: [{key, name, color}] — grouped vertical bars over an x category (e.g. week)
  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} margin={{ top: 6, right: 12, left: 4, bottom: 0 }} barGap={2} barCategoryGap="22%">
          <CartesianGrid {...GRID} />
          <XAxis dataKey={x} {...AXIS} tickFormatter={tickFmt} minTickGap={12} />
          <YAxis {...AXIS} width={40} tickFormatter={valueFmt} />
          <Tooltip cursor={{ fill: 'rgba(76,141,255,0.06)' }} labelFormatter={tickFmt} />
          <Legend />
          {keys.map((k, i) => (
            <Bar key={k.key} dataKey={k.key} name={k.name} fill={k.color || colorFor(i)} maxBarSize={22} radius={[4, 4, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export function ForecastChart({ data, height = 300, tickFmt, valueFmt }) {
  // data: [{ request_date, actual?, predicted?, band?: [lo, hi] }]
  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height={height}>
        <ComposedChart data={data} margin={{ top: 6, right: 14, left: 4, bottom: 0 }}>
          <CartesianGrid {...GRID} />
          <XAxis dataKey="request_date" {...AXIS} tickFormatter={tickFmt} minTickGap={26} />
          <YAxis {...AXIS} width={48} tickFormatter={valueFmt} />
          <Tooltip formatter={valueFmt ? (v) => (Array.isArray(v) ? v.map(valueFmt).join(' – ') : valueFmt(v)) : undefined}
                   labelFormatter={tickFmt} />
          <Legend iconType="plainline" />
          <Area dataKey="band" name="Confidence band" stroke="none" fill="#4C8DFF" fillOpacity={0.14} legendType="none" />
          <Line type="monotone" dataKey="actual" name="Actual" stroke="#35D6BE" strokeWidth={2.4} dot={false} connectNulls />
          <Line type="monotone" dataKey="predicted" name="Forecast" stroke="#4C8DFF" strokeWidth={2.4}
                strokeDasharray="6 4" dot={false} connectNulls />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

export function Donut({ data, nameKey, valueKey, height = 260, colors, fmt }) {
  const total = data.reduce((s, d) => s + (+d[valueKey] || 0), 0)
  return (
    <div className="chart-wrap" style={{ position: 'relative' }}>
      <ResponsiveContainer width="100%" height={height}>
        <PieChart>
          <Pie data={data} dataKey={valueKey} nameKey={nameKey} innerRadius="58%" outerRadius="82%"
               paddingAngle={2} stroke="none">
            {data.map((d, i) => <Cell key={i} fill={(colors && colors[i]) || colorFor(i)} />)}
          </Pie>
          <Tooltip formatter={(v, n) => [fmt ? fmt(v) : v, n]} />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
      <div style={{ position: 'absolute', top: '42%', left: 0, right: 0, textAlign: 'center', pointerEvents: 'none', transform: 'translateY(-50%)' }}>
        <div style={{ fontSize: 11, color: 'var(--text-faint)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Total</div>
        <div style={{ fontSize: 20, fontWeight: 800, fontVariantNumeric: 'tabular-nums' }}>{fmt ? fmt(total) : total}</div>
      </div>
    </div>
  )
}
