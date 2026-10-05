// Shared UI primitives for GatewayIQ
import React from 'react'

export function Card({ title, hint, right, className = '', bodyClass = '', children, span }) {
  return (
    <div className={`card ${span ? 'span-' + span : ''} ${className}`}>
      {(title || right) && (
        <div className="card-head">
          <div>
            <div className="card-title">{title}</div>
            {hint && <div className="card-hint">{hint}</div>}
          </div>
          {right}
        </div>
      )}
      <div className={bodyClass}>{children}</div>
    </div>
  )
}

export function Kpi({ label, value, unit, foot, accent = 'var(--primary)', icon: Icon }) {
  return (
    <div className="card kpi" style={{ '--kpi-accent': accent }}>
      <div className="kpi-label">
        {Icon && <Icon className="ic" size={14} />}
        {label}
      </div>
      <div className="kpi-value">
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      {foot && <div className="kpi-foot">{foot}</div>}
    </div>
  )
}

export function SeverityBadge({ value }) {
  const v = String(value || '').toUpperCase()
  return <span className={`badge sev-${v || 'NORMAL'}`}>{v || '—'}</span>
}

export function Empty({ children = 'No data' }) {
  return <div className="empty">{children}</div>
}

export function Spinner({ label = 'Loading…' }) {
  return (
    <div className="loading">
      <div className="spinner" />
      <div>{label}</div>
    </div>
  )
}
