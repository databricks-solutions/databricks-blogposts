// Generic, fast data table with sticky header + client-side sort + optional cap.
import React, { useMemo, useState } from 'react'
import { ChevronUp, ChevronDown } from 'lucide-react'

/**
 * cols: [{ key, label, num?, mono?, clip?, width?, render?(row)=>node, align? }]
 */
export default function DataTable({ cols, rows, maxHeight = 460, initialSort, cap = 500 }) {
  const [sort, setSort] = useState(initialSort || null) // {key, dir}

  const sorted = useMemo(() => {
    let out = rows || []
    if (sort) {
      const { key, dir } = sort
      out = [...out].sort((a, b) => {
        let x = a[key], y = b[key]
        const nx = parseFloat(x), ny = parseFloat(y)
        if (!isNaN(nx) && !isNaN(ny)) { x = nx; y = ny }
        if (x == null) return 1
        if (y == null) return -1
        if (x < y) return dir === 'asc' ? -1 : 1
        if (x > y) return dir === 'asc' ? 1 : -1
        return 0
      })
    }
    return out
  }, [rows, sort])

  const shown = cap ? sorted.slice(0, cap) : sorted
  const toggle = (key) =>
    setSort((s) => (s && s.key === key ? { key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key, dir: 'desc' }))

  if (!rows || !rows.length) return <div className="empty">No matching rows</div>

  return (
    <div className="table-wrap" style={{ '--tbl-h': maxHeight + 'px' }}>
      <table className="dt">
        <thead>
          <tr>
            {cols.map((c) => (
              <th
                key={c.key}
                className={c.num ? 'num' : ''}
                style={{ width: c.width, cursor: 'pointer', userSelect: 'none' }}
                onClick={() => toggle(c.key)}
              >
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, justifyContent: c.num ? 'flex-end' : 'flex-start' }}>
                  {c.label}
                  {sort?.key === c.key && (sort.dir === 'asc' ? <ChevronUp size={12} /> : <ChevronDown size={12} />)}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td
                  key={c.key}
                  className={[c.num ? 'num' : '', c.mono ? 'mono' : '', c.clip ? 'clip' : ''].join(' ').trim()}
                  title={c.clip ? String(r[c.key] ?? '') : undefined}
                >
                  {c.render ? c.render(r) : (r[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {cap && sorted.length > cap && (
        <div className="faint" style={{ padding: '8px 12px', fontSize: 11.5 }}>
          Showing {cap.toLocaleString()} of {sorted.length.toLocaleString()} rows
        </div>
      )}
    </div>
  )
}
