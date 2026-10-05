import React, { useEffect, useMemo, useState } from 'react'
import { UserPlus, UserMinus, Search, ShieldAlert, CheckCircle2 } from 'lucide-react'
import { fetchUsers, saveUser, removeUser } from '../api'
import { initials } from '../components/Login.jsx'
import { Card } from '../components/ui.jsx'

const ROLE_LABEL = { ic: 'User', manager: 'Manager', admin: 'Admin' }
const ROLE_BADGE = { admin: 'sev-MEDIUM', manager: 'sev-LOW', ic: 'badge-soft' }

function Avatar({ name, role }) {
  return <div className={`avatar avatar-sm ${role && role !== 'ic' ? 'avatar-mgr' : ''}`}>{initials(name)}</div>
}

// Admin/manager console — add users to the system (Name / Email / Manager /
// Role), and edit or remove existing ones. Works like adding users to a
// Databricks workspace. All state lives in Lakebase (app_users / app_membership);
// the app rescopes to the new membership after each change.
export default function ManageUsers({ persona, onChanged }) {
  const [data, setData] = useState(null)     // { users, managers, roles }
  const [q, setQ] = useState('')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState(null)
  const [ok, setOk] = useState(null)

  // Add-user form
  const [form, setForm] = useState({ name: '', email: '', role_type: 'ic', manager: '' })

  useEffect(() => { load() }, [])
  function load() { fetchUsers().then(setData).catch((e) => setErr(e.message)) }

  const managers = data?.managers || []
  const users = useMemo(() => {
    const s = q.trim().toLowerCase()
    const list = data?.users || []
    return s ? list.filter((u) => (u.name + ' ' + u.email + ' ' + u.title).toLowerCase().includes(s)) : list
  }, [q, data])

  const managerName = (email) => managers.find((m) => m.email === email)?.name || email || '—'

  async function submit(payload, okMsg, clearForm) {
    setBusy(true); setErr(null); setOk(null)
    try {
      const d = await saveUser(payload)
      setData(d)
      setOk(okMsg)
      if (clearForm) setForm({ name: '', email: '', role_type: 'ic', manager: '' })
      onChanged?.()
    } catch (e) { setErr(e.message) } finally { setBusy(false) }
  }

  function addUser(e) {
    e.preventDefault()
    submit(
      { ...form, email: form.email.trim().toLowerCase(), name: form.name.trim() },
      `Added ${form.name.trim()} as ${ROLE_LABEL[form.role_type]}.`,
      true,
    )
  }

  async function changeRole(u, role_type) {
    await submit({ email: u.email, name: u.name, role_type, manager: u.manager || '' },
      `Updated ${u.name}’s role to ${ROLE_LABEL[role_type]}.`, false)
  }
  async function changeManager(u, manager) {
    await submit({ email: u.email, name: u.name, role_type: u.role_type, manager },
      `Moved ${u.name} under ${managerName(manager)}.`, false)
  }
  async function remove(u) {
    setBusy(true); setErr(null); setOk(null)
    try {
      const d = await removeUser(u.email)
      setData(d)
      setOk(`Removed ${u.name}.`)
      onChanged?.()
    } catch (e) { setErr(e.message) } finally { setBusy(false) }
  }

  if (!persona?.can_manage) {
    return (
      <div className="page-head">
        <div className="login-sso-note">
          <ShieldAlert size={14} style={{ verticalAlign: '-2px', marginRight: 6 }} />
          User management is available to admins and managers only.
        </div>
      </div>
    )
  }

  const needsManager = form.role_type === 'ic'

  return (
    <>
      <div className="page-head">
        <h1 className="page-title">Manage Users</h1>
        <div className="page-desc">
          Add people to GatewayIQ and set their role. Users see only their own AI usage,
          managers see their team, admins see everyone. Changes take effect immediately.
        </div>
      </div>

      {err && (
        <div className="login-sso-note" style={{ marginBottom: 14 }}>
          <ShieldAlert size={14} style={{ verticalAlign: '-2px', marginRight: 6 }} /> {err}
        </div>
      )}
      {ok && (
        <div className="banner-ok" style={{ marginBottom: 14 }}>
          <CheckCircle2 size={14} style={{ verticalAlign: '-2px', marginRight: 6 }} /> {ok}
        </div>
      )}

      <Card title="Add a user" hint="Enter their details and pick a role — like adding a user to a Databricks workspace.">
        <form className="user-form" onSubmit={addUser}>
          <label className="field">
            <span className="field-label">Full name</span>
            <input className="input" placeholder="Jordan Lee" value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })} required />
          </label>
          <label className="field">
            <span className="field-label">Email</span>
            <input className="input" type="email" placeholder="jordan.lee@company.com" value={form.email}
              onChange={(e) => setForm({ ...form, email: e.target.value })} required />
          </label>
          <label className="field">
            <span className="field-label">Role</span>
            <select className="input" value={form.role_type}
              onChange={(e) => setForm({ ...form, role_type: e.target.value })}>
              <option value="ic">User</option>
              <option value="manager">Manager</option>
              <option value="admin">Admin</option>
            </select>
          </label>
          <label className="field">
            <span className="field-label">Manager{needsManager ? '' : ' (optional)'}</span>
            <select className="input" value={form.manager}
              onChange={(e) => setForm({ ...form, manager: e.target.value })} required={needsManager}>
              <option value="">{needsManager ? 'Select a manager…' : 'No manager'}</option>
              {managers.map((m) => <option key={m.email} value={m.email}>{m.name}</option>)}
            </select>
          </label>
          <button className="pw-submit" type="submit" disabled={busy}>
            <UserPlus size={15} /> Add user
          </button>
        </form>
      </Card>

      <Card title={`Users${data ? ` · ${users.length}` : ''}`}
        hint="Change a role or manager inline, or remove a user."
        right={
          <div className="login-search" style={{ minWidth: 220 }}>
            <Search size={15} className="muted" />
            <input className="input" placeholder="Search users…" value={q} onChange={(e) => setQ(e.target.value)} />
          </div>
        }>
        <div className="user-table">
          <div className="user-row user-row-head">
            <span>Name</span><span>Role</span><span>Manager</span><span></span>
          </div>
          {!data && <div className="empty">Loading users…</div>}
          {data && !users.length && <div className="empty">No users match “{q}”.</div>}
          {users.map((u) => (
            <div key={u.email} className="user-row">
              <div className="user-cell-name">
                <Avatar name={u.name} role={u.role_type} />
                <div className="login-person-info">
                  <div className="login-person-name">
                    {u.name}{u.is_self && <span className="faint"> · you</span>}
                    <span className={`badge ${ROLE_BADGE[u.role_type]}`} style={{ marginLeft: 8 }}>
                      {ROLE_LABEL[u.role_type]}
                    </span>
                  </div>
                  <div className="login-person-title">{u.email}</div>
                </div>
              </div>
              <select className="input input-sm" value={u.role_type} disabled={busy || u.is_self}
                onChange={(e) => changeRole(u, e.target.value)}>
                <option value="ic">User</option>
                <option value="manager">Manager</option>
                <option value="admin">Admin</option>
              </select>
              <select className="input input-sm" value={u.manager || ''} disabled={busy}
                onChange={(e) => changeManager(u, e.target.value)}>
                <option value="">No manager</option>
                {managers.filter((m) => m.email !== u.email).map((m) => (
                  <option key={m.email} value={m.email}>{m.name}</option>
                ))}
              </select>
              <button className="btn-ghost btn-danger" disabled={busy || u.is_self}
                title={u.is_self ? 'You can’t remove yourself' : 'Remove user'}
                onClick={() => remove(u)}>
                <UserMinus size={14} /> Remove
              </button>
            </div>
          ))}
        </div>
      </Card>
    </>
  )
}
