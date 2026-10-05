import React, { useEffect, useState } from 'react'
import { Activity, LayoutDashboard, Users, ShieldAlert, ShieldCheck, Rocket,
         Users2, LogOut, RefreshCw, UserCog, Cloud, TrendingUp, UserCircle, Mail } from 'lucide-react'
import ManageUsers from './tabs/ManageUsers.jsx'
import { fetchBundle, fetchMe, setIdentity } from './api'
import { Spinner } from './components/ui.jsx'
import Login, { initials } from './components/Login.jsx'
import ExecutiveOverview from './tabs/ExecutiveOverview.jsx'
import UsersTeams from './tabs/UsersTeams.jsx'
import AnomalyDetection from './tabs/AnomalyDetection.jsx'
import Enforcement from './tabs/Enforcement.jsx'
import DeveloperProductivity from './tabs/DeveloperProductivity.jsx'
import WordCloud from './tabs/WordCloud.jsx'
import CostForecast from './tabs/CostForecast.jsx'
import MyUsage from './tabs/MyUsage.jsx'
import Notifications from './tabs/Notifications.jsx'

// Individual contributors see only My Usage + Word Cloud; everything else is
// manager/admin-only (mgr: true).
const TABS = [
  { id: 'exec', label: 'Executive Overview', icon: LayoutDashboard, C: ExecutiveOverview, mgr: true },
  { id: 'forecast', label: 'Cost Forecast', icon: TrendingUp, C: CostForecast, mgr: true },
  { id: 'myusage', label: 'My Usage', icon: UserCircle, C: MyUsage },
  { id: 'wordcloud', label: 'Word Cloud', icon: Cloud, C: WordCloud },
  { id: 'users', label: 'Users & Teams', icon: Users, C: UsersTeams, mgr: true },
  { id: 'anomaly', label: 'Anomaly Detection', icon: ShieldAlert, C: AnomalyDetection, mgr: true },
  { id: 'enforce', label: 'AI Gateway Enforcement', icon: ShieldCheck, C: Enforcement, mgr: true },
  { id: 'devprod', label: 'Developer Productivity', icon: Rocket, C: DeveloperProductivity, mgr: true },
  { id: 'notifs', label: 'Notifications', icon: Mail, C: Notifications, mgr: true },
  { id: 'manage', label: 'Manage Users', icon: UserCog, C: ManageUsers, manage: true },
]

export default function App() {
  const [active, setActive] = useState('exec')
  const [me, setMe] = useState(null)
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [forcePick, setForcePick] = useState(false)
  const [tick, setTick] = useState(0)

  useEffect(() => {
    let alive = true
    setData(null); setErr(null)
    fetchMe()
      .then((m) => {
        if (!alive) return
        setMe(m)
        if (m.persona) {
          fetchBundle().then((d) => alive && setData(d)).catch((e) => alive && setErr(e.message))
        }
      })
      .catch((e) => alive && setErr(e.message))
    return () => { alive = false }
  }, [tick])

  const reload = () => setTick((t) => t + 1)
  // Refetch the scoped data bundle only (no remount) — used after a team edit so
  // dashboards rescope while the Manage Users tab keeps its state.
  const refreshData = () => fetchBundle().then(setData).catch((e) => setErr(e.message))
  const pick = (email) => { setIdentity(email); setForcePick(false); reload() }
  const signOut = () => { setIdentity(''); setForcePick(true); setMe(null); reload() }

  // ---- gates ----
  if (err && !me) return <div className="loading"><div>⚠️ {err}</div></div>
  if (!me) return <Spinner label="Authenticating…" />
  if (forcePick || !me.persona) {
    return <Login me={me} onPick={pick} />
  }

  const p = me.persona
  const mgr = p.is_manager
  const admin = p.is_admin
  const roleLabel = admin ? 'Admin' : p.role_type === 'director' ? 'Director' : mgr ? 'Manager' : 'Individual'
  // Tab gating: `mgr` tabs need the team/all view (managers + admin); `manage`
  // tabs need user-admin rights (admins + managers, via can_manage).
  const visibleTabs = TABS.filter((t) => (!t.mgr || mgr) && (!t.manage || p.can_manage))
  const Active = visibleTabs.find((t) => t.id === active) || visibleTabs[0]

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><Activity size={20} strokeWidth={2.4} /></div>
          <div>
            <div className="brand-name">Gateway<b>IQ</b></div>
            <div className="brand-sub">AI Gateway Governance Console</div>
          </div>
        </div>
        <div className="topbar-spacer" />

        <div className="identity">
          <div className={`avatar avatar-sm ${mgr ? 'avatar-mgr' : ''}`}>{initials(p.name)}</div>
          <div className="identity-info">
            <div className="identity-name">
              {p.name}
              <span className={`badge ${admin ? 'sev-MEDIUM' : mgr ? 'sev-LOW' : 'badge-soft'}`} style={{ marginLeft: 8 }}>
                {roleLabel}
              </span>
            </div>
            <div className="identity-sub">
              {admin ? `All teams · ${p.scope.member_count} users in view`
                : mgr ? `${p.team} · ${p.scope.member_count} people in view`
                : `${p.title} · personal view`}
            </div>
          </div>
          <div className="identity-actions">
            {p.can_manage && (
              <button className="btn-ghost" onClick={() => setActive('manage')} title="Add or remove team members">
                <UserCog size={15} /> Manage users
              </button>
            )}
            <button className="icon-btn" onClick={reload} title="Refresh"><RefreshCw size={15} /></button>
            <button className="btn-ghost" onClick={signOut} title="Switch persona / sign out">
              <LogOut size={15} /> Switch
            </button>
          </div>
        </div>
      </header>

      <nav className="tabs">
        {visibleTabs.map((t) => (
          <button key={t.id} className={`tab ${Active.id === t.id ? 'active' : ''}`} onClick={() => setActive(t.id)}>
            <t.icon size={15} /> {t.label}
          </button>
        ))}
      </nav>

      {/* Scope ribbon — makes clear whose data is on screen */}
      <div className={`scope-ribbon ${admin ? 'scope-admin' : mgr ? 'scope-team' : 'scope-self'}`}>
        <Users2 size={14} />
        {admin
          ? <span><b>Admin view</b> — showing all {p.scope.member_count} users across every team in the system.</span>
          : mgr
          ? <span><b>Team view</b> — showing aggregated governance for <b>{p.team}</b> ({p.scope.member_count} people, including you).</span>
          : <span><b>Personal view</b> — showing only your own AI Gateway activity. You don’t have access to other users’ data.</span>}
      </div>

      {err ? (
        <div className="loading"><div>⚠️ {err}</div></div>
      ) : !data ? (
        <Spinner label="Loading governance data…" />
      ) : (
        <main className="page fade-in" key={active + tick}>
          <Active.C data={data} persona={p} onChanged={refreshData} />
        </main>
      )}

    </div>
  )
}
