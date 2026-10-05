import React from 'react'
import { Activity, Fingerprint, ChevronRight, ShieldAlert } from 'lucide-react'

export const initials = (name) =>
  (name || '?').split(/\s+/).slice(0, 2).map((s) => s[0]).join('').toUpperCase()

// Production login: workspace SSO only. The app auto-detects the signed-in user
// (X-Forwarded-Email); this screen only appears if that identity isn't
// provisioned, or after an explicit re-check.
export default function Login({ me, onPick }) {
  return (
    <div className="login-screen">
      <div className="login-card fade-in">
        <div className="login-head">
          <div className="brand-mark"><Activity size={22} strokeWidth={2.4} /></div>
          <div>
            <div className="brand-name" style={{ fontSize: 22 }}>Gateway<b>IQ</b></div>
            <div className="brand-sub">AI Gateway Governance Console</div>
          </div>
        </div>

        <div className="login-title">Sign in</div>
        <div className="login-desc">
          Your view is scoped to your identity — admins and managers see their teams, individuals see
          only their own AI usage.
        </div>

        <div className="sso-panel">
          {me?.sso_matched ? (
            <>
              <button className="sso-continue" onClick={() => onPick(me.sso_email)}>
                <Fingerprint size={18} />
                <div>
                  <div className="sso-continue-t">Continue as {me.sso_name}</div>
                  <div className="sso-continue-s">{me.sso_email} · via workspace SSO</div>
                </div>
                <ChevronRight size={16} style={{ marginLeft: 'auto' }} />
              </button>
              <div className="sso-hint">You’re authenticated with your Databricks workspace identity.</div>
            </>
          ) : (
            <div className="login-sso-note">
              <ShieldAlert size={14} style={{ verticalAlign: '-2px', marginRight: 6 }} />
              {me?.sso_email
                ? <>Your account <b>{me.sso_email}</b> isn’t provisioned in GatewayIQ yet. Contact your workspace admin to be added.</>
                : <>No single sign-on identity was detected. Open GatewayIQ from your Databricks workspace so you’re signed in.</>}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
