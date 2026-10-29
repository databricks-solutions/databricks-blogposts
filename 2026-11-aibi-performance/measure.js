// Run via chrome-devtools evaluate_script right after navigating to a published dashboard
// (after ./step.sh <variant>; navigate with initScript: performance.setResourceTimingBufferSize(10000);)
// 1. subsequent load: navigation start -> last dashboard-query response (served from the dashboard cache, which
//    ignores the table-property commits of invalidate.sh)
// 2. first load: clicks Refresh -> last dashboard-query response (cold: new table versions, no result cache)
// "Settled" = no new dashboard-query requests for 10s (long-poll requests take ~5s).
async () => {
  const isQ = e => e.name.includes('/lakeview-query/');
  const settle = async (since) => {
    const qs = () => performance.getEntriesByType('resource').filter(e => isQ(e) && e.startTime >= since);
    let last = -1, stable = 0; const t0 = Date.now();
    while (Date.now() - t0 < 240000) {
      const n = qs().length;
      if (n === last && n > 0) { stable += 500; if (stable >= 10000) break; } else { stable = 0; last = n; }
      await new Promise(r => setTimeout(r, 500));
    }
    const es = qs();
    return { n: es.length, end: Math.max(...es.map(e => e.responseEnd)) };
  };
  const nav = await settle(0);
  const btn = [...document.querySelectorAll('button')].find(b => /^Refresh/.test(b.getAttribute('aria-label') || ''));
  if (!btn) return { error: 'Refresh button not found', subsequent_load_s: +(nav.end / 1000).toFixed(2) };
  const tClick = performance.now();
  btn.click();
  const ref = await settle(tClick);
  return {
    subsequent_load_s: +(nav.end / 1000).toFixed(2), n_requests_nav: nav.n,
    first_load_s: +((ref.end - tClick) / 1000).toFixed(2), n_requests_refresh: ref.n,
  };
}
