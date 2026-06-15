from playwright.sync_api import sync_playwright
import json

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={'width': 1280, 'height': 900})
    page = ctx.new_page()

    # Use Playwright request context to bypass CORS
    req = ctx.request
    stats = req.get('http://127.0.0.1:8129/stats?window=10s').json()
    backends = req.get('http://127.0.0.1:8129/backends').json()
    healthz = req.get('http://127.0.0.1:8128/healthz').text()
    proxy_resp = req.get('http://127.0.0.1:8128/')
    proxy_body = proxy_resp.text()
    proxy_header = proxy_resp.headers.get('x-backend-id', '')

    print('stats:', json.dumps(stats, indent=2))
    print('backends:', json.dumps(backends, indent=2))
    print('healthz:', healthz)
    print('proxy_body:', proxy_body, 'x-backend-id:', proxy_header)

    # Render a self-contained dashboard with the data already embedded
    rows_html = ''.join(
        f'<tr><th>{k}</th><td class="num">{v}</td></tr>'
        for k, v in [
            ('total_requests', stats['total_requests']),
            ('total_errors', stats['total_errors']),
            ('window_requests', stats['window_requests']),
            ('window_errors', stats['window_errors']),
            ('avg_latency_ms', round(stats['avg_latency_ms'], 2)),
            ('p99_latency_ms', round(stats['p99_latency_ms'], 2)),
            ('active_connections', stats['active_connections']),
            ('backend_count', stats['backend_count']),
        ]
    )
    be_rows = ''.join(
        f'<tr><td>{b["id"]}</td><td>{b["address"]}</td><td>{b["weight"]}</td>'
        f'<td class="{"ok" if b["healthy"] else "bad"}">{b["healthy"]}</td>'
        f'<td>{b["circuit_breaker_state"]}</td><td>{b["active_conns"]}</td></tr>'
        for b in backends
    )

    html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><title>Load Balancer R2</title>
<style>
body {{ font-family:-apple-system,system-ui,sans-serif; background:#1a1a1a; color:#eaeaea; margin:24px; }}
h1 {{ color:#4ade80; }} h2 {{ color:#93c5fd; border-bottom:1px solid #444; padding-bottom:6px; }}
.grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
.card {{ background:#262626; padding:16px; border-radius:8px; border:1px solid #3a3a3a; }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th,td {{ padding:6px 10px; text-align:left; border-bottom:1px solid #3a3a3a; }}
th {{ color:#fbbf24; }} .ok {{ color:#4ade80; font-weight:600; }} .bad {{ color:#f87171; }}
.num {{ font-family:Consolas,monospace; color:#f0abfc; }}
pre {{ background:#111; padding:10px; border-radius:4px; overflow-x:auto; }}
.badge {{ background:#14532d; color:#4ade80; padding:4px 10px; border-radius:4px; display:inline-block; }}
</style></head><body>
<h1>Load Balancer R2 - 5521-go-loadbalancer <span class='badge'>{healthz}</span></h1>
<p>Snapshot at: {__import__('datetime').datetime.now().isoformat()}</p>
<div class='grid'>
  <div class='card'><h2>Stats (window={stats['window']})</h2><table>{rows_html}</table></div>
  <div class='card'><h2>Backends</h2><table>
    <thead><tr><th>ID</th><th>Addr</th><th>Wt</th><th>Healthy</th><th>CB</th><th>Conns</th></tr></thead>
    <tbody>{be_rows}</tbody></table></div>
</div>
<h2>Live Proxy Check</h2>
<p>GET <code>http://127.0.0.1:8128/</code> &rarr; <code>{proxy_body}</code> via <code>X-Backend-ID: {proxy_header}</code></p>
<h2>Raw /stats?window=10s response</h2>
<pre>{json.dumps(stats, indent=2)}</pre>
</body></html>"""

    page.set_content(html)
    page.wait_for_load_state('domcontentloaded')
    page.wait_for_timeout(500)
    page.screenshot(
        path='C:/Users/白东鑫/work01/SoloCoder/5521-go-loadbalancer/screenshot_R2.png',
        full_page=True
    )
    print('SCREENSHOT_SAVED')
    browser.close()
