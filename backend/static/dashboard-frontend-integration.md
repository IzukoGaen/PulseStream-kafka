# Cursor task: add the PulseStream dashboard UI

## Context
The FastAPI backend already exists and is working (Kafka/Confluent consumer
threads, `/ws/{name}` websocket route, `static/index.html` as a bare-bones
test page). This task only adds a new, polished frontend file — no backend
logic changes are needed.

## What to do
1. Save the attached `dashboard.html` file as `backend/static/dashboard.html`
   (same `static/` folder that already holds `index.html` — do not overwrite
   `index.html`, just add this as a second file next to it).
2. Do not modify `main.py`, `consumer.py`, or `config.py`. The existing
   `app.mount("/static", StaticFiles(directory="static", html=True), name="static")`
   line already serves any file placed in `static/`, including this one —
   no route changes are required.
3. Confirm the backend is running (`uvicorn main:app --reload` or however it's
   currently started), then open:
   `http://localhost:8000/static/dashboard.html`
4. Verify all four panels move from their "waiting for data" state to live
   values within ~10-20 seconds (one tumbling window or two):
   - Requests / 10s window (top bar chart)
   - Status breakdown (2xx/3xx/4xx/5xx bars + error rate)
   - Active users (number + sparkline)
   - Top pages (ranked table)
5. If a panel never leaves its "waiting" state, open the browser dev console
   and check the raw payload on that specific websocket
   (`ws://localhost:8000/ws/request-count`, `/ws/status-breakdown`,
   `/ws/active-users`, `/ws/top-pages`) — the frontend expects these exact
   field names per message, matching the materialized table columns:
   - `request-count`: `window_start`, `window_end`, `request_count`
   - `status-breakdown`: `window_start`, `window_end`, `status`, `status_count`
   - `active-users`: `window_start`, `window_end`, `active_users`
   - `top-pages`: `window_start`, `window_end`, `request`, `hit_count`
   If the real payload uses different key names, only `dashboard.html`'s
   JS needs to change (the field references are all in one place per
   stream) — the backend/consumer code stays untouched.

## Design decisions to preserve
- `dashboard.html` is a single self-contained file (HTML/CSS/JS inline,
  Google Fonts as the only external request) — don't split it into
  separate CSS/JS files or add a build step (no bundler, no npm) unless
  explicitly asked.
- Status-breakdown and top-pages group incoming rows client-side by
  `window_start`, because each materialized-table row for those two
  streams arrives as a separate websocket message (one per status code,
  one per top-5 page) rather than one combined payload per window. Don't
  "simplify" this into expecting one message per window for those two
  streams — that would break the grouping logic.
- The websocket reconnect logic (retry every 3s, per-stream connection
  state, collapsed into one overall Live / Reconnecting / Offline
  indicator) is intentional so the page degrades gracefully instead of
  breaking if the backend isn't reachable yet (e.g. corporate network
  blocking port 9092 upstream) — keep it rather than replacing with a
  single unconditional `WebSocket()` call.
