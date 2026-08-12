# spark-training-monitor

Liveness and training monitoring for spark training hosts on the LAN.

## Components

- **spark-monitor-server** — runs on each training host. Advertises
  `_spark-monitor._tcp` over mDNS, answers TCP check-ins on port 7879, and
  serves an HTTP status API on port 7880.
- **spark-monitor-client** — runs on the desktop. Discovers hosts over mDNS,
  checks in every 30s, sends desktop notifications on state changes, and
  serves a REST API on `127.0.0.1:7881` aggregating every registered device.
- **web/** — Svelte + Tailwind + Font Awesome dashboard that polls the REST
  API and displays fleet health, GPU stats, and training metrics.

## Spark-side HTTP API (port 7880)

- `GET /status` — snapshot: hostname, uptime, load averages, memory, GPUs
  (via `nvidia-smi`), and the last pushed training metrics.
- `POST /training` — training scripts push a JSON object of metrics, e.g.:

  ```bash
  curl -X POST http://localhost:7880/training \
    -H 'Content-Type: application/json' \
    -d '{"run":"llama-ft-3","epoch":2,"step":1480,"total_steps":5000,"loss":0.8342}'
  ```

  The payload is schemaless; whatever keys are sent are displayed on the
  dashboard.

## Client REST API (127.0.0.1:7881)

- `GET /api/health` — API liveness and version.
- `GET /api/devices` — all registered devices with liveness, last check-in,
  and the latest `/status` snapshot (kept stale on flaps, `null` for hosts
  running an older server without the HTTP API).
- `GET /api/devices/{name}` — one device by instance name; 404 if unknown.

## Dashboard

```bash
cd web
npm install
npm run dev     # http://localhost:5173, proxies /api to 127.0.0.1:7881
npm run build   # static build in web/dist
```

## Environment variables

| Variable | Component | Default | Purpose |
|---|---|---|---|
| `SPARK_MONITOR_BIND` | server | `::` | Bind address for check-in + status API |
| `SPARK_MONITOR_INSTANCE` | server | `spark` | mDNS instance name |
| `SPARK_MONITOR_HTTP_PORT` | server | `7880` | Status API port (advertised via mDNS TXT `http_port`) |
| `SPARK_MONITOR_API_BIND` | client | `127.0.0.1:7881` | REST API bind address |
| `SPARK_MONITOR_LOG_FILE` | both | unset | Also append logs to this file |
