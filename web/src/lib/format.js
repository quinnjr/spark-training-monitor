/** Pure formatting helpers for the dashboard. */

export function formatUptime(secs) {
  if (secs == null) return '—';
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

export function formatKb(kb) {
  if (kb == null) return '—';
  const gib = kb / (1024 * 1024);
  return gib >= 10 ? `${gib.toFixed(0)} GiB` : `${gib.toFixed(1)} GiB`;
}

export function formatMib(mib) {
  if (mib == null) return '—';
  const gib = mib / 1024;
  return gib >= 10 ? `${gib.toFixed(0)} GiB` : `${gib.toFixed(1)} GiB`;
}

/** Percentage of memory in use, from total/available kB. */
export function memUsedPct(totalKb, availableKb) {
  if (!totalKb || availableKb == null) return null;
  return ((totalKb - availableKb) / totalKb) * 100;
}

export function formatTimestamp(iso) {
  if (!iso) return '—';
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function formatMetricValue(value) {
  if (typeof value === 'number') {
    if (Number.isInteger(value)) return value.toLocaleString();
    return Math.abs(value) < 0.01 ? value.toExponential(2) : value.toFixed(4);
  }
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

/**
 * Flattens a pushed training-metrics object into displayable [key, value]
 * rows, skipping nested objects/arrays beyond one JSON-stringified level.
 */
export function metricRows(metrics) {
  if (!metrics || typeof metrics !== 'object' || Array.isArray(metrics)) return [];
  return Object.entries(metrics).map(([k, v]) => [k, formatMetricValue(v)]);
}
