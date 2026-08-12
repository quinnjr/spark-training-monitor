<script>
  import MeterBar from './MeterBar.svelte';
  import {
    formatUptime,
    formatKb,
    formatMib,
    memUsedPct,
    formatTimestamp,
    metricRows,
  } from './format.js';

  let { device } = $props();

  const status = $derived(device.status ?? null);
  const memPct = $derived(
    status ? memUsedPct(status.mem_total_kb, status.mem_available_kb) : null
  );
  const training = $derived(status?.training ?? null);
  const rows = $derived(training ? metricRows(training.metrics) : []);
</script>

<article
  class="flex overflow-hidden rounded-lg bg-panel ring-1 ring-panel-edge {device.online
    ? ''
    : 'opacity-70'}"
>
  <!-- LED rail: the rack-unit signature -->
  <div class="flex w-10 shrink-0 flex-col items-center gap-3 border-r border-panel-edge bg-inset py-4">
    <span
      class="h-2.5 w-2.5 rounded-full {device.online ? 'bg-good lamp-on' : 'bg-crit'}"
      role="img"
      aria-label={device.online ? 'online' : 'offline'}
    ></span>
    <span
      class="text-[10px] font-semibold uppercase [writing-mode:vertical-rl] tracking-[0.3em] text-ink-faint"
    >
      {device.online ? 'online' : 'offline'}
    </span>
  </div>

  <div class="flex-1 space-y-4 p-4">
    <header class="flex items-baseline justify-between gap-2">
      <h2 class="font-mono text-lg font-semibold tracking-tight text-lamp">{device.name}</h2>
      <span class="font-mono text-xs text-ink-muted">{device.addr}</span>
    </header>

    {#if !device.online}
      <p class="rounded border border-crit/30 bg-crit/10 px-3 py-2 text-xs text-crit">
        <i class="fa-solid fa-triangle-exclamation mr-1.5"></i>
        {device.last_error ?? 'Check-in failed'} — showing last known readings.
      </p>
    {/if}

    {#if status}
      <div class="grid grid-cols-3 gap-2 text-center">
        <div class="rounded bg-inset px-2 py-2 ring-1 ring-panel-edge">
          <div class="text-[10px] uppercase tracking-widest text-ink-faint">Uptime</div>
          <div class="mt-1 font-mono text-sm">{formatUptime(status.uptime_secs)}</div>
        </div>
        <div class="rounded bg-inset px-2 py-2 ring-1 ring-panel-edge">
          <div class="text-[10px] uppercase tracking-widest text-ink-faint">Load 1m</div>
          <div class="mt-1 font-mono text-sm">{status.load_avg ? status.load_avg[0].toFixed(2) : '—'}</div>
        </div>
        <div class="rounded bg-inset px-2 py-2 ring-1 ring-panel-edge">
          <div class="text-[10px] uppercase tracking-widest text-ink-faint">Memory</div>
          <div class="mt-1 font-mono text-sm">{formatKb(status.mem_total_kb)}</div>
        </div>
      </div>

      <MeterBar label="RAM" pct={memPct} />

      {#if status.gpus?.length}
        <div class="space-y-3">
          {#each status.gpus as gpu (gpu.index)}
            <div class="space-y-2 rounded bg-inset/60 p-3 ring-1 ring-panel-edge">
              <div class="flex items-center justify-between text-xs">
                <span class="text-ink-muted">
                  <i class="fa-solid fa-microchip mr-1.5 text-data"></i>GPU {gpu.index} · {gpu.name}
                </span>
                <span class="font-mono text-ink-muted">
                  {gpu.temperature_c != null ? `${gpu.temperature_c}°C` : ''}
                  {gpu.power_w != null ? ` · ${Math.round(gpu.power_w)} W` : ''}
                </span>
              </div>
              <MeterBar label="Util" pct={gpu.utilization_pct} />
              <MeterBar
                label="VRAM"
                pct={gpu.mem_total_mib ? ((gpu.mem_used_mib ?? 0) / gpu.mem_total_mib) * 100 : null}
                detail={formatMib(gpu.mem_used_mib)}
              />
            </div>
          {/each}
        </div>
      {:else}
        <p class="text-xs text-ink-faint"><i class="fa-solid fa-microchip mr-1.5"></i>No GPU reported.</p>
      {/if}

      <section class="rounded bg-inset/60 p-3 ring-1 ring-panel-edge">
        <div class="mb-2 flex items-center justify-between text-[10px] uppercase tracking-widest text-ink-faint">
          <span><i class="fa-solid fa-dumbbell mr-1.5"></i>Training</span>
          {#if training}
            <span>reported {formatTimestamp(training.received_at)}</span>
          {/if}
        </div>
        {#if rows.length}
          <dl class="grid grid-cols-2 gap-x-4 gap-y-1 sm:grid-cols-3">
            {#each rows as [key, value] (key)}
              <div>
                <dt class="truncate text-[11px] text-ink-muted">{key}</dt>
                <dd class="truncate font-mono text-sm" title={value}>{value}</dd>
              </div>
            {/each}
          </dl>
        {:else}
          <p class="text-xs text-ink-faint">
            No run reporting. POST metrics to
            <code class="font-mono">:{device.http_port}/training</code> on this host.
          </p>
        {/if}
      </section>
    {:else}
      <p class="text-xs text-ink-faint">
        No status snapshot yet — the host is reachable but hasn't served <code class="font-mono">/status</code>.
      </p>
    {/if}

    <footer class="text-[11px] text-ink-faint">
      Last check-in: <span class="font-mono">{formatTimestamp(device.last_checkin)}</span>
    </footer>
  </div>
</article>
