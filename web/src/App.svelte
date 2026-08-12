<script>
  import DeviceCard from './lib/DeviceCard.svelte';
  import { formatTimestamp } from './lib/format.js';

  const POLL_INTERVAL_MS = 5000;

  let devices = $state([]);
  let apiError = $state(null);
  let lastPoll = $state(null);

  let polling = false;

  async function poll() {
    if (polling) return;
    polling = true;
    try {
      const resp = await fetch('/api/devices');
      if (!resp.ok) throw new Error(`API returned ${resp.status}`);
      devices = await resp.json();
      apiError = null;
      lastPoll = new Date().toISOString();
    } catch (e) {
      apiError = e.message;
    } finally {
      polling = false;
    }
  }

  $effect(() => {
    poll();
    const id = setInterval(poll, POLL_INTERVAL_MS);
    return () => clearInterval(id);
  });

  const online = $derived(devices.filter((d) => d.online).length);
</script>

<div class="mx-auto max-w-5xl px-4 py-8">
  <!-- Nameplate -->
  <header class="mb-8 flex items-end justify-between border-b border-panel-edge pb-4">
    <div>
      <p class="text-[11px] uppercase tracking-[0.35em] text-lamp">
        <i class="fa-solid fa-bolt mr-1.5"></i>Spark Fleet
      </p>
      <h1 class="mt-1 font-mono text-2xl font-semibold tracking-tight">Training Monitor</h1>
    </div>
    <div class="text-right text-xs text-ink-muted">
      <p>
        <span class="font-mono text-ink">{online}</span> of
        <span class="font-mono text-ink">{devices.length}</span> hosts online
      </p>
      <p class="mt-1">
        polled <span class="font-mono">{formatTimestamp(lastPoll)}</span>
      </p>
    </div>
  </header>

  {#if apiError}
    <div class="mb-6 rounded border border-crit/30 bg-crit/10 px-4 py-3 text-sm text-crit">
      <i class="fa-solid fa-plug-circle-xmark mr-2"></i>
      Can't reach the monitor API ({apiError}). Is <code class="font-mono">spark-monitor-client</code> running?
    </div>
  {/if}

  {#if devices.length === 0 && !apiError}
    <div class="rounded-lg border border-dashed border-panel-edge px-6 py-16 text-center text-ink-muted">
      <i class="fa-solid fa-satellite-dish mb-3 block text-2xl text-ink-faint"></i>
      <p class="text-sm">No spark hosts registered yet.</p>
      <p class="mt-1 text-xs text-ink-faint">
        Devices appear here as mDNS discovers <span class="font-mono">_spark-monitor._tcp</span> on the network.
      </p>
    </div>
  {:else}
    <div class="grid gap-5 md:grid-cols-2">
      {#each devices as device (device.fullname)}
        <DeviceCard {device} />
      {/each}
    </div>
  {/if}
</div>
