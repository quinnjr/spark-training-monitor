<script>
  /**
   * A recessed meter well with a thin phosphor-teal fill and a mono direct
   * label. `pct` may be null (renders an empty well with an em-dash).
   */
  let { label, pct, detail = '' } = $props();

  const clamped = $derived(pct == null ? 0 : Math.max(0, Math.min(100, pct)));
</script>

<div class="flex items-center gap-3">
  <span class="w-16 shrink-0 text-[11px] uppercase tracking-widest text-ink-faint">{label}</span>
  <div class="relative h-2 flex-1 overflow-hidden rounded-full bg-inset ring-1 ring-panel-edge">
    {#if pct != null}
      <div
        class="absolute inset-y-0 left-0 rounded-full bg-data transition-[width] duration-700"
        style="width: {clamped}%"
      ></div>
    {/if}
  </div>
  <span class="w-24 shrink-0 text-right font-mono text-xs text-ink">
    {pct == null ? '—' : `${Math.round(pct)}%`}{detail ? ` · ${detail}` : ''}
  </span>
</div>
