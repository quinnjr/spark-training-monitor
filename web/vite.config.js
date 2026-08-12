import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';

// The dev server proxies /api to the spark-monitor-client REST API so the
// dashboard needs no CORS or base-URL configuration in development.
export default defineConfig({
  plugins: [svelte(), tailwindcss()],
  server: {
    proxy: {
      '/api': 'http://127.0.0.1:7881',
    },
  },
});
