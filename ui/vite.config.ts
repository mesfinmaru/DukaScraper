import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      // Pin to IPv4: Node resolves `localhost` to ::1 first, and a stale
      // wslrelay.exe can squat on [::1]:8000, causing 502s from this proxy.
      "/api": { target: "http://127.0.0.1:8000", ws: true },
      "/health": "http://127.0.0.1:8000",
      "/metrics": "http://127.0.0.1:8000",
    },
  },
})
