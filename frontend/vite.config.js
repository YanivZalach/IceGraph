import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: process.env.VITE_BASE ?? '/',
  server: {
    host: true,
    port: 3000,
    allowedHosts: ['host.docker.internal'],
    proxy: {
      '/api': 'http://localhost:5050',
    },
  },
  build: {
    outDir: process.env.VITE_OUT_DIR ?? '/dist',
    emptyOutDir: true,
  },
})
