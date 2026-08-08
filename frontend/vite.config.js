import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  return {
    plugins: [react(), tailwindcss()],
    base: process.env.VITE_BASE ?? '/',
    server: {
      host: true,
      port: 3000,
      allowedHosts: [
        'host.docker.internal',
        ...(env.VITE_DEV_ALLOWED_HOSTS?.split(',').map((host) => host.trim()) ?? []),
      ],
      proxy: {
        '/api': 'http://localhost:5050',
      },
    },
    build: {
      outDir: process.env.VITE_OUT_DIR ?? '/dist',
      emptyOutDir: true,
    },
  }
})
