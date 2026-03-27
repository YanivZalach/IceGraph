import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: '/',
  // In dev mode, forward Flask API routes to the running Flask server.
  // The React dev server runs on :5173; Flask runs on :5000.
  server: {
    proxy: {
      '/generate': 'http://localhost:5000',
      '/lib':      'http://localhost:5000',
    },
  },
  build: {
    outDir: '../static/react',
    emptyOutDir: true,
  },
})
