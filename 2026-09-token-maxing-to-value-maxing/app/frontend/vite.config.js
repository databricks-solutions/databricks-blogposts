import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: { outDir: 'dist', chunkSizeWarningLimit: 1200 },
  server: {
    port: 3000,
    proxy: { '/api': { target: 'http://localhost:8000', changeOrigin: true } },
  },
})
