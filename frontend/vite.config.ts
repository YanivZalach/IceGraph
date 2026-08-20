import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  return {
    plugins: [
      // Must precede react() so route-file transforms run before Babel:
      // https://tanstack.com/router/latest/docs/framework/react/routing/installation-with-vite
      tanstackRouter({ target: "react", autoCodeSplitting: false }),
      react({
        babel: {
          plugins: ["babel-plugin-react-compiler"],
        },
      }),
      tailwindcss(),
    ],
    base: process.env.VITE_BASE ?? "/",
    server: {
      host: true,
      port: 3000,
      allowedHosts: [
        "host.docker.internal",
        ...(env.VITE_DEV_ALLOWED_HOSTS?.split(",").map((host) => host.trim()) ??
          []),
      ],
      proxy: {
        "/api": "http://localhost:5050",
      },
    },
    build: {
      target: "chrome110",
      outDir: process.env.VITE_OUT_DIR ?? "/dist",
      emptyOutDir: true,
    },
  };
});
