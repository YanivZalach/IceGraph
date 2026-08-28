import { defineConfig, loadEnv } from "vite";
import mdx from "@mdx-js/rollup";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { tanstackRouter } from "@tanstack/router-plugin/vite";

// @mdx-js/rollup strips the query before filtering, so it would also compile
// `.mdx?raw` imports (used as the docs search source) into components.
const mdxSkippingRawImports = () => {
  const plugin = mdx();

  return {
    ...plugin,
    enforce: "pre" as const,
    transform: (value: string, id: string) =>
      /[?&]raw(?:&|$)/.test(id) ? undefined : plugin.transform(value, id),
  };
};

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  return {
    plugins: [
      // Must precede react() so route-file transforms run before Babel:
      // https://tanstack.com/router/latest/docs/framework/react/routing/installation-with-vite
      tanstackRouter({ target: "react", autoCodeSplitting: false }),
      mdxSkippingRawImports(),
      react({
        include: /\.(js|jsx|mdx|ts|tsx)$/,
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
