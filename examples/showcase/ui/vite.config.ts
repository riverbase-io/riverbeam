// The showcase UI. `npm run dev` serves it on :5174 with the API proxied from
// SHOWCASE_ADDR (default 127.0.0.1:8097); `npm run build` writes one
// self-contained dist/index.html, which showcase-server serves at /showcase.
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

const api = `http://${process.env.SHOWCASE_ADDR ?? "127.0.0.1:8097"}`;

export default defineConfig({
  plugins: [react(), viteSingleFile()],
  server: {
    port: 5174,
    proxy: { "/api": { target: api, changeOrigin: true }, "/ws": { target: api, ws: true } },
  },
  build: { outDir: "dist", emptyOutDir: true },
});
