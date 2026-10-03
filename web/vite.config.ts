import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In development the API is proxied to a locally running dlq-triage.
export default defineConfig({
  plugins: [react()],
  build: { outDir: "dist", emptyOutDir: true },
  server: { proxy: { "/graphql": "http://127.0.0.1:8099" } },
});
