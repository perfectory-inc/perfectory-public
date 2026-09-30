import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The Dawneer server serves the built screens from `dist` on its own origin (root ADR-0116), so
// session cookies and the CSRF origin check see one origin. There is no dev server proxy: during
// development run `vite build --watch` next to the server.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false },
  test: { environment: "node" },
});
