import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react-swc";
import path from "path";

const projectRoot = path.resolve(__dirname, "..");

export default defineConfig({
  root: projectRoot,
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: [path.resolve(projectRoot, "src/test/setup.ts")],
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
  },
  resolve: {
    alias: { "@": path.resolve(projectRoot, "src") },
  },
});
