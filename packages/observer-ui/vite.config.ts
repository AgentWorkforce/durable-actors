import tailwindcss from "@tailwindcss/postcss"
import { defineConfig } from "vite"

import packageJson from "./package.json" with { type: "json" }

export default defineConfig(({ mode }) => ({
    base: "./",
    css: { postcss: { plugins: [tailwindcss()] } },
    server: { host: "127.0.0.1", proxy: { "/api/observe": { target: process.env.OBSERVER_API_URL ?? "http://127.0.0.1:4174", changeOrigin: true } } },
    build:
        mode === "library"
            ? {
                  outDir: "dist",
                  emptyOutDir: true,
                  lib: { entry: { index: "src/index.ts", theme: "src/theme.css" }, formats: ["es"], fileName: (_format, name) => `${name}.js` },
                  cssCodeSplit: true,
                  rolldownOptions: {
                      external: id => Object.keys({ ...packageJson.dependencies, ...packageJson.peerDependencies }).some(name => id === name || id.startsWith(`${name}/`)),
                      output: { assetFileNames: "[name][extname]" }
                  }
              }
            : {
                  outDir: "dist/standalone",
                  license: { fileName: "THIRD-PARTY-LICENSES.md" },
                  emptyOutDir: true,
                  rolldownOptions: { output: { entryFileNames: "app.js", assetFileNames: "app.[ext]" } }
              }
}))
