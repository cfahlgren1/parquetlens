import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/main.ts", "src/tui.tsx"],
  format: ["esm"],
  sourcemap: true,
  banner: {
    js: `import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
const require = createRequire(import.meta.url);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);`,
  },
  external: [
    "@opentui/core",
    "@opentui/react",
    "react",
    "apache-arrow",
    "parquet-wasm/esm",
  ],
  noExternal: ["@parquetlens/parquet-reader"],
});
