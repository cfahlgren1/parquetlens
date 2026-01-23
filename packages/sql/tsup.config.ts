import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm"],
  dts: true,
  sourcemap: true,
  external: ["@duckdb/duckdb-wasm", "apache-arrow"],
  noExternal: ["@parquetlens/parquet-reader"],
});
