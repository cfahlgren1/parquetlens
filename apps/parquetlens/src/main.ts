#!/usr/bin/env node
import {
  ParquetReadOptions,
  readParquetTableFromPath,
  readParquetTableFromStdin,
  readParquetTableFromUrl,
  resolveParquetUrl,
  runSqlOnParquet,
  runSqlOnParquetFromStdin,
} from "@parquetlens/parquet-reader";
import type { Table } from "apache-arrow";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath as nodeFileURLToPath } from "node:url";

// __filename is provided by tsup banner in production.
// In dev mode (tsx/ESM), we derive it from import.meta.url.
// Use a different name to avoid duplicate declaration with banner.
const __parquetlens_filename =
  typeof __filename !== "undefined"
    ? __filename
    : nodeFileURLToPath(import.meta.url);

declare const __filename: string;

type TuiMode = "auto" | "on" | "off";

type Options = {
  limit: number;
  columns: string[];
  json: boolean;
  schemaOnly: boolean;
  showSchema: boolean;
  tuiMode: TuiMode;
  sql?: string;
};

type ParsedArgs = {
  input?: string;
  options: Options;
  limitSpecified: boolean;
  help: boolean;
  error?: string;
};

const DEFAULT_LIMIT = 20;

function parseArgs(argv: string[]): ParsedArgs {
  const options: Options = {
    limit: DEFAULT_LIMIT,
    columns: [],
    json: false,
    schemaOnly: false,
    showSchema: true,
    tuiMode: "auto",
  };

  let input: string | undefined;
  let limitSpecified = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--") {
      continue;
    }

    if (arg === "-h" || arg === "--help") {
      return { options, limitSpecified, help: true };
    }

    if (arg === "--json") {
      options.json = true;
      options.tuiMode = "off";
      continue;
    }

    if (arg === "--tui") {
      options.tuiMode = "on";
      continue;
    }

    if (arg === "--plain" || arg === "--no-tui") {
      options.tuiMode = "off";
      continue;
    }

    if (arg === "--schema") {
      options.schemaOnly = true;
      options.tuiMode = "off";
      continue;
    }

    if (arg === "--no-schema") {
      options.showSchema = false;
      continue;
    }

    const limitValue = readOptionValue(arg, "--limit", argv[i + 1]);
    if (limitValue) {
      const parsed = Number.parseInt(limitValue.value, 10);
      if (!Number.isFinite(parsed) || parsed < 0) {
        return {
          options,
          limitSpecified,
          help: false,
          error: `invalid --limit value: ${limitValue.value}`,
        };
      }
      options.limit = parsed;
      limitSpecified = true;
      if (limitValue.usedNext) {
        i += 1;
      }
      continue;
    }

    const columnsValue = readOptionValue(arg, "--columns", argv[i + 1]);
    if (columnsValue) {
      const rawColumns = columnsValue.value
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      options.columns = rawColumns;
      if (columnsValue.usedNext) {
        i += 1;
      }
      continue;
    }

    const sqlValue = readOptionValue(arg, "--sql", argv[i + 1]);
    if (sqlValue) {
      options.sql = sqlValue.value;
      if (sqlValue.usedNext) {
        i += 1;
      }
      continue;
    }

    if (arg === "-") {
      if (input) {
        return { options, limitSpecified, help: false, error: "unexpected extra argument: -" };
      }
      input = arg;
      continue;
    }

    if (arg.startsWith("-")) {
      return { options, limitSpecified, help: false, error: `unknown option: ${arg}` };
    }

    if (input) {
      return { options, limitSpecified, help: false, error: `unexpected extra argument: ${arg}` };
    }

    input = arg;
  }

  return { input, options, limitSpecified, help: false };
}

function readOptionValue(
  arg: string,
  name: "--limit" | "--columns" | "--sql",
  next?: string,
): { value: string; usedNext: boolean } | null {
  if (arg === name) {
    if (!next || next.startsWith("-")) {
      return null;
    }
    return { value: next, usedNext: true };
  }

  if (arg.startsWith(`${name}=`)) {
    return { value: arg.slice(name.length + 1), usedNext: false };
  }

  return null;
}

function printUsage(): void {
  const helpText = `parquetlens <file|url|-> [options]

options:
  --limit, --limit=<n>       number of rows to show (default: ${DEFAULT_LIMIT})
  --columns, --columns=<c>   comma-separated column list
  --sql, --sql=<query>       run SQL query (use 'data' as table name)
  --schema                   print schema only
  --no-schema                skip schema output
  --json                     output rows as json lines
  --tui                      open interactive viewer (default)
  --plain, --no-tui          disable interactive viewer
  -h, --help                 show help

examples:
  parquetlens data.parquet --limit 25
  parquetlens data.parquet --columns=city,state
  parquetlens data.parquet --sql "SELECT city, COUNT(*) FROM data GROUP BY city"
  parquetlens hf://datasets/cfahlgren1/hub-stats/daily_papers.parquet
  parquetlens https://huggingface.co/datasets/cfahlgren1/hub-stats/resolve/main/daily_papers.parquet
  parquetlens data.parquet --plain
  parquetlens - < input.parquet
`;

  process.stdout.write(helpText);
}

function formatSchema(table: Table): string {
  const lines = table.schema.fields.map((field, index) => {
    const typeName = String(field.type);
    return `${index + 1}. ${field.name}: ${typeName}`;
  });

  return lines.join("\n");
}

function totalRows(table: Table): number {
  return table.batches.reduce((count, batch) => count + batch.numRows, 0);
}

function resolveColumns(table: Table, requested: string[]): { names: string[]; indices: number[] } {
  const fields = table.schema.fields;
  const nameToIndex = new Map<string, number>();

  fields.forEach((field, index) => {
    nameToIndex.set(field.name, index);
  });

  const names = requested.length > 0 ? requested : fields.map((field) => field.name);
  const missing = names.filter((name) => !nameToIndex.has(name));

  if (missing.length > 0) {
    throw new Error(`unknown columns: ${missing.join(", ")}`);
  }

  return {
    names,
    indices: names.map((name) => nameToIndex.get(name) ?? -1),
  };
}

const MAX_COL_WIDTH = 40;

function printTable(rows: Record<string, unknown>[]): void {
  if (rows.length === 0) {
    process.stdout.write("(no rows)\n");
    return;
  }

  const columns = Object.keys(rows[0]);
  const widths = columns.map((col) => {
    const maxVal = rows.reduce((max, row) => {
      const val = String(row[col] ?? "");
      return Math.max(max, val.length);
    }, col.length);
    return Math.min(maxVal, MAX_COL_WIDTH);
  });

  const pad = (str: string, width: number) => {
    if (str.length > width) {
      return str.slice(0, width - 3) + "...";
    }
    return str.padEnd(width);
  };

  const header = columns.map((col, i) => pad(col, widths[i])).join("  ");
  const separator = widths.map((w) => "-".repeat(w)).join("  ");

  process.stdout.write(`${header}\n${separator}\n`);

  for (const row of rows) {
    const line = columns.map((col, i) => pad(String(row[col] ?? ""), widths[i])).join("  ");
    process.stdout.write(`${line}\n`);
  }
}

function formatCell(value: unknown): unknown {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof Uint8Array) {
    return `Uint8Array(${value.length})`;
  }

  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return value;
}

function previewRows(
  table: Table,
  limit: number,
  requestedColumns: string[],
): Record<string, unknown>[] {
  const { names, indices } = resolveColumns(table, requestedColumns);
  const rows: Record<string, unknown>[] = [];

  for (const batch of table.batches) {
    const vectors = indices.map((index) => batch.getChildAt(index));

    for (let rowIndex = 0; rowIndex < batch.numRows; rowIndex += 1) {
      if (rows.length >= limit) {
        return rows;
      }

      const row: Record<string, unknown> = {};

      for (let colIndex = 0; colIndex < names.length; colIndex += 1) {
        const vector = vectors[colIndex];
        row[names[colIndex]] = formatCell(vector?.get(rowIndex));
      }

      rows.push(row);
    }
  }

  return rows;
}

async function loadTable(
  input: string | undefined,
  readOptions: ParquetReadOptions,
): Promise<Table> {
  const stdinFallback = process.stdin.isTTY ? undefined : "-";
  const source = input ?? stdinFallback;

  if (!source) {
    throw new Error("missing input file (pass a path, URL, or pipe stdin)");
  }

  if (source === "-") {
    return readParquetTableFromStdin("stdin.parquet", readOptions);
  }

  if (resolveParquetUrl(source)) {
    return readParquetTableFromUrl(source, readOptions);
  }

  return readParquetTableFromPath(source, readOptions);
}

async function main(): Promise<void> {
  const { input, options, limitSpecified, help, error } = parseArgs(process.argv.slice(2));

  if (help) {
    printUsage();
    return;
  }

  if (error) {
    process.stderr.write(`parquetlens: ${error}\n`);
    printUsage();
    process.exitCode = 1;
    return;
  }

  // Handle SQL mode
  if (options.sql) {
    const stdinFallback = process.stdin.isTTY ? undefined : "-";
    const source = input ?? stdinFallback;

    if (!source) {
      process.stderr.write("parquetlens: missing input file for SQL query\n");
      process.exitCode = 1;
      return;
    }

    const table =
      source === "-"
        ? await runSqlOnParquetFromStdin(options.sql)
        : await runSqlOnParquet(source, options.sql);

    // Use TUI for SQL results if not in JSON/plain mode
    const wantsSqlTui =
      !options.json && options.tuiMode !== "off" && source !== "-" && process.stdin.isTTY && process.stdout.isTTY;

    if (wantsSqlTui) {
      if (isBunRuntime()) {
        const { runTuiWithTable } = await importTuiModule();
        const title = `SQL: ${options.sql.slice(0, 50)}${options.sql.length > 50 ? "..." : ""}`;
        await runTuiWithTable(table, title, { columns: [], maxRows: options.limit });
        return;
      }
      // Spawn bun to re-run the query and display in TUI
      const spawned = spawnBun(process.argv.slice(1));
      if (spawned) {
        return;
      }
      process.stderr.write("parquetlens: bun not found, falling back to plain output\n");
    }

    const rows = previewRows(table, options.limit, []);

    if (options.json) {
      for (const row of rows) {
        process.stdout.write(`${JSON.stringify(row)}\n`);
      }
    } else {
      printTable(rows);
    }
    return;
  }

  const wantsTui = resolveTuiMode(options.tuiMode, options);
  if (wantsTui) {
    if (!input || input === "-") {
      process.stderr.write(
        "parquetlens: tui mode requires a file path or URL (stdin not supported)\n",
      );
      process.exitCode = 1;
      return;
    }

    if (!process.stdin.isTTY || !process.stdout.isTTY) {
      process.stderr.write("parquetlens: tui mode requires a tty, falling back to plain output\n");
    } else if (!isBunRuntime()) {
      const spawned = spawnBun(process.argv.slice(1));
      if (spawned) {
        return;
      }
      process.stderr.write("parquetlens: bun not found, falling back to plain output\n");
    } else {
      const { runTui } = await importTuiModule();
      const maxRows = limitSpecified ? options.limit : undefined;

      await runTui(input, { columns: options.columns, maxRows });
      return;
    }
  }

  const readOptions: ParquetReadOptions = {
    batchSize: 1024,
    columns: options.columns.length > 0 ? options.columns : undefined,
    limit: options.schemaOnly ? 0 : options.limit,
  };

  const table = await loadTable(input, readOptions);

  if (options.showSchema || options.schemaOnly) {
    const rowsCount = totalRows(table);
    const title = input ? path.basename(input) : "stdin";
    const limitSuffix = readOptions.limit ? ` (limit ${readOptions.limit})` : "";
    process.stdout.write(`file: ${title}\nrows loaded: ${rowsCount}${limitSuffix}\n`);
    process.stdout.write("schema:\n");
    process.stdout.write(`${formatSchema(table)}\n`);
  }

  if (options.schemaOnly) {
    return;
  }

  const rows = previewRows(table, options.limit, options.columns);

  if (options.json) {
    for (const row of rows) {
      process.stdout.write(`${JSON.stringify(row)}\n`);
    }
    return;
  }

  printTable(rows);
}

function resolveTuiMode(mode: TuiMode, options: Options): boolean {
  if (mode === "on") {
    if (options.json || options.schemaOnly) {
      return false;
    }
    return true;
  }

  if (mode === "off") {
    return false;
  }

  return !options.json && !options.schemaOnly;
}

function isBunRuntime(): boolean {
  return !!process.versions.bun;
}

function spawnBun(argv: string[]): boolean {
  const bunCheck = spawnSync("bun", ["--version"], { stdio: "ignore" });
  if (bunCheck.error || bunCheck.status !== 0) {
    return false;
  }

  const result = spawnSync("bun", [__parquetlens_filename, ...argv.slice(1)], {
    stdio: "inherit",
    env: { ...process.env, PARQUETLENS_BUN: "1" },
  });

  process.exitCode = result.status ?? 1;
  return true;
}

async function importTuiModule(): Promise<typeof import("./tui.js")> {
  const extension = path.extname(__parquetlens_filename);
  const modulePath = extension === ".js" ? "./tui.js" : "./tui.tsx";
  return import(modulePath);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`parquetlens: ${message}\n`);
  process.exitCode = 1;
});
