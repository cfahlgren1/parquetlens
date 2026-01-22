import { createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { createRequire as nodeCreateRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

import type { Table } from "apache-arrow";
import {
  DuckDBAccessMode,
  DuckDBBundles,
  DuckDBConnection,
  DuckDBDataProtocol,
  DuckDBBindings,
  NODE_RUNTIME,
  VoidLogger,
  createDuckDB,
} from "@duckdb/duckdb-wasm/blocking";

import { resolveParquetUrl } from "./urls.js";

export type TempParquetFile = {
  path: string;
  cleanup: () => Promise<void>;
};

export type ParquetReadOptions = {
  batchSize?: number;
  columns?: string[];
  limit?: number;
  offset?: number;
  rowGroups?: number[];
};

export type ParquetFileMetadata = {
  createdBy?: string;
  keyValueMetadata: Record<string, string>;
};

export type ParquetSource = {
  readTable: (options?: ParquetReadOptions) => Promise<Table>;
  readMetadata: () => Promise<ParquetFileMetadata>;
  close: () => Promise<void>;
};

let duckDbPromise: Promise<DuckDBBindings> | null = null;

async function getDuckDb(): Promise<DuckDBBindings> {
  if (!duckDbPromise) {
    duckDbPromise = (async () => {
      const bundles = getDuckDbBundles();
      const db = await createDuckDB(bundles, new VoidLogger(), NODE_RUNTIME);
      await db.instantiate();
      db.open({
        accessMode: DuckDBAccessMode.READ_WRITE,
        filesystem: {
          allowFullHTTPReads: true,
        },
      });
      return db;
    })();
  }

  return duckDbPromise;
}

function getDuckDbBundles(): DuckDBBundles {
  const localRequire = nodeCreateRequire(import.meta.url);
  const mvpModule = localRequire.resolve("@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm");
  const mvpWorker = localRequire.resolve("@duckdb/duckdb-wasm/dist/duckdb-node-mvp.worker.cjs");
  const ehModule = localRequire.resolve("@duckdb/duckdb-wasm/dist/duckdb-eh.wasm");
  const ehWorker = localRequire.resolve("@duckdb/duckdb-wasm/dist/duckdb-node-eh.worker.cjs");

  return {
    mvp: {
      mainModule: mvpModule,
      mainWorker: mvpWorker,
    },
    eh: {
      mainModule: ehModule,
      mainWorker: ehWorker,
    },
  };
}

export async function openParquetSourceFromPath(filePath: string): Promise<ParquetSource> {
  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName(filePath);

  db.registerFileURL(fileName, filePath, DuckDBDataProtocol.NODE_FS, true);

  return createParquetSource(db, conn, fileName);
}

export async function openParquetSourceFromUrl(input: string): Promise<ParquetSource> {
  const resolved = resolveParquetUrl(input);
  if (!resolved) {
    throw new Error("Not a URL");
  }

  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName(resolved.url);

  db.registerFileURL(fileName, resolved.url, DuckDBDataProtocol.HTTP, true);

  return createParquetSource(db, conn, fileName);
}

export async function openParquetSourceFromBuffer(buffer: Uint8Array): Promise<ParquetSource> {
  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName("buffer");

  db.registerFileBuffer(fileName, buffer);

  return createParquetSource(db, conn, fileName);
}

export async function openParquetSource(input: string): Promise<ParquetSource> {
  const resolved = resolveParquetUrl(input);
  if (resolved) {
    return openParquetSourceFromUrl(input);
  }

  return openParquetSourceFromPath(input);
}

export async function readParquetTableFromBuffer(
  buffer: Uint8Array,
  options?: ParquetReadOptions,
): Promise<Table> {
  const source = await openParquetSourceFromBuffer(buffer);

  try {
    return await source.readTable(options);
  } finally {
    await source.close();
  }
}

export async function readParquetTableFromPath(
  filePath: string,
  options?: ParquetReadOptions,
): Promise<Table> {
  const source = await openParquetSourceFromPath(filePath);

  try {
    return await source.readTable(options);
  } finally {
    await source.close();
  }
}

export async function readParquetTableFromUrl(
  input: string,
  options?: ParquetReadOptions,
): Promise<Table> {
  const source = await openParquetSourceFromUrl(input);

  try {
    return await source.readTable(options);
  } finally {
    await source.close();
  }
}

export async function readParquetMetadataFromBuffer(
  buffer: Uint8Array,
): Promise<ParquetFileMetadata> {
  const source = await openParquetSourceFromBuffer(buffer);

  try {
    return await source.readMetadata();
  } finally {
    await source.close();
  }
}

export async function bufferStdinToTempFile(
  filenameHint = "stdin.parquet",
): Promise<TempParquetFile> {
  const tempDir = await fs.mkdtemp(path.join(tmpdir(), "parquetlens-"));
  const safeName = filenameHint.replace(/[\\/]/g, "_");
  const filePath = path.join(tempDir, `${randomUUID()}-${safeName}`);
  const writeStream = createWriteStream(filePath);

  await pipeline(process.stdin, writeStream);

  return {
    path: filePath,
    cleanup: async () => {
      await fs.rm(tempDir, { recursive: true, force: true });
    },
  };
}

export async function readParquetTableFromStdin(
  filenameHint = "stdin.parquet",
  options?: ParquetReadOptions,
): Promise<Table> {
  const temp = await bufferStdinToTempFile(filenameHint);

  try {
    return await readParquetTableFromPath(temp.path, options);
  } finally {
    await temp.cleanup();
  }
}

function createParquetSource(
  db: DuckDBBindings,
  conn: DuckDBConnection,
  fileName: string,
): ParquetSource {
  let metadataPromise: Promise<ParquetFileMetadata> | null = null;

  return {
    readTable: async (options?: ParquetReadOptions) => {
      const query = buildSelectQuery(fileName, options);
      return conn.query(query);
    },
    readMetadata: () => {
      if (!metadataPromise) {
        metadataPromise = readParquetMetadata(conn, fileName);
      }
      return metadataPromise;
    },
    close: async () => {
      conn.close();
      db.dropFile(fileName);
    },
  };
}

function buildDuckDbFileName(input: string): string {
  const suffix = path.extname(input) || ".parquet";
  return `parquetlens-${randomUUID()}${suffix}`;
}

function buildSelectQuery(fileName: string, options?: ParquetReadOptions): string {
  const columns = options?.columns && options.columns.length > 0 ? options.columns : null;
  const selectList = columns ? columns.map(quoteIdentifier).join(", ") : "*";
  const limit = options?.limit;
  const offset = options?.offset;

  let query = `select ${selectList} from read_parquet(${quoteLiteral(fileName)})`;

  if (typeof limit === "number") {
    query += ` limit ${Math.max(0, limit)}`;
  }

  if (typeof offset === "number" && offset > 0) {
    query += ` offset ${Math.max(0, offset)}`;
  }

  return query;
}

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

async function readParquetMetadata(
  conn: DuckDBConnection,
  fileName: string,
): Promise<ParquetFileMetadata> {
  const metadataRows = tableToObjects(
    conn.query(`select * from parquet_file_metadata(${quoteLiteral(fileName)})`),
  );
  const kvRows = tableToObjects(
    conn.query(`select * from parquet_kv_metadata(${quoteLiteral(fileName)})`),
  );

  const createdByRaw = metadataRows[0]?.created_by ?? metadataRows[0]?.createdBy ?? null;
  const keyValueMetadata: Record<string, unknown> = {};

  for (const row of kvRows) {
    const key = row.key ?? row.key_name ?? row.name;
    if (typeof key !== "string" || key.length === 0) {
      continue;
    }
    keyValueMetadata[key] = row.value ?? row.val ?? "";
  }

  return {
    createdBy: normalizeMetadataValue(createdByRaw),
    keyValueMetadata: normalizeMetadataValues(keyValueMetadata),
  };
}

function tableToObjects(table: Table): Record<string, unknown>[] {
  const fields = table.schema.fields.map((field) => field.name);
  const rows: Record<string, unknown>[] = [];

  for (const batch of table.batches) {
    const vectors = fields.map((_, index) => batch.getChildAt(index));

    for (let rowIndex = 0; rowIndex < batch.numRows; rowIndex += 1) {
      const row: Record<string, unknown> = {};

      for (let colIndex = 0; colIndex < fields.length; colIndex += 1) {
        row[fields[colIndex]] = vectors[colIndex]?.get(rowIndex);
      }

      rows.push(row);
    }
  }

  return rows;
}

function normalizeMetadataValue(value: unknown): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString("utf8");
  }

  if (typeof value === "string") {
    return value;
  }

  return String(value);
}

function normalizeMetadataValues(input: Record<string, unknown>): Record<string, string> {
  const normalized: Record<string, string> = {};

  for (const [key, value] of Object.entries(input)) {
    const normalizedValue = normalizeMetadataValue(value);
    normalized[key] = normalizedValue ?? "";
  }

  return normalized;
}

export { resolveParquetUrl } from "./urls.js";
export type { ResolvedParquetUrl } from "./urls.js";
