import { readFileSync, rmSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { Buffer } from "node:buffer";
import { createRequire as nodeCreateRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";

import type { Table } from "apache-arrow";
import {
  DuckDBAccessMode,
  DuckDBBundles,
  DuckDBConnection,
  DuckDBDataProtocol,
  DuckDBBindings,
  FileFlags,
  NODE_RUNTIME,
  VoidLogger,
  createDuckDB,
  failWith,
  readString,
} from "@duckdb/duckdb-wasm/blocking";
import type { DuckDBModule } from "@duckdb/duckdb-wasm/blocking";
import {
  bufferStdinToTempFile,
  resolveParquetUrl,
  type ParquetRow,
  type TempParquetFile,
} from "@parquetlens/parquet-reader";

type SqlParquetSource = {
  runSql: (query: string) => Promise<ParquetRow[]>;
  close: () => Promise<void>;
};

let duckDbPromise: Promise<DuckDBBindings> | null = null;
let httpRuntimePatched = false;

type HttpBuffer = {
  dataPtr: number;
  size: number;
};

const httpBuffers = new Map<number, HttpBuffer>();

type WasmModule = DuckDBModule & {
  HEAPU8: Uint8Array;
  HEAPF64: Float64Array;
  _malloc: (size: number) => number;
  _free: (ptr: number) => void;
};

export async function runSqlOnParquet(input: string, query: string): Promise<ParquetRow[]> {
  const source = await openParquetSource(input);

  try {
    return await source.runSql(query);
  } finally {
    await source.close();
  }
}

export async function runSqlOnParquetFromStdin(
  query: string,
  filenameHint = "stdin.parquet",
): Promise<ParquetRow[]> {
  const temp = await bufferStdinToTempFile(filenameHint);
  try {
    return await runSqlOnParquet(temp.path, query);
  } finally {
    await temp.cleanup();
  }
}

async function openParquetSource(input: string): Promise<SqlParquetSource> {
  const resolved = resolveParquetUrl(input);
  if (resolved) {
    return openParquetSourceFromUrl(resolved.url);
  }

  return openParquetSourceFromPath(input);
}

async function openParquetSourceFromPath(filePath: string): Promise<SqlParquetSource> {
  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName(filePath);

  db.registerFileURL(fileName, filePath, DuckDBDataProtocol.NODE_FS, true);

  return createParquetSource(db, conn, fileName);
}

async function openParquetSourceFromUrl(url: string): Promise<SqlParquetSource> {
  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName(url);

  db.registerFileURL(fileName, url, DuckDBDataProtocol.HTTP, true);

  return createParquetSource(db, conn, fileName);
}

async function getDuckDb(): Promise<DuckDBBindings> {
  if (!duckDbPromise) {
    duckDbPromise = (async () => {
      ensureHttpRuntimeSupport();
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

function ensureHttpRuntimeSupport(): void {
  if (httpRuntimePatched) {
    return;
  }
  httpRuntimePatched = true;

  const nodeOpenFile = NODE_RUNTIME.openFile as unknown as (
    mod: WasmModule,
    fileId: number,
    flags: FileFlags,
  ) => number;
  const nodeReadFile = NODE_RUNTIME.readFile as unknown as (
    mod: WasmModule,
    fileId: number,
    buffer: number,
    bytes: number,
    location: number,
  ) => number;
  const nodeCheckFile = NODE_RUNTIME.checkFile.bind(NODE_RUNTIME);
  const nodeGlob = NODE_RUNTIME.glob.bind(NODE_RUNTIME);
  const nodeCloseFile = NODE_RUNTIME.closeFile.bind(NODE_RUNTIME);
  const nodeGetLastModified = NODE_RUNTIME.getLastFileModificationTime.bind(NODE_RUNTIME);

  NODE_RUNTIME.openFile = (mod: WasmModule, fileId: number, flags: FileFlags): number => {
    const file = NODE_RUNTIME.resolveFileInfo(mod, fileId);
    if (!file || file.dataProtocol !== DuckDBDataProtocol.HTTP) {
      return nodeOpenFile(mod, fileId, flags);
    }

    if (flags & FileFlags.FILE_FLAGS_WRITE || flags & FileFlags.FILE_FLAGS_APPEND) {
      failWith(mod, `Opening file ${file.fileName} failed: HTTP writes are not supported`);
      return 0;
    }

    if (!(flags & FileFlags.FILE_FLAGS_READ)) {
      failWith(mod, `Opening file ${file.fileName} failed: unsupported file flags: ${flags}`);
      return 0;
    }

    if (!file.dataUrl) {
      failWith(mod, `Opening file ${file.fileName} failed: missing data URL`);
      return 0;
    }

    const allowFull = file.allowFullHttpReads ?? true;
    const forceFull = file.forceFullHttpReads ?? false;

    if (!forceFull) {
      try {
        const probe = requestHttpRange(file.dataUrl, 0, 0);
        if (probe.status === 206) {
          const total =
            parseContentRangeTotal(probe.headers["content-range"]) ??
            parseContentLength(probe.headers["content-length"]);
          if (total !== null) {
            return buildOpenResult(mod, total, 0);
          }
        }

        if (probe.status === 200 && allowFull) {
          const dataPtr = writeResponseToHeap(mod, probe.bytes);
          httpBuffers.set(fileId, { dataPtr, size: probe.bytes.length });
          return buildOpenResult(mod, probe.bytes.length, dataPtr);
        }
      } catch (error) {
        if (!allowFull) {
          failWith(mod, `Opening file ${file.fileName} failed: ${String(error)}`);
          return 0;
        }
      }
    }

    if (allowFull) {
      try {
        const full = requestHttp(file.dataUrl);
        if (full.status === 200) {
          const dataPtr = writeResponseToHeap(mod, full.bytes);
          httpBuffers.set(fileId, { dataPtr, size: full.bytes.length });
          return buildOpenResult(mod, full.bytes.length, dataPtr);
        }
      } catch (error) {
        failWith(mod, `Opening file ${file.fileName} failed: ${String(error)}`);
        return 0;
      }
    }

    failWith(mod, `Opening file ${file.fileName} failed: HTTP range requests unavailable`);
    return 0;
  };

  NODE_RUNTIME.readFile = (
    mod: WasmModule,
    fileId: number,
    buffer: number,
    bytes: number,
    location: number,
  ): number => {
    if (bytes === 0) {
      return 0;
    }

    const file = NODE_RUNTIME.resolveFileInfo(mod, fileId);
    if (!file || file.dataProtocol !== DuckDBDataProtocol.HTTP) {
      return nodeReadFile(mod, fileId, buffer, bytes, location);
    }

    const cached = httpBuffers.get(fileId);
    if (cached) {
      const sliceStart = Math.max(0, location);
      const sliceEnd = Math.min(cached.size, location + bytes);
      const length = Math.max(0, sliceEnd - sliceStart);
      if (length > 0) {
        const src = mod.HEAPU8.subarray(cached.dataPtr + sliceStart, cached.dataPtr + sliceEnd);
        mod.HEAPU8.set(src, buffer);
      }
      return length;
    }

    if (!file.dataUrl) {
      failWith(mod, `Reading file ${file.fileName} failed: missing data URL`);
      return 0;
    }

    try {
      const response = requestHttpRange(file.dataUrl, location, location + bytes - 1);
      if (response.status === 206 || (response.status === 200 && location === 0)) {
        const length = Math.min(bytes, response.bytes.length);
        if (length > 0) {
          mod.HEAPU8.set(response.bytes.subarray(0, length), buffer);
        }
        return length;
      }

      failWith(mod, `Reading file ${file.fileName} failed with HTTP ${response.status}`);
      return 0;
    } catch (error) {
      failWith(mod, `Reading file ${file.fileName} failed: ${String(error)}`);
      return 0;
    }
  };

  NODE_RUNTIME.checkFile = (mod: DuckDBModule, pathPtr: number, pathLen: number): boolean => {
    const path = readString(mod, pathPtr, pathLen);
    if (isHttpUrl(path)) {
      const response = requestHttpHead(path);
      return response.status === 200 || response.status === 206;
    }
    return nodeCheckFile(mod, pathPtr, pathLen);
  };

  NODE_RUNTIME.glob = (mod: DuckDBModule, pathPtr: number, pathLen: number): void => {
    const path = readString(mod, pathPtr, pathLen);
    if (isHttpUrl(path)) {
      const response = requestHttpHead(path);
      if (response.status === 200 || response.status === 206) {
        mod.ccall("duckdb_web_fs_glob_add_path", null, ["string"], [path]);
      }
      return;
    }

    return nodeGlob(mod, pathPtr, pathLen);
  };

  NODE_RUNTIME.closeFile = (mod: DuckDBModule, fileId: number): void => {
    const cached = httpBuffers.get(fileId);
    if (cached) {
      if (cached.dataPtr) {
        (mod as WasmModule)._free(cached.dataPtr);
      }
      httpBuffers.delete(fileId);
    }
    nodeCloseFile(mod, fileId);
  };

  NODE_RUNTIME.getLastFileModificationTime = (mod: DuckDBModule, fileId: number): number => {
    const file = NODE_RUNTIME.resolveFileInfo(mod, fileId);
    if (file?.dataProtocol === DuckDBDataProtocol.HTTP) {
      return Date.now() / 1000;
    }
    return nodeGetLastModified(mod, fileId);
  };
}

function isHttpUrl(value: string): boolean {
  return value.startsWith("http://") || value.startsWith("https://");
}

function buildOpenResult(mod: WasmModule, size: number, dataPtr: number): number {
  const result = mod._malloc(2 * 8);
  mod.HEAPF64[(result >> 3) + 0] = +size;
  mod.HEAPF64[(result >> 3) + 1] = dataPtr;
  return result;
}

function writeResponseToHeap(mod: WasmModule, bytes: Uint8Array): number {
  const dataPtr = mod._malloc(bytes.byteLength);
  mod.HEAPU8.set(bytes, dataPtr);
  return dataPtr;
}

function parseContentRangeTotal(contentRange: string | null): number | null {
  if (!contentRange) {
    return null;
  }
  const [, total] = contentRange.split("/");
  if (!total) {
    return null;
  }
  const parsed = Number.parseInt(total, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseContentLength(contentLength: string | null): number | null {
  if (!contentLength) {
    return null;
  }
  const parsed = Number.parseInt(contentLength, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

type HttpResponse = {
  status: number;
  bytes: Uint8Array;
  headers: Record<string, string>;
};

function requestHttp(url: string): HttpResponse {
  return requestCurl([url]);
}

function requestHttpHead(url: string): HttpResponse {
  return requestCurl(["-I", url]);
}

function requestHttpRange(url: string, start: number, end: number): HttpResponse {
  return requestCurl(["-r", `${start}-${end}`, url]);
}

function requestCurl(args: string[]): HttpResponse {
  const tempPath = path.join(tmpdir(), `parquetlens-http-${randomUUID()}`);
  try {
    const result = spawnSync("curl", ["-sS", "-L", "-D", "-", "-o", tempPath, ...args], {
      encoding: "buffer",
      maxBuffer: 4 * 1024 * 1024,
    });

    if (result.error) {
      if ((result.error as NodeJS.ErrnoException).code === "ENOENT") {
        throw new Error("curl not found (required for HTTP range reads)");
      }
      throw result.error;
    }
    if (result.status !== 0) {
      const stderr = result.stderr?.toString("utf8").trim();
      throw new Error(stderr || "curl failed");
    }

    const body = readFileSync(tempPath);
    return parseCurlResponse(Buffer.from(result.stdout ?? []), body);
  } finally {
    rmSync(tempPath, { force: true });
  }
}

function parseCurlResponse(headersBuffer: Buffer, body: Buffer): HttpResponse {
  const headerBlob = headersBuffer.toString("latin1");
  const blocks = headerBlob.split(/\r\n\r\n/).filter(Boolean);
  const lastBlock = blocks[blocks.length - 1] ?? "";
  const lines = lastBlock.split(/\r\n/).filter(Boolean);
  const statusLine = lines.shift() ?? "";
  const statusToken = statusLine.split(" ")[1] ?? "";
  const status = Number.parseInt(statusToken, 10);
  const headers: Record<string, string> = {};

  for (const line of lines) {
    const index = line.indexOf(":");
    if (index === -1) {
      continue;
    }
    const key = line.slice(0, index).trim().toLowerCase();
    const value = line.slice(index + 1).trim();
    headers[key] = value;
  }

  return {
    status: Number.isFinite(status) ? status : 0,
    bytes: new Uint8Array(body),
    headers,
  };
}

function createParquetSource(
  db: DuckDBBindings,
  conn: DuckDBConnection,
  fileName: string,
): SqlParquetSource {
  let viewCreated = false;

  const ensureDataView = () => {
    if (!viewCreated) {
      conn.query(
        `CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet(${quoteLiteral(fileName)})`,
      );
      viewCreated = true;
    }
  };

  return {
    runSql: async (query: string) => {
      ensureDataView();
      return tableToObjects(conn.query(query));
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

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
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
