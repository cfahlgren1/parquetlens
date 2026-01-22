import { createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

import { tableFromIPC, Table } from "apache-arrow";
import { readParquet, ReaderOptions } from "parquet-wasm";

export type TempParquetFile = {
  path: string;
  cleanup: () => Promise<void>;
};

export type ParquetReadOptions = Pick<
  ReaderOptions,
  "batchSize" | "columns" | "limit" | "offset" | "rowGroups"
>;

export type ParquetBufferSource = {
  buffer: Uint8Array;
  byteLength: number;
  readTable: (options?: ParquetReadOptions) => Table;
};

export function readParquetTableFromBuffer(
  buffer: Uint8Array,
  options?: ParquetReadOptions,
): Table {
  const wasmTable = readParquet(buffer, options ?? undefined);
  const ipcStream = wasmTable.intoIPCStream();
  return tableFromIPC(ipcStream);
}

export function createParquetBufferSource(buffer: Uint8Array): ParquetBufferSource {
  return {
    buffer,
    byteLength: buffer.byteLength,
    readTable: (options?: ParquetReadOptions) => readParquetTableFromBuffer(buffer, options),
  };
}

export async function openParquetBufferFromPath(filePath: string): Promise<ParquetBufferSource> {
  const buffer = await fs.readFile(filePath);
  return createParquetBufferSource(buffer);
}

export async function readParquetTableFromPath(
  filePath: string,
  options?: ParquetReadOptions,
): Promise<Table> {
  const buffer = await fs.readFile(filePath);
  return readParquetTableFromBuffer(buffer, options);
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
