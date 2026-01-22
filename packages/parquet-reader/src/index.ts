import { Blob as NodeBlob } from "node:buffer";
import { createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

import { tableFromIPC, Table } from "apache-arrow";
import { ParquetFile, readParquet, ReaderOptions } from "parquet-wasm";

const BlobCtor: typeof Blob =
  typeof Blob === "undefined" ? (NodeBlob as unknown as typeof Blob) : Blob;

export type TempParquetFile = {
  path: string;
  cleanup: () => Promise<void>;
};

export type ParquetReadOptions = Pick<
  ReaderOptions,
  "batchSize" | "columns" | "limit" | "offset" | "rowGroups"
>;

export type ParquetFileMetadata = {
  createdBy?: string;
  keyValueMetadata: Record<string, string>;
};

export type ParquetBufferSource = {
  buffer: Uint8Array;
  byteLength: number;
  readTable: (options?: ParquetReadOptions) => Table;
  readMetadata: () => Promise<ParquetFileMetadata>;
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
  let metadataPromise: Promise<ParquetFileMetadata> | null = null;

  return {
    buffer,
    byteLength: buffer.byteLength,
    readTable: (options?: ParquetReadOptions) => readParquetTableFromBuffer(buffer, options),
    readMetadata: () => {
      if (!metadataPromise) {
        metadataPromise = readParquetMetadataFromBuffer(buffer);
      }
      return metadataPromise;
    },
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

export async function readParquetMetadataFromBuffer(
  buffer: Uint8Array,
): Promise<ParquetFileMetadata> {
  const blobInput = new Uint8Array(buffer).buffer as ArrayBuffer;
  const file = await ParquetFile.fromFile(new BlobCtor([blobInput]));
  const meta = file.metadata();
  const fileMeta = meta.fileMetadata();
  const createdBy = fileMeta.createdBy();
  const keyValueMetadata = Object.fromEntries(fileMeta.keyValueMetadata());

  fileMeta.free();
  meta.free();
  file.free();

  return {
    createdBy: createdBy ?? undefined,
    keyValueMetadata: normalizeMetadataValues(keyValueMetadata),
  };
}

function normalizeMetadataValues(input: Record<string, unknown>): Record<string, string> {
  const normalized: Record<string, string> = {};

  for (const [key, value] of Object.entries(input)) {
    if (value === null || value === undefined) {
      normalized[key] = "";
      continue;
    }
    normalized[key] = typeof value === "string" ? value : String(value);
  }

  return normalized;
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
