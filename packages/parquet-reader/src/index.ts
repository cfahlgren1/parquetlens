import { createWriteStream } from "node:fs";
import { promises as fs } from "node:fs";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import path from "node:path";
import { pipeline } from "node:stream/promises";

import type { AsyncBuffer } from "hyparquet";

import {
  asyncBufferFromHttpUrl,
  asyncBufferFromPath,
  asyncBufferFromStdin,
  buildParquetMetadata,
  getMetadata,
  getRawMetadata,
  readParquet,
} from "./reader.js";
import {
  readPage,
  streamRows,
  type PageOptions,
  type PageResult,
  type StreamOptions,
} from "./streaming.js";
import type { ParquetMetadata, ParquetReadOptions, ParquetRow } from "./types.js";
import { resolveParquetUrl } from "./urls.js";

export type TempParquetFile = {
  path: string;
  cleanup: () => Promise<void>;
};

export type ParquetSource = {
  readTable: (options?: ParquetReadOptions) => Promise<ParquetRow[]>;
  readMetadata: () => Promise<ParquetMetadata>;
  close: () => Promise<void>;
};

type ParquetFile = AsyncBuffer | ArrayBuffer;

export async function openParquetSourceFromPath(filePath: string): Promise<ParquetSource> {
  const file = await asyncBufferFromPath(filePath);
  return createParquetSource(file);
}

export async function openParquetSourceFromUrl(input: string): Promise<ParquetSource> {
  const resolved = resolveParquetUrl(input);
  if (!resolved) {
    throw new Error("Not a URL");
  }

  const file = await asyncBufferFromHttpUrl(resolved.url);
  return createParquetSource(file);
}

export async function openParquetSourceFromBuffer(buffer: Uint8Array): Promise<ParquetSource> {
  const arrayBuffer = buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength,
  ) as ArrayBuffer;
  return createParquetSource(arrayBuffer);
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
): Promise<ParquetRow[]> {
  const arrayBuffer = buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength,
  ) as ArrayBuffer;
  return readParquet(arrayBuffer, options);
}

export async function readParquetTableFromPath(
  filePath: string,
  options?: ParquetReadOptions,
): Promise<ParquetRow[]> {
  const file = await asyncBufferFromPath(filePath);
  return readParquet(file, options);
}

export async function readParquetTableFromUrl(
  input: string,
  options?: ParquetReadOptions,
): Promise<ParquetRow[]> {
  const resolved = resolveParquetUrl(input);
  if (!resolved) {
    throw new Error("Not a URL");
  }

  const file = await asyncBufferFromHttpUrl(resolved.url);
  return readParquet(file, options);
}

export async function readParquetMetadataFromBuffer(buffer: Uint8Array): Promise<ParquetMetadata> {
  const arrayBuffer = buffer.buffer.slice(
    buffer.byteOffset,
    buffer.byteOffset + buffer.byteLength,
  ) as ArrayBuffer;
  return getMetadata(arrayBuffer);
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
): Promise<ParquetRow[]> {
  const buffer = await asyncBufferFromStdin();
  return readParquet(buffer, options);
}

function createParquetSource(file: ParquetFile): ParquetSource {
  let rawMetadataPromise: ReturnType<typeof getRawMetadata> | null = null;
  let metadataPromise: Promise<ParquetMetadata> | null = null;
  const readRawMetadata = () => {
    if (!rawMetadataPromise) {
      rawMetadataPromise = getRawMetadata(file);
    }
    return rawMetadataPromise;
  };

  return {
    readTable: async (options?: ParquetReadOptions) => {
      return readParquet(file, options, await readRawMetadata());
    },
    readMetadata: () => {
      if (!metadataPromise) {
        metadataPromise = readRawMetadata().then((rawMetadata) => {
          return buildParquetMetadata(rawMetadata);
        });
      }
      return metadataPromise;
    },
    close: async () => {
      return Promise.resolve();
    },
  };
}

// Streaming API

async function resolveInputToFile(input: string): Promise<ParquetFile> {
  const resolved = resolveParquetUrl(input);
  if (resolved) {
    return asyncBufferFromHttpUrl(resolved.url);
  }
  return asyncBufferFromPath(input);
}

/**
 * Stream rows from a parquet file.
 *
 * By default, yields individual rows. Set `batchSize` to yield arrays of rows.
 *
 * @example
 * // Stream rows one at a time
 * for await (const row of streamParquet('data.parquet')) {
 *   console.log(row);
 * }
 *
 * @example
 * // Stream in batches
 * for await (const batch of streamParquet('large.parquet', { batchSize: 1000 })) {
 *   await processBatch(batch);
 * }
 *
 * @example
 * // Select columns
 * for await (const row of streamParquet('data.parquet', { columns: ['id', 'name'] })) {
 *   console.log(row.id, row.name);
 * }
 *
 * @example
 * // Cancellable streaming
 * const controller = new AbortController();
 * for await (const row of streamParquet('huge.parquet', { signal: controller.signal })) {
 *   if (found) controller.abort();
 * }
 */
export async function* streamParquet(
  input: string,
  options?: StreamOptions,
): AsyncGenerator<ParquetRow | ParquetRow[]> {
  const file = await resolveInputToFile(input);
  yield* streamRows(file, options);
}

/**
 * Read a specific page of rows from a parquet file.
 *
 * @example
 * const { rows, hasMore, totalRows } = await readParquetPage('data.parquet', {
 *   page: 0,
 *   pageSize: 100
 * });
 */
export async function readParquetPage(input: string, options: PageOptions): Promise<PageResult> {
  const file = await resolveInputToFile(input);
  return readPage(file, options);
}

export { resolveParquetUrl } from "./urls.js";
export type { ResolvedParquetUrl } from "./urls.js";
export type {
  ParquetByteRange,
  ParquetColumn,
  ParquetColumnChunkLayout,
  ParquetFileMetadata,
  ParquetLayout,
  ParquetMetadata,
  ParquetRowGroupLayout,
  ParquetReadOptions,
  ParquetRow,
} from "./types.js";
export type { PageOptions, PageResult, StreamOptions } from "./streaming.js";
