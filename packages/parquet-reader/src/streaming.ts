import type { AsyncBuffer } from "hyparquet";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors } from "hyparquet-compressors";

import type { ParquetRow } from "./types.js";

export type StreamOptions = {
  columns?: string[];
  batchSize?: number;
  signal?: AbortSignal;
};

export type PageOptions = {
  page: number;
  pageSize: number;
  columns?: string[];
};

export type PageResult = {
  rows: ParquetRow[];
  hasMore: boolean;
  totalRows?: number;
};

type ParquetFile = AsyncBuffer | ArrayBuffer;

const DEFAULT_READ_CHUNK_ROWS = 10000;
const LARGE_ROW_GROUP_ROWS_THRESHOLD = 100000;
const DEFAULT_FIRST_STREAM_CHUNK_ROWS = 256;

/**
 * Stream rows from a parquet file.
 *
 * By default, yields individual rows. Set `batchSize` to yield arrays of rows.
 *
 * @example
 * // Stream rows one at a time
 * for await (const row of streamRows(file)) {
 *   console.log(row);
 * }
 *
 * @example
 * // Stream in batches
 * for await (const batch of streamRows(file, { batchSize: 100 })) {
 *   console.log(batch.length);
 * }
 */
export async function* streamRows(
  file: ParquetFile,
  options?: StreamOptions,
): AsyncGenerator<ParquetRow | ParquetRow[]> {
  const { columns, batchSize, signal } = options ?? {};
  const metadata = await parquetMetadataAsync(file);
  const totalRows = bigintToPositiveNumber(metadata.num_rows);
  if (totalRows <= 0) {
    return;
  }

  const normalizedBatchSize = normalizeBatchSize(batchSize);
  let currentRow = 0;
  let pendingBatch: ParquetRow[] = [];

  for (const rowGroup of metadata.row_groups) {
    if (signal?.aborted) {
      return;
    }

    const rowGroupRows = bigintToPositiveNumber(rowGroup.num_rows);
    if (rowGroupRows <= 0) {
      continue;
    }
    const rowGroupStart = currentRow;
    const rowGroupEnd = currentRow + rowGroupRows;
    const readChunkRows = getReadChunkRows(rowGroupRows, normalizedBatchSize);

    while (currentRow < rowGroupEnd) {
      if (signal?.aborted) {
        return;
      }

      const useLowLatencyFirstChunk =
        normalizedBatchSize === undefined && currentRow === rowGroupStart && rowGroupRows > 1;
      const currentChunkRows = useLowLatencyFirstChunk
        ? Math.min(readChunkRows, DEFAULT_FIRST_STREAM_CHUNK_ROWS)
        : readChunkRows;
      const rowEnd = Math.min(rowGroupEnd, currentRow + currentChunkRows);
      const rows = (await parquetReadObjects({
        file,
        metadata,
        columns,
        rowStart: currentRow,
        rowEnd,
        compressors,
        rowFormat: "object",
      })) as ParquetRow[];

      if (normalizedBatchSize) {
        let offset = 0;

        if (pendingBatch.length > 0) {
          const needed = normalizedBatchSize - pendingBatch.length;
          pendingBatch.push(...rows.slice(0, needed));
          offset = Math.min(rows.length, needed);

          if (pendingBatch.length === normalizedBatchSize) {
            yield pendingBatch;
            pendingBatch = [];
          }
        }

        while (offset + normalizedBatchSize <= rows.length) {
          yield rows.slice(offset, offset + normalizedBatchSize);
          offset += normalizedBatchSize;
        }

        if (offset < rows.length) {
          pendingBatch = rows.slice(offset);
        }
      } else {
        for (const row of rows) {
          if (signal?.aborted) {
            return;
          }
          yield row;
        }
      }

      currentRow = rowEnd;
    }
  }

  if (normalizedBatchSize && pendingBatch.length > 0) {
    yield pendingBatch;
  }
}

function normalizeBatchSize(batchSize: number | undefined): number | undefined {
  if (typeof batchSize !== "number" || !Number.isFinite(batchSize)) {
    return undefined;
  }
  const normalized = Math.trunc(batchSize);
  if (normalized <= 0) {
    return undefined;
  }
  return normalized;
}

function bigintToPositiveNumber(value: bigint): number {
  return value > 0n ? Number(value) : 0;
}

function getReadChunkRows(rowGroupRows: number, batchSize: number | undefined): number {
  if (rowGroupRows <= LARGE_ROW_GROUP_ROWS_THRESHOLD) {
    return rowGroupRows;
  }
  if (batchSize !== undefined) {
    return Math.max(batchSize, DEFAULT_READ_CHUNK_ROWS);
  }
  return DEFAULT_READ_CHUNK_ROWS;
}

/**
 * Read a specific page of rows from a parquet file.
 *
 * @example
 * const { rows, hasMore, totalRows } = await readPage(file, { page: 0, pageSize: 100 });
 */
export async function readPage(file: ParquetFile, options: PageOptions): Promise<PageResult> {
  const { page, pageSize, columns } = options;

  if (page < 0 || pageSize <= 0) {
    return { rows: [], hasMore: false, totalRows: 0 };
  }

  const metadata = await parquetMetadataAsync(file);
  const totalRows = bigintToPositiveNumber(metadata.num_rows);

  const rowStart = page * pageSize;
  const rowEnd = Math.min(rowStart + pageSize, totalRows);

  if (rowStart >= totalRows) {
    return { rows: [], hasMore: false, totalRows };
  }

  const rows = (await parquetReadObjects({
    file,
    metadata,
    columns,
    rowStart,
    rowEnd,
    compressors,
    rowFormat: "object",
  })) as ParquetRow[];

  return {
    rows,
    hasMore: rowEnd < totalRows,
    totalRows,
  };
}
