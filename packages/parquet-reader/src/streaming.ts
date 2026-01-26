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

const DEFAULT_CHUNK_SIZE = 1000;

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
  const totalRows = Number(metadata.num_rows);

  if (totalRows === 0) {
    return;
  }

  const chunkSize = batchSize ?? DEFAULT_CHUNK_SIZE;
  let currentRow = 0;

  while (currentRow < totalRows) {
    if (signal?.aborted) {
      return;
    }

    const rowEnd = Math.min(currentRow + chunkSize, totalRows);
    const rows = (await parquetReadObjects({
      file,
      columns,
      rowStart: currentRow,
      rowEnd,
      compressors,
      rowFormat: "object",
    })) as ParquetRow[];

    if (batchSize) {
      yield rows;
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
  const totalRows = Number(metadata.num_rows);

  const rowStart = page * pageSize;
  const rowEnd = Math.min(rowStart + pageSize, totalRows);

  if (rowStart >= totalRows) {
    return { rows: [], hasMore: false, totalRows };
  }

  const rows = (await parquetReadObjects({
    file,
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
