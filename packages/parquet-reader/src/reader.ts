import { Buffer } from "node:buffer";

import type {
  AsyncBuffer,
  ColumnChunk,
  FileMetaData,
  LogicalType,
  RowGroup,
  SchemaElement,
  SchemaTree,
} from "hyparquet";
import {
  asyncBufferFromFile,
  asyncBufferFromUrl,
  parquetMetadataAsync,
  parquetReadObjects,
  parquetSchema,
} from "hyparquet";
import { compressors } from "hyparquet-compressors";

import type {
  ParquetByteRange,
  ParquetColumn,
  ParquetColumnChunkLayout,
  ParquetLayout,
  ParquetMetadata,
  ParquetReadOptions,
  ParquetRow,
  ParquetRowGroupLayout,
} from "./types.js";

type ParquetFile = AsyncBuffer | ArrayBuffer;

type NormalizedReadOptions = {
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
};

export async function asyncBufferFromPath(filePath: string): Promise<AsyncBuffer> {
  return asyncBufferFromFile(filePath);
}

export async function asyncBufferFromHttpUrl(url: string): Promise<AsyncBuffer> {
  return asyncBufferFromUrl({ url });
}

export async function asyncBufferFromStdin(): Promise<ArrayBuffer> {
  const chunks: Uint8Array[] = [];

  for await (const chunk of process.stdin) {
    if (typeof chunk === "string") {
      chunks.push(Buffer.from(chunk));
    } else {
      chunks.push(chunk);
    }
  }

  const buffer = Buffer.concat(chunks);
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
}

export async function readParquet(
  file: ParquetFile,
  options?: ParquetReadOptions,
  metadata?: FileMetaData,
): Promise<ParquetRow[]> {
  const { columns, rowStart, rowEnd } = normalizeReadOptions(options);
  if (rowStart !== undefined && rowEnd !== undefined && rowEnd <= rowStart) {
    return [];
  }
  const rows = await parquetReadObjects({
    file,
    metadata,
    columns,
    rowStart,
    rowEnd,
    compressors,
  });
  return rows as ParquetRow[];
}

export async function getMetadata(file: ParquetFile): Promise<ParquetMetadata> {
  return buildParquetMetadata(await getRawMetadata(file));
}

export async function getRawMetadata(file: ParquetFile): Promise<FileMetaData> {
  return parquetMetadataAsync(file);
}

export function buildParquetMetadata(metadata: FileMetaData): ParquetMetadata {
  return buildMetadata(metadata);
}

function normalizeReadOptions(options?: ParquetReadOptions): NormalizedReadOptions {
  if (!options) {
    return {};
  }

  const columns = options.columns && options.columns.length > 0 ? options.columns : undefined;
  const hasRange =
    options.rowStart !== undefined ||
    options.rowEnd !== undefined ||
    options.offset !== undefined ||
    options.limit !== undefined;
  const baseStart = options.rowStart ?? options.offset ?? 0;
  const rowStart = hasRange ? Math.max(0, baseStart) : undefined;
  let rowEnd = options.rowEnd;

  if (rowEnd === undefined && typeof options.limit === "number") {
    const safeLimit = Math.max(0, options.limit);
    const startValue = rowStart ?? 0;
    rowEnd = startValue + safeLimit;
  }

  if (rowStart !== undefined && rowEnd !== undefined) {
    rowEnd = Math.max(rowStart, rowEnd);
  }

  return { columns, rowStart, rowEnd };
}

function buildMetadata(metadata: FileMetaData): ParquetMetadata {
  const createdBy = normalizeMetadataValue(metadata.created_by);
  const keyValueMetadata = normalizeKeyValueMetadata(metadata.key_value_metadata);
  const rowCount = normalizeRowCount(metadata.num_rows);
  const schemaTree = parquetSchema(metadata);

  return {
    createdBy,
    keyValueMetadata,
    rowCount,
    columns: buildColumns(schemaTree),
    layout: buildLayout(metadata),
  };
}

function buildLayout(metadata: FileMetaData): ParquetLayout | undefined {
  const rowGroups = metadata.row_groups ?? [];
  if (rowGroups.length === 0) {
    return undefined;
  }

  return {
    magic: createByteRange(0n, 4n),
    rowGroups: rowGroups.map((rowGroup, index) => buildRowGroupLayout(rowGroup, index)),
  };
}

function buildRowGroupLayout(rowGroup: RowGroup, index: number): ParquetRowGroupLayout {
  const columns = rowGroup.columns
    .map((columnChunk) => buildColumnChunkLayout(columnChunk))
    .filter((columnChunk): columnChunk is ParquetColumnChunkLayout => columnChunk !== null);

  return {
    index,
    bytes: resolveRowGroupBytes(rowGroup, columns),
    numRows: normalizeBigInt(rowGroup.num_rows),
    columns,
  };
}

function buildColumnChunkLayout(columnChunk: ColumnChunk): ParquetColumnChunkLayout | null {
  const meta = columnChunk.meta_data;
  if (!meta) {
    return null;
  }

  const path = meta.path_in_schema ?? [];
  const name = path.length > 0 ? path.join(".") : "(unknown)";
  const totalBytes = normalizeBigInt(meta.total_compressed_size) ?? 0n;
  const dataStart = normalizeBigInt(meta.data_page_offset);

  if (dataStart === undefined) {
    return null;
  }

  const rawDictionaryStart = normalizeBigInt(meta.dictionary_page_offset);
  const hasDictionary =
    rawDictionaryStart !== undefined && rawDictionaryStart >= 0n && rawDictionaryStart < dataStart;
  const chunkStart = hasDictionary ? rawDictionaryStart : dataStart;
  const totalRange = createByteRange(chunkStart, totalBytes);

  const dictionaryRange = hasDictionary
    ? createByteRange(
        rawDictionaryStart!,
        clampRangeBytes(dataStart - rawDictionaryStart!, totalBytes),
      )
    : undefined;

  const dataBytes = totalRange.end > dataStart ? totalRange.end - dataStart : 0n;
  const dataRange = createByteRange(dataStart, dataBytes);

  return {
    name,
    path,
    bytes: totalBytes,
    compression: meta.codec,
    totalRange,
    dictionaryRange,
    dataRange,
  };
}

function resolveRowGroupBytes(rowGroup: RowGroup, columns: ParquetColumnChunkLayout[]): bigint {
  const totalCompressed = normalizeBigInt(rowGroup.total_compressed_size);
  if (totalCompressed !== undefined) {
    return totalCompressed;
  }

  const fromColumns = columns.reduce((sum, columnChunk) => sum + columnChunk.bytes, 0n);
  if (fromColumns > 0n) {
    return fromColumns;
  }

  return normalizeBigInt(rowGroup.total_byte_size) ?? 0n;
}

function createByteRange(start: bigint, bytes: bigint): ParquetByteRange {
  const safeStart = start >= 0n ? start : 0n;
  const safeBytes = bytes >= 0n ? bytes : 0n;
  return {
    start: safeStart,
    bytes: safeBytes,
    end: safeStart + safeBytes,
  };
}

function clampRangeBytes(value: bigint, max: bigint): bigint {
  if (value <= 0n) {
    return 0n;
  }
  if (value > max) {
    return max;
  }
  return value;
}

function buildColumns(schema: SchemaTree): ParquetColumn[] {
  return schema.children.map((child) => ({
    name: child.element.name,
    type: formatSchemaType(child.element),
    path: child.path,
  }));
}

function formatSchemaType(element: SchemaElement): string {
  if (element.logical_type) {
    return formatLogicalType(element.logical_type);
  }

  if (element.converted_type) {
    return element.converted_type;
  }

  if (element.type) {
    return element.type;
  }

  if (element.num_children) {
    return "GROUP";
  }

  return "UNKNOWN";
}

function formatLogicalType(value: LogicalType): string {
  if (typeof value === "string") {
    return value;
  }

  if (value && typeof value === "object" && "type" in value && value.type) {
    return value.type;
  }

  return "LOGICAL";
}

function normalizeKeyValueMetadata(
  input: FileMetaData["key_value_metadata"] | undefined,
): Record<string, string> {
  const normalized: Record<string, string> = {};

  if (!input) {
    return normalized;
  }

  for (const entry of input) {
    if (!entry || typeof entry.key !== "string") {
      continue;
    }
    normalized[entry.key] = normalizeMetadataValue(entry.value) ?? "";
  }

  return normalized;
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

  if (typeof value === "bigint") {
    return value.toString();
  }

  return String(value);
}

function normalizeBigInt(value: unknown): bigint | undefined {
  if (typeof value === "bigint") {
    if (value < 0n) {
      return 0n;
    }
    return value;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return BigInt(Math.max(0, Math.trunc(value)));
  }

  return undefined;
}

function normalizeRowCount(value: unknown): number | bigint | undefined {
  const normalized = normalizeBigInt(value);
  if (normalized !== undefined) {
    if (normalized <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(normalized);
    }
    return normalized;
  }

  return undefined;
}
