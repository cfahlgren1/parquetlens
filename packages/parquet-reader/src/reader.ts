import { Buffer } from "node:buffer";

import type { AsyncBuffer, FileMetaData, LogicalType, SchemaElement, SchemaTree } from "hyparquet";
import {
  asyncBufferFromFile,
  asyncBufferFromUrl,
  parquetMetadataAsync,
  parquetReadObjects,
  parquetSchema,
} from "hyparquet";
import { compressors } from "hyparquet-compressors";

import type { ParquetColumn, ParquetMetadata, ParquetReadOptions, ParquetRow } from "./types.js";

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
): Promise<ParquetRow[]> {
  const { columns, rowStart, rowEnd } = normalizeReadOptions(options);
  if (rowStart !== undefined && rowEnd !== undefined && rowEnd <= rowStart) {
    return [];
  }
  const rows = await parquetReadObjects({ file, columns, rowStart, rowEnd, compressors });
  return rows as ParquetRow[];
}

export async function getMetadata(file: ParquetFile): Promise<ParquetMetadata> {
  const metadata = await parquetMetadataAsync(file);
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
  const createdBy = normalizeMetadataValue(
    "created_by" in metadata ? metadata.created_by : metadata.createdBy,
  );
  const keyValueMetadata = normalizeKeyValueMetadata(
    "key_value_metadata" in metadata ? metadata.key_value_metadata : metadata.keyValueMetadata,
  );
  const rowCount = normalizeRowCount("num_rows" in metadata ? metadata.num_rows : metadata.numRows);
  const schemaTree = parquetSchema(metadata);

  return {
    createdBy,
    keyValueMetadata,
    rowCount,
    columns: buildColumns(schemaTree),
  };
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

function normalizeRowCount(value: unknown): number | bigint | undefined {
  if (typeof value === "bigint") {
    if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(value);
    }
    return value;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  return undefined;
}
