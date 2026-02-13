import type { ParquetColumnChunkLayout, ParquetRowGroupLayout } from "@parquetlens/parquet-reader";

export type ColumnInfo = {
  name: string;
  type: string;
};

export type GridState = {
  columns: ColumnInfo[];
  rows: import("@parquetlens/parquet-reader").ParquetRow[];
};

export type GridLines = {
  headerNameLine: string;
  headerTypeLine: string;
  separatorLine: string;
  rowLines: string[];
  maxLineLength: number;
  columnRanges: Array<{ start: number; end: number }>;
  scrollStops: number[];
};

export type ViewerTab = "table" | "layout" | "bytes";

export type BytesModelRow = {
  rowGroup: ParquetRowGroupLayout;
  chunksByColumn: Array<ParquetColumnChunkLayout | null>;
};

export type BytesModel = {
  columns: string[];
  rows: BytesModelRow[];
};

export type ByteSegment = {
  columnIndex: number;
  width: number;
  bytes: bigint;
  chunk: ParquetColumnChunkLayout | null;
};

export type ColumnTotal = {
  name: string;
  columnIndex: number;
  totalBytes: bigint;
  color: string;
};

export type BytesSummary = {
  totalBytes: bigint;
  rowGroupCount: number;
  columnCount: number;
  largestColumn: { name: string; bytes: bigint; percent: string } | null;
};

export type BytesViewMode = "chart" | "totals";

export type TuiOptions = {
  columns: string[];
  maxRows?: number;
  batchSize?: number;
};
