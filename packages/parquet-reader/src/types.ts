export type ParquetRow = Record<string, unknown>;

export type ParquetReadOptions = {
  columns?: string[];
  rowStart?: number;
  rowEnd?: number;
  limit?: number;
  offset?: number;
  batchSize?: number;
  rowGroups?: number[];
};

export type ParquetColumn = {
  name: string;
  type: string;
  path?: string[];
};

export type ParquetByteRange = {
  start: bigint;
  bytes: bigint;
  end: bigint;
};

export type ParquetColumnChunkLayout = {
  name: string;
  path: string[];
  bytes: bigint;
  compression?: string;
  totalRange: ParquetByteRange;
  dictionaryRange?: ParquetByteRange;
  dataRange: ParquetByteRange;
};

export type ParquetRowGroupLayout = {
  index: number;
  bytes: bigint;
  numRows?: bigint;
  columns: ParquetColumnChunkLayout[];
};

export type ParquetLayout = {
  magic: ParquetByteRange;
  rowGroups: ParquetRowGroupLayout[];
};

export type ParquetMetadata = {
  createdBy?: string;
  keyValueMetadata: Record<string, string>;
  rowCount?: number | bigint;
  columns: ParquetColumn[];
  layout?: ParquetLayout;
};

export type ParquetFileMetadata = ParquetMetadata;
