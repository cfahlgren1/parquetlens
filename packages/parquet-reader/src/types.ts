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

export type ParquetMetadata = {
  createdBy?: string;
  keyValueMetadata: Record<string, string>;
  rowCount?: number | bigint;
  columns: ParquetColumn[];
};

export type ParquetFileMetadata = ParquetMetadata;
