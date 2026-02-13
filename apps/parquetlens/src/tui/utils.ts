import { spawnSync } from "node:child_process";

import type {
  ParquetColumnChunkLayout,
  ParquetFileMetadata,
  ParquetLayout,
  ParquetRow,
} from "@parquetlens/parquet-reader";

import { safeStringify } from "../formatting.js";
import { COLUMN_COLORS, DEFAULT_COLUMN_WIDTH } from "./constants.js";
import type {
  ByteSegment,
  BytesModel,
  BytesSummary,
  ColumnInfo,
  ColumnTotal,
  GridLines,
  GridState,
  ViewerTab,
} from "./types.js";

export function clampNumber(value: number, min: number, max: number): number {
  if (value < min) {
    return min;
  }
  if (value > max) {
    return max;
  }
  return value;
}

export function formatBigInt(value: bigint): string {
  return value.toLocaleString("en-US");
}

export function formatBytes(bytes: bigint): string {
  if (bytes < 0n) return "0 B";
  if (bytes < 1024n) return `${bytes} B`;
  if (bytes < 1024n * 1024n) {
    const kb = Number(bytes * 10n / 1024n) / 10;
    return `${kb.toFixed(1)} KB`;
  }
  if (bytes < 1024n * 1024n * 1024n) {
    const mb = Number(bytes * 10n / (1024n * 1024n)) / 10;
    return `${mb.toFixed(1)} MB`;
  }
  const gb = Number(bytes * 10n / (1024n * 1024n * 1024n)) / 10;
  return `${gb.toFixed(1)} GB`;
}

export function formatPercent(value: bigint, total: bigint): string {
  if (total <= 0n || value <= 0n) {
    return "0.0%";
  }

  const scaledTenths = Number((value * 1000n) / total);
  const whole = Math.floor(scaledTenths / 10);
  const tenth = scaledTenths % 10;
  return `${whole}.${tenth}%`;
}

export function scaleWidth(value: bigint, max: bigint, width: number): number {
  if (width <= 0 || max <= 0n || value <= 0n) {
    return 0;
  }

  const scaled = Number((value * BigInt(width)) / max);
  return clampNumber(scaled, 1, width);
}

export function getColumnColor(columnIndex: number): string {
  return COLUMN_COLORS[columnIndex % COLUMN_COLORS.length];
}

export function padCell(value: string, width: number): string {
  const normalized = normalizeCell(value);
  if (normalized.length > width) {
    if (width <= 3) {
      return normalized.slice(0, width);
    }
    return `${normalized.slice(0, width - 3)}...`;
  }
  return normalized.padEnd(width, " ");
}

export function normalizeCell(value: string): string {
  return value.replace(/\r?\n/g, "\\n").replace(/\t/g, "\\t");
}

export function clampScroll(value: number, max: number): number {
  if (value < 0) {
    return 0;
  }
  if (value > max) {
    return max;
  }
  return value;
}

export function buildBytesModel(layout: ParquetLayout | null | undefined): BytesModel | null {
  if (!layout || layout.rowGroups.length === 0) {
    return null;
  }

  const columns: string[] = [];
  const seenColumns = new Set<string>();

  for (const rowGroup of layout.rowGroups) {
    for (const chunk of rowGroup.columns) {
      if (!seenColumns.has(chunk.name)) {
        seenColumns.add(chunk.name);
        columns.push(chunk.name);
      }
    }
  }

  const rows = layout.rowGroups.map((rowGroup) => {
    const byName = new Map<string, ParquetColumnChunkLayout>();
    for (const chunk of rowGroup.columns) {
      byName.set(chunk.name, chunk);
    }

    return {
      rowGroup,
      chunksByColumn: columns.map((columnName) => byName.get(columnName) ?? null),
    };
  });

  return { columns, rows };
}

export function buildByteSegments(
  chunksByColumn: Array<ParquetColumnChunkLayout | null>,
  totalBytes: bigint,
  chartWidth: number,
): ByteSegment[] {
  if (chartWidth <= 0 || totalBytes <= 0n || chunksByColumn.length === 0) {
    return [];
  }

  const nonZeroColumnCount = chunksByColumn.reduce((count, chunk) => {
    return count + (chunk?.bytes && chunk.bytes > 0n ? 1 : 0);
  }, 0);
  const shouldReserveMinWidth = nonZeroColumnCount > 0 && chartWidth >= nonZeroColumnCount;

  const baseAllocations = chunksByColumn.map((chunk, columnIndex) => {
    const bytes = chunk?.bytes ?? 0n;
    if (bytes <= 0n) {
      return {
        columnIndex,
        bytes,
        chunk,
        width: 0,
        remainder: 0n,
      };
    }

    return {
      columnIndex,
      bytes,
      chunk,
      width: shouldReserveMinWidth ? 1 : 0,
      remainder: 0n,
    };
  });

  const reservedWidth = shouldReserveMinWidth ? nonZeroColumnCount : 0;
  const distributableWidth = chartWidth - reservedWidth;

  if (distributableWidth > 0) {
    const distributableTarget = BigInt(distributableWidth);
    for (const allocation of baseAllocations) {
      if (allocation.bytes <= 0n) {
        continue;
      }
      const scaled = allocation.bytes * distributableTarget;
      allocation.width += Number(scaled / totalBytes);
      allocation.remainder = scaled % totalBytes;
    }
  }

  let remaining = chartWidth - baseAllocations.reduce((sum, item) => sum + item.width, 0);

  if (remaining > 0) {
    const ranked = baseAllocations
      .filter((item) => item.bytes > 0n)
      .sort((left, right) => {
        if (left.remainder === right.remainder) {
          return left.columnIndex - right.columnIndex;
        }
        return left.remainder > right.remainder ? -1 : 1;
      });

    let cursor = 0;
    while (remaining > 0 && ranked.length > 0) {
      const current = ranked[cursor % ranked.length];
      current.width += 1;
      remaining -= 1;
      cursor += 1;
    }
  }

  return baseAllocations
    .filter((item) => item.width > 0)
    .map((item) => ({
      columnIndex: item.columnIndex,
      width: item.width,
      bytes: item.bytes,
      chunk: item.chunk,
    }));
}

export function buildColumnTotals(model: BytesModel): ColumnTotal[] {
  return model.columns.map((name, columnIndex) => {
    let totalBytes = 0n;
    for (const row of model.rows) {
      const chunk = row.chunksByColumn[columnIndex];
      if (chunk) {
        totalBytes += chunk.bytes;
      }
    }
    return { name, columnIndex, totalBytes, color: getColumnColor(columnIndex) };
  });
}

export function computeBytesSummary(model: BytesModel, totals: ColumnTotal[]): BytesSummary {
  let totalBytes = 0n;
  for (const row of model.rows) {
    totalBytes += row.rowGroup.bytes;
  }

  let largestColumn: BytesSummary["largestColumn"] = null;
  for (const col of totals) {
    if (!largestColumn || col.totalBytes > largestColumn.bytes) {
      largestColumn = {
        name: col.name,
        bytes: col.totalBytes,
        percent: formatPercent(col.totalBytes, totalBytes),
      };
    }
  }

  return {
    totalBytes,
    rowGroupCount: model.rows.length,
    columnCount: model.columns.length,
    largestColumn,
  };
}

export function buildGridLines(grid: GridState, offset: number, targetWidth: number): GridLines {
  const columns: ColumnInfo[] =
    grid.columns.length > 0 ? grid.columns : [{ name: "(loading)", type: "" }];
  const rows = grid.rows;

  const rowNumberWidth = Math.max(String(offset + rows.length).length, 3);
  const columnWidths = columns.map((col) => {
    const longestCell = rows.reduce(
      (max, row) => {
        const value = formatCellValue(row[col.name]);
        return Math.max(max, value.length);
      },
      Math.max(col.name.length, col.type.length),
    );
    return Math.min(Math.max(longestCell, DEFAULT_COLUMN_WIDTH), 40);
  });

  const headerNames = ["#", ...columns.map((c) => c.name)];
  const headerTypes = ["", ...columns.map((c) => c.type)];
  const headerWidths = [rowNumberWidth, ...columnWidths];
  const separatorWidth = headerWidths.length > 1 ? (headerWidths.length - 1) * 2 : 0;
  const baseLength = headerWidths.reduce((total, width) => total + width, 0) + separatorWidth;

  if (targetWidth > baseLength && headerWidths.length > 1) {
    headerWidths[headerWidths.length - 1] += targetWidth - baseLength;
  }
  const headerNameLine = buildLine(headerNames, headerWidths);
  const headerTypeLine = buildLine(headerTypes, headerWidths);
  const separatorLine = buildSeparator(headerWidths);
  const rowLines = rows.map((row, index) => {
    const rowIndex = String(offset + index + 1);
    const values = [rowIndex, ...columns.map((col) => formatCellValue(row[col.name]))];
    return buildLine(values, headerWidths);
  });

  const { columnRanges, scrollStops } = buildColumnRanges(headerWidths);

  const maxLineLength = Math.max(
    headerNameLine.length,
    headerTypeLine.length,
    separatorLine.length,
    ...rowLines.map((line) => line.length),
  );

  return {
    headerNameLine,
    headerTypeLine,
    separatorLine,
    rowLines,
    maxLineLength,
    columnRanges,
    scrollStops,
  };
}

export function applyHorizontalScroll(
  lines: GridLines,
  width: number,
  xOffset: number,
  pageSize: number,
): { headerName: string; headerType: string; separator: string; rows: string[] } {
  const sliceLine = (line: string) => {
    if (width <= 0) {
      return "";
    }
    const sliced = xOffset <= 0 ? line.slice(0, width) : line.slice(xOffset, xOffset + width);
    return sliced.padEnd(width, " ");
  };

  const rows = lines.rowLines.slice(0, pageSize).map(sliceLine);
  const emptyRow = width > 0 ? " ".repeat(width) : "";
  while (rows.length < pageSize) {
    rows.push(emptyRow);
  }

  return {
    headerName: sliceLine(lines.headerNameLine),
    headerType: sliceLine(lines.headerTypeLine),
    separator: sliceLine(lines.separatorLine),
    rows,
  };
}

function buildLine(values: string[], widths: number[]): string {
  return values
    .map((value, index) => {
      const width = widths[index] ?? DEFAULT_COLUMN_WIDTH;
      return padCell(value, width);
    })
    .join("  ");
}

function buildSeparator(_widths: number[]): string {
  return "";
}

export function buildColumnRanges(widths: number[]): {
  columnRanges: Array<{ start: number; end: number }>;
  scrollStops: number[];
} {
  const columnRanges: Array<{ start: number; end: number }> = [];
  const scrollStops: number[] = [0];
  let cursor = 0;

  for (let i = 0; i < widths.length; i += 1) {
    const width = widths[i];
    if (i > 0) {
      const start = cursor;
      const end = cursor + width - 1;
      columnRanges.push({ start, end });
      scrollStops.push(start);
    }
    cursor += width;
    if (i < widths.length - 1) {
      cursor += 2;
    }
  }

  return { columnRanges, scrollStops };
}

export function findColumnIndex(x: number, ranges: Array<{ start: number; end: number }>): number {
  for (let index = 0; index < ranges.length; index += 1) {
    const range = ranges[index];
    if (x >= range.start && x <= range.end) {
      return index;
    }
  }
  return -1;
}

export function findScrollStop(current: number, stops: number[], direction: 1 | -1): number {
  if (direction > 0) {
    for (const stop of stops) {
      if (stop > current) {
        return stop;
      }
    }
    return current;
  }

  for (let index = stops.length - 1; index >= 0; index -= 1) {
    const stop = stops[index];
    if (stop < current) {
      return stop;
    }
  }

  return 0;
}

export function buildDetail(
  selection: { row: number; col: number } | null,
  grid: GridState,
  offset: number,
): string {
  if (!selection || grid.columns.length === 0 || grid.rows.length === 0) {
    return "click a cell to see full details";
  }

  const rowIndex = Math.min(selection.row, grid.rows.length - 1);
  const colIndex = Math.min(selection.col, grid.columns.length - 1);
  const col = grid.columns[colIndex];
  const columnName = col?.name ?? "(unknown)";
  const columnType = col?.type ?? "";
  const row = grid.rows[rowIndex];
  const value = row ? formatCellDetail(row[columnName]) : "";
  const absoluteRow = offset + rowIndex + 1;

  return `row ${absoluteRow} • ${columnName}\n${columnType}\n\n${value}`;
}

export function buildErrorDetail(message: string): string {
  return `error\n\n${message}`;
}

export function getMetadataFlags(metadata: ParquetFileMetadata | null): {
  optimized: boolean;
  createdBy?: string;
} {
  if (!metadata) {
    return { optimized: false };
  }

  const raw = metadata.keyValueMetadata["content_defined_chunking"];
  const normalized = raw?.toLowerCase?.() ?? "";
  const optimized =
    raw !== undefined && raw !== null && normalized !== "false" && normalized !== "0";

  return {
    optimized,
    createdBy: metadata.createdBy,
  };
}

export function buildColumnInfo(
  metadata: ParquetFileMetadata | null,
  rows: ParquetRow[],
  requestedColumns: string[],
): ColumnInfo[] {
  if (metadata?.columns?.length) {
    const names =
      requestedColumns.length > 0 ? requestedColumns : metadata.columns.map((col) => col.name);
    return names.map((name) => {
      const col = metadata.columns.find((meta) => meta.name === name);
      const fallback = rows.find((row) => row[name] !== null && row[name] !== undefined)?.[name];
      return { name, type: col?.type ?? inferColumnType(fallback) };
    });
  }

  const names =
    requestedColumns.length > 0
      ? requestedColumns
      : rows.length > 0
        ? Object.keys(rows[0] ?? {})
        : [];

  return names.map((name) => {
    const value = rows.find((row) => row[name] !== null && row[name] !== undefined)?.[name];
    return { name, type: inferColumnType(value) };
  });
}

function inferColumnType(value: unknown): string {
  if (value === null || value === undefined) {
    return "unknown";
  }

  if (typeof value === "bigint") {
    return "bigint";
  }

  if (value instanceof Date) {
    return "timestamp";
  }

  if (value instanceof Uint8Array) {
    return "binary";
  }

  if (Array.isArray(value)) {
    return "array";
  }

  if (typeof value === "object") {
    return "object";
  }

  return typeof value;
}

export function formatCellDetail(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof Uint8Array) {
    return `Uint8Array(${value.length})`;
  }

  if (typeof value === "object") {
    return safeStringify(value);
  }

  return String(value);
}

export function formatCellValue(value: unknown): string {
  return formatCellDetail(value);
}

export function resolveInitialTotal(
  metadata: ParquetFileMetadata | null,
  rows: ParquetRow[],
  initialLimit: number,
): number | null {
  if (metadata?.rowCount !== undefined) {
    const rowCount = normalizeRowCount(metadata.rowCount);
    if (rowCount !== undefined) {
      return rowCount;
    }
  }

  return rows.length < initialLimit ? rows.length : null;
}

function normalizeRowCount(value: number | bigint | undefined): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "bigint") {
    if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return Number(value);
    }
  }

  return undefined;
}

export function copyToClipboard(value: string): boolean {
  const platform = process.platform;
  const candidates: Array<[string, string[]]> = [];

  if (platform === "darwin") {
    candidates.push(["pbcopy", []]);
  } else if (platform === "win32") {
    candidates.push(["clip", []]);
  } else {
    candidates.push(["wl-copy", []], ["xclip", ["-selection", "clipboard"]]);
  }

  for (const [command, args] of candidates) {
    const result = spawnSync(command, args, { input: value });
    if (!result.error && result.status === 0) {
      return true;
    }
  }

  return false;
}

export function getAvailableTabs(hasLayout: boolean): ViewerTab[] {
  return hasLayout ? ["table", "layout", "bytes"] : ["table"];
}

export function cycleTab(
  current: ViewerTab,
  availableTabs: ViewerTab[],
  direction: 1 | -1,
): ViewerTab {
  const index = availableTabs.indexOf(current);
  const startIndex = index >= 0 ? index : 0;
  const nextIndex = (startIndex + direction + availableTabs.length) % availableTabs.length;
  return availableTabs[nextIndex];
}

export function getTabFromKeyName(name: string, availableTabs: ViewerTab[]): ViewerTab | null {
  if (name === "1") {
    return "table";
  }

  if (name === "2" && availableTabs.includes("layout")) {
    return "layout";
  }

  if (name === "3" && availableTabs.includes("bytes")) {
    return "bytes";
  }

  return null;
}
