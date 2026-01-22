import { createCliRenderer } from "@opentui/core";
import { createRoot, useKeyboard, useTerminalDimensions } from "@opentui/react";
import React, { useEffect, useMemo, useState } from "react";

import type {
  ParquetBufferSource,
  ParquetFileMetadata,
  ParquetReadOptions,
} from "@parquetlens/parquet-reader";
import { openParquetBufferFromPath } from "@parquetlens/parquet-reader";

type TuiOptions = {
  columns: string[];
  maxRows?: number;
  batchSize?: number;
};

type ColumnInfo = {
  name: string;
  type: string;
};

type GridState = {
  columns: ColumnInfo[];
  rows: string[][];
};

type GridLines = {
  headerNameLine: string;
  headerTypeLine: string;
  separatorLine: string;
  rowLines: string[];
  maxLineLength: number;
  columnRanges: Array<{ start: number; end: number }>;
  scrollStops: number[];
};

const TOP_BAR_LINES = 3;
const FOOTER_LINES = 4;
const TABLE_BORDER_LINES = 2;
const TABLE_HEADER_LINES = 3;
const RESERVED_LINES = TOP_BAR_LINES + FOOTER_LINES + TABLE_BORDER_LINES + TABLE_HEADER_LINES;
const DEFAULT_COLUMN_WIDTH = 6;
const MAX_COLUMN_WIDTH = 40;
const SCROLL_STEP = 3;
const SIDEBAR_WIDTH_RATIO = 0.35;
const SIDEBAR_MIN_WIDTH = 24;
const CONTENT_BORDER_WIDTH = 2;
const PANEL_GAP = 1;

const THEME = {
  background: "#1e1f29",
  panel: "#22232e",
  header: "#2d2f3d",
  border: "#44475a",
  accent: "#bd93f9",
  badge: "#50fa7b",
  badgeText: "#1e1f29",
  text: "#c5cee0",
  muted: "#6272a4",
  stripe: "#252733",
};

export async function runTui(filePath: string, options: TuiOptions): Promise<void> {
  const source = await openParquetBufferFromPath(filePath);
  const renderer = await createCliRenderer({
    exitOnCtrlC: true,
    useAlternateScreen: true,
    useConsole: false,
    useMouse: true,
    enableMouseMovement: true,
  });
  renderer.setTerminalTitle("parquetlens");
  const root = createRoot(renderer);

  const handleExit = () => {
    root.unmount();
    renderer.destroy();
  };

  root.render(<App source={source} filePath={filePath} options={options} onExit={handleExit} />);
}

type AppProps = {
  source: ParquetBufferSource;
  filePath: string;
  options: TuiOptions;
  onExit: () => void;
};

function App({ source, filePath, options, onExit }: AppProps) {
  const { width, height } = useTerminalDimensions();
  const pageSize = Math.max(1, height - RESERVED_LINES);
  const maxOffset =
    options.maxRows === undefined ? undefined : Math.max(0, options.maxRows - pageSize);

  const [offset, setOffset] = useState(0);
  const [xOffset, setXOffset] = useState(0);
  const [grid, setGrid] = useState<GridState>({ columns: [], rows: [] } as GridState);
  const [selection, setSelection] = useState<{ row: number; col: number } | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<ParquetFileMetadata | null>(null);
  const sidebarWidth = sidebarOpen
    ? Math.min(width, Math.max(SIDEBAR_MIN_WIDTH, Math.floor(width * SIDEBAR_WIDTH_RATIO)))
    : 0;
  const tableWidth = Math.max(0, width - (sidebarOpen ? sidebarWidth + PANEL_GAP : 0));
  const tableContentWidth = Math.max(0, tableWidth - CONTENT_BORDER_WIDTH);

  const columnsToRead = options.columns;

  useEffect(() => {
    let canceled = false;

    const loadWindow = () => {
      setLoading(true);
      setError(null);
      try {
        const limit = options.maxRows
          ? Math.max(0, Math.min(pageSize, options.maxRows - offset))
          : pageSize;
        const readOptions: ParquetReadOptions = {
          batchSize: options.batchSize ?? 1024,
          columns: columnsToRead.length > 0 ? columnsToRead : undefined,
          limit,
          offset,
        };
        const table = source.readTable(readOptions);
        const columns: ColumnInfo[] = table.schema.fields.map((field) => ({
          name: field.name,
          type: formatArrowType(field.type),
        }));
        const rows = tableToRows(
          table,
          columns.map((c) => c.name),
        );

        if (!canceled) {
          setGrid({ columns, rows });
        }
      } catch (caught) {
        const message = caught instanceof Error ? caught.message : String(caught);
        if (!canceled) {
          setError(message);
        }
      } finally {
        if (!canceled) {
          setLoading(false);
        }
      }
    };

    loadWindow();

    return () => {
      canceled = true;
    };
  }, [columnsToRead, offset, options.batchSize, options.maxRows, pageSize, source]);

  useEffect(() => {
    let canceled = false;
    source
      .readMetadata()
      .then((meta) => {
        if (!canceled) {
          setMetadata(meta);
        }
      })
      .catch(() => {
        if (!canceled) {
          setMetadata(null);
        }
      });

    return () => {
      canceled = true;
    };
  }, [source]);

  const gridLines = useMemo(
    () => buildGridLines(grid, offset, tableContentWidth),
    [grid, offset, tableContentWidth],
  );
  const maxScrollX = Math.max(0, gridLines.maxLineLength - tableContentWidth);

  useEffect(() => {
    setXOffset((current) => Math.min(current, maxScrollX));
  }, [gridLines.maxLineLength, maxScrollX, tableContentWidth]);

  useEffect(() => {
    if (grid.rows.length === 0 || grid.columns.length === 0) {
      setSelection(null);
      return;
    }

    setSelection((current) => {
      const nextRow = current ? Math.min(current.row, grid.rows.length - 1) : 0;
      const nextCol = current ? Math.min(current.col, grid.columns.length - 1) : 0;
      return { row: nextRow, col: nextCol };
    });
  }, [grid.columns.length, grid.rows.length]);

  useKeyboard((key) => {
    if ((key.ctrl && key.name === "c") || key.name === "escape" || key.name === "q") {
      if (sidebarOpen && key.name === "escape") {
        setSidebarOpen(false);
        return;
      }
      onExit();
      return;
    }

    const clampOffset = (value: number) => {
      const clamped = Math.max(0, value);
      return maxOffset === undefined ? clamped : Math.min(clamped, maxOffset);
    };

    if (key.name === "down" || key.name === "j") {
      setOffset((current) => clampOffset(current + 1));
      return;
    }

    if (key.name === "up" || key.name === "k") {
      setOffset((current) => clampOffset(current - 1));
      return;
    }

    if (key.name === "pagedown" || key.name === "space") {
      setOffset((current) => clampOffset(current + pageSize));
      return;
    }

    if (key.name === "pageup") {
      setOffset((current) => clampOffset(current - pageSize));
      return;
    }

    if (key.name === "home" || (key.name === "g" && !key.shift)) {
      setOffset(0);
      return;
    }

    if ((key.name === "end" || (key.name === "g" && key.shift)) && maxOffset !== undefined) {
      setOffset(maxOffset);
      return;
    }

    if (key.name === "return" || key.name === "enter") {
      setSidebarOpen((current) => !current);
      return;
    }

    if (key.name === "x") {
      setSidebarOpen(false);
      return;
    }

    if (key.name === "s") {
      setSidebarOpen((current) => !current);
      return;
    }

    if (key.name === "left" || key.name === "h") {
      setXOffset((current) =>
        clampScroll(findScrollStop(current, gridLines.scrollStops, -1), maxScrollX),
      );
      return;
    }

    if (key.name === "right" || key.name === "l") {
      setXOffset((current) =>
        clampScroll(findScrollStop(current, gridLines.scrollStops, 1), maxScrollX),
      );
    }
  });

  const visibleLines = applyHorizontalScroll(gridLines, tableContentWidth, xOffset, pageSize);
  const detail = buildDetail(selection, grid, offset);
  const metaFlags = getMetadataFlags(metadata);

  return (
    <box flexDirection="column" width="100%" height="100%" backgroundColor={THEME.background}>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderHeader({
          filePath,
          offset,
          rows: grid.rows.length,
          columns: grid.columns.length,
          loading,
          error,
          maxRows: options.maxRows,
          optimized: metaFlags.optimized,
          createdBy: metaFlags.createdBy,
        })}
      </box>
      <box flexGrow={1} flexDirection="row" gap={PANEL_GAP}>
        <box
          backgroundColor={THEME.panel}
          border
          borderColor={THEME.border}
          flexGrow={1}
          width={sidebarOpen ? tableWidth : "100%"}
          onMouseScroll={(event) => {
            if (!event.scroll) {
              return;
            }

            const delta = Math.max(1, event.scroll.delta);
            const step = delta * SCROLL_STEP;

            if (event.scroll.direction === "up") {
              setOffset((current) => Math.max(0, current - step));
            } else if (event.scroll.direction === "down") {
              setOffset((current) => {
                const next = current + step;
                return maxOffset === undefined ? next : Math.min(next, maxOffset);
              });
            } else if (event.scroll.direction === "left") {
              setXOffset((current) => clampScroll(current - step, maxScrollX));
            } else if (event.scroll.direction === "right") {
              setXOffset((current) => clampScroll(current + step, maxScrollX));
            }
          }}
        >
          <text wrapMode="none" truncate fg={THEME.accent}>
            {visibleLines.headerName}
          </text>
          <text wrapMode="none" truncate fg={THEME.muted}>
            {visibleLines.headerType}
          </text>
          <text wrapMode="none" truncate fg={THEME.border}>
            {visibleLines.separator}
          </text>
          {visibleLines.rows.map((line, index) => {
            const isSelected = selection?.row === index;
            return (
              <text
                key={`row-${index}`}
                wrapMode="none"
                truncate
                fg={THEME.text}
                bg={isSelected ? THEME.header : index % 2 === 0 ? THEME.background : THEME.stripe}
                onMouseDown={(event) => {
                  const target = event.target;
                  if (!target) {
                    return;
                  }
                  const localX = Math.max(0, event.x - target.x);
                  const absoluteX = localX + xOffset;
                  const colIndex = findColumnIndex(absoluteX, gridLines.columnRanges);
                  if (colIndex >= 0) {
                    setSelection({ row: index, col: colIndex });
                    setSidebarOpen(true);
                  }
                }}
              >
                {line}
              </text>
            );
          })}
        </box>
        {sidebarOpen ? (
          <box
            width="35%"
            minWidth={24}
            backgroundColor={THEME.panel}
            border
            borderColor={THEME.border}
            title="cell detail"
            titleAlignment="left"
          >
            <text wrapMode="none" truncate fg={THEME.muted}>
              press esc/x to close
            </text>
            <scrollbox scrollY flexGrow={1} backgroundColor={THEME.panel}>
              <text
                wrapMode="word"
                fg={THEME.text}
                selectable
                selectionBg={THEME.accent}
                selectionFg={THEME.background}
              >
                {detail}
              </text>
            </scrollbox>
            <text
              wrapMode="none"
              truncate
              fg={THEME.accent}
              onMouseDown={() => {
                setSidebarOpen(false);
              }}
            >
              [ close ]
            </text>
          </box>
        ) : null}
      </box>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderFooter()}
      </box>
    </box>
  );
}

type HeaderProps = {
  filePath: string;
  offset: number;
  rows: number;
  columns: number;
  loading: boolean;
  error: string | null;
  maxRows?: number;
  createdBy?: string;
  optimized: boolean;
};

function renderHeader(props: HeaderProps) {
  const { filePath, offset, rows, columns, loading, error, maxRows, createdBy, optimized } = props;

  const start = rows > 0 ? offset + 1 : offset;
  const end = rows > 0 ? offset + rows : offset;
  const totalText = maxRows !== undefined ? `of ${maxRows.toLocaleString()}` : "";
  const fileName = filePath.split("/").pop() ?? filePath;

  if (error) {
    return (
      <box flexDirection="row" alignItems="center" gap={2} width="100%">
        <text wrapMode="none" fg={THEME.accent}>
          {"◈ parquetlens"}
        </text>
        <text wrapMode="none" fg="#ef4444">
          {"error: " + error}
        </text>
      </box>
    );
  }

  return (
    <box flexDirection="row" alignItems="center" gap={2} width="100%">
      <text wrapMode="none" fg={THEME.accent}>
        {"◈ parquetlens"}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {"│"}
      </text>
      <text wrapMode="none" fg={THEME.text}>
        {fileName}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {"│"}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {"rows "}
      </text>
      <text wrapMode="none" fg={THEME.text}>
        {`${start.toLocaleString()}-${end.toLocaleString()} ${totalText}`}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {"│"}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {"cols "}
      </text>
      <text wrapMode="none" fg={THEME.text}>
        {columns.toString()}
      </text>
      {createdBy ? (
        <>
          <text wrapMode="none" fg={THEME.muted}>
            {"│"}
          </text>
          <text wrapMode="none" fg={THEME.muted} truncate>
            {createdBy}
          </text>
        </>
      ) : null}
      <box flexGrow={1} />
      {loading ? (
        <text wrapMode="none" fg={THEME.badge}>
          {"● loading"}
        </text>
      ) : null}
      {optimized ? (
        <text wrapMode="none" fg={THEME.badgeText} bg={THEME.badge}>
          {" ✓ OPTIMIZED "}
        </text>
      ) : null}
    </box>
  );
}

function renderFooterLine(): string {
  return "q exit | arrows/jk scroll | pgup/pgdn page | h/l col jump | mouse wheel scroll | click cell for detail | s/enter toggle panel";
}

function renderFooter() {
  const controls = renderFooterLine();

  return (
    <box flexDirection="column" width="100%">
      <text wrapMode="none" truncate fg={THEME.muted}>
        {controls}
      </text>
    </box>
  );
}

function buildGridLines(grid: GridState, offset: number, targetWidth: number): GridLines {
  const columns: ColumnInfo[] =
    grid.columns.length > 0 ? grid.columns : [{ name: "(loading)", type: "" }];
  const rows = grid.rows;

  const rowNumberWidth = Math.max(String(offset + rows.length).length, 3);
  const columnWidths = columns.map((col, index) => {
    const longestCell = rows.reduce(
      (max, row) => {
        const value = row[index] ?? "";
        return Math.max(max, value.length);
      },
      Math.max(col.name.length, col.type.length),
    );
    return Math.min(Math.max(longestCell, DEFAULT_COLUMN_WIDTH), MAX_COLUMN_WIDTH);
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
    const values = [rowIndex, ...row];
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

function applyHorizontalScroll(
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

function buildSeparator(widths: number[]): string {
  return "";
}

function padCell(value: string, width: number): string {
  const normalized = normalizeCell(value);
  if (normalized.length > width) {
    if (width <= 3) {
      return normalized.slice(0, width);
    }
    return `${normalized.slice(0, width - 3)}...`;
  }
  return normalized.padEnd(width, " ");
}

function normalizeCell(value: string): string {
  return value.replace(/\r?\n/g, "\\n").replace(/\t/g, "\\t");
}

function buildColumnRanges(widths: number[]): {
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

function findColumnIndex(x: number, ranges: Array<{ start: number; end: number }>): number {
  for (let index = 0; index < ranges.length; index += 1) {
    const range = ranges[index];
    if (x >= range.start && x <= range.end) {
      return index;
    }
  }
  return -1;
}

function findScrollStop(current: number, stops: number[], direction: 1 | -1): number {
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

function clampScroll(value: number, max: number): number {
  if (value < 0) {
    return 0;
  }
  if (value > max) {
    return max;
  }
  return value;
}

function buildDetail(
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
  const value = grid.rows[rowIndex]?.[colIndex] ?? "";
  const absoluteRow = offset + rowIndex + 1;

  return `row ${absoluteRow} • ${columnName}\n${columnType}\n\n${value}`;
}

function getMetadataFlags(metadata: ParquetFileMetadata | null): {
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

function tableToRows(table: import("apache-arrow").Table, columns: string[]): string[][] {
  const rows: string[][] = [];

  for (const batch of table.batches) {
    const vectors = columns.map((_, index) => batch.getChildAt(index));

    for (let rowIndex = 0; rowIndex < batch.numRows; rowIndex += 1) {
      const row = vectors.map((vector) => formatCell(vector?.get(rowIndex)));
      rows.push(row);
    }
  }

  return rows;
}

function formatCell(value: unknown): string {
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
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  return String(value);
}

function formatArrowType(type: import("apache-arrow").DataType): string {
  return type.toString();
}
