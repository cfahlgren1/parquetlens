import { createCliRenderer } from "@opentui/core";
import { createRoot, useKeyboard, useTerminalDimensions } from "@opentui/react";
import { spawnSync } from "node:child_process";
import React, { useEffect, useMemo, useRef, useState } from "react";

import type {
  ParquetFileMetadata,
  ParquetReadOptions,
  ParquetRow,
  ParquetSource,
} from "@parquetlens/parquet-reader";
import { openParquetSource } from "@parquetlens/parquet-reader";

import { safeStringify } from "./formatting.js";

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
  rows: ParquetRow[];
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

async function createTuiRenderer() {
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
  return { root, handleExit };
}

export async function runTui(input: string, options: TuiOptions): Promise<void> {
  const source = await openParquetSource(input);

  const terminalRows = process.stdout.rows ?? 24;
  const pageSize = Math.max(1, terminalRows - RESERVED_LINES);
  const windowSize = Math.max(50, pageSize * 3);
  const initialLimit = options.maxRows ? Math.min(windowSize, options.maxRows) : windowSize;
  const readOptions: ParquetReadOptions = {
    batchSize: options.batchSize ?? 1024,
    columns: options.columns.length > 0 ? options.columns : undefined,
    limit: initialLimit,
    offset: 0,
  };

  const [initialRows, metadata] = await Promise.all([
    source.readTable(readOptions),
    source.readMetadata().catch(() => null),
  ]);

  const initialColumns = buildColumnInfo(metadata, initialRows, options.columns);
  const initialGrid: GridState = { columns: initialColumns, rows: initialRows };
  const initialKnownTotal = resolveInitialTotal(metadata, initialRows, initialLimit);

  const { root, handleExit } = await createTuiRenderer();
  root.render(
    <App
      source={source}
      filePath={input}
      options={options}
      onExit={handleExit}
      initialGrid={initialGrid}
      initialMetadata={metadata}
      initialKnownTotal={initialKnownTotal}
    />,
  );
}

export async function runTuiWithRows(
  rows: ParquetRow[],
  title: string,
  options: TuiOptions,
): Promise<void> {
  const { root, handleExit } = await createTuiRenderer();
  root.render(<StaticApp rows={rows} title={title} options={options} onExit={handleExit} />);
}

type TableViewerProps = {
  grid: GridState;
  title: string;
  offset: number;
  setOffset: React.Dispatch<React.SetStateAction<number>>;
  maxOffset: number | undefined;
  totalRows: number | undefined;
  pageSize: number;
  loading?: boolean;
  error?: string | null;
  notice?: string | null;
  metadata?: ParquetFileMetadata | null;
  onExit: () => void;
  onCopyError?: () => void;
};

function TableViewer({
  grid,
  title,
  offset,
  setOffset,
  maxOffset,
  totalRows,
  pageSize,
  loading = false,
  error = null,
  notice = null,
  metadata = null,
  onExit,
  onCopyError,
}: TableViewerProps) {
  const { width } = useTerminalDimensions();

  const [xOffset, setXOffset] = useState(0);
  const [selection, setSelection] = useState<{ row: number; col: number } | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const sidebarWidth = sidebarOpen
    ? Math.min(width, Math.max(SIDEBAR_MIN_WIDTH, Math.floor(width * SIDEBAR_WIDTH_RATIO)))
    : 0;
  const tableWidth = Math.max(0, width - (sidebarOpen ? sidebarWidth + PANEL_GAP : 0));
  const tableContentWidth = Math.max(0, tableWidth - CONTENT_BORDER_WIDTH);

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

  useEffect(() => {
    if (error) {
      setSidebarOpen(true);
    }
  }, [error]);

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
    if (key.name === "return" || key.name === "enter" || key.name === "s") {
      setSidebarOpen((current) => !current);
      return;
    }
    if (key.name === "x") {
      setSidebarOpen(false);
      return;
    }
    if (key.name === "e" && error) {
      setSidebarOpen(true);
      return;
    }
    if (key.name === "y" && error && onCopyError) {
      onCopyError();
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
  const detail = error ? buildErrorDetail(error) : buildDetail(selection, grid, offset);
  const detailTitle = error ? "error detail" : "cell detail";
  const metaFlags = getMetadataFlags(metadata);

  return (
    <box flexDirection="column" width="100%" height="100%" backgroundColor={THEME.background}>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderHeader({
          filePath: title,
          offset,
          rows: grid.rows.length,
          columns: grid.columns.length,
          loading,
          error,
          maxRows: totalRows,
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
            if (!event.scroll) return;
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
                  if (!target) return;
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
            title={detailTitle}
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
              onMouseDown={() => setSidebarOpen(false)}
            >
              [ close ]
            </text>
          </box>
        ) : null}
      </box>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderFooter(!!error, notice)}
      </box>
    </box>
  );
}

type AppProps = {
  source: ParquetSource;
  filePath: string;
  options: TuiOptions;
  onExit: () => void;
  initialGrid?: GridState;
  initialMetadata?: ParquetFileMetadata | null;
  initialKnownTotal?: number | null;
};

function App({
  source,
  filePath,
  options,
  onExit,
  initialGrid,
  initialMetadata,
  initialKnownTotal,
}: AppProps) {
  const { height } = useTerminalDimensions();
  const pageSize = Math.max(1, height - RESERVED_LINES);

  const [offset, setOffset] = useState(0);
  const [pendingOffset, setPendingOffset] = useState(0);
  const [windowStart, setWindowStart] = useState(0);
  const [windowRows, setWindowRows] = useState<ParquetRow[]>(initialGrid?.rows ?? []);
  const [columns, setColumns] = useState<ColumnInfo[]>(initialGrid?.columns ?? []);
  const [loading, setLoading] = useState(!initialGrid);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<ParquetFileMetadata | null>(initialMetadata ?? null);
  const [knownTotalRows, setKnownTotalRows] = useState<number | null>(initialKnownTotal ?? null);
  const noticeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const effectiveTotal = options.maxRows ?? knownTotalRows;
  const maxOffset =
    effectiveTotal === undefined || effectiveTotal === null
      ? undefined
      : Math.max(0, effectiveTotal - pageSize);

  const columnsToRead = options.columns;
  const visibleRows = useMemo(() => {
    if (windowRows.length === 0) return [];
    const startIndex = offset - windowStart;
    if (startIndex < 0 || startIndex >= windowRows.length) return [];
    return windowRows.slice(startIndex, startIndex + pageSize);
  }, [offset, pageSize, windowRows, windowStart]);
  const grid = useMemo(() => ({ columns, rows: visibleRows }), [columns, visibleRows]);

  // Cleanup source on unmount
  useEffect(() => {
    return () => {
      void source.close();
    };
  }, [source]);

  // Load data when offset changes
  useEffect(() => {
    const windowEnd = windowStart + windowRows.length;
    const withinWindow =
      windowRows.length > 0 &&
      pendingOffset >= windowStart &&
      pendingOffset + pageSize <= windowEnd;
    if (withinWindow) {
      if (offset !== pendingOffset) {
        setOffset(pendingOffset);
      }
      if (loading) {
        setLoading(false);
      }
      return;
    }

    let canceled = false;
    const targetOffset = pendingOffset;

    const loadWindow = async () => {
      setLoading(true);
      setError(null);
      try {
        const windowSize = Math.max(50, pageSize * 3);
        const start = Math.max(0, targetOffset - pageSize);
        const maxRows = options.maxRows ?? knownTotalRows;
        const remaining =
          maxRows === undefined || maxRows === null ? undefined : Math.max(0, maxRows - start);
        const limit = remaining === undefined ? windowSize : Math.min(windowSize, remaining);
        const readOptions: ParquetReadOptions = {
          batchSize: options.batchSize ?? 1024,
          columns: columnsToRead.length > 0 ? columnsToRead : undefined,
          limit,
          offset: start,
        };
        const rowsPage = await source.readTable(readOptions);
        const nextColumns = buildColumnInfo(metadata, rowsPage, columnsToRead);

        if (!canceled) {
          setWindowStart(start);
          setWindowRows(rowsPage);
          setColumns(nextColumns);
          setOffset(targetOffset);
          if (rowsPage.length < limit) {
            setKnownTotalRows(start + rowsPage.length);
          }
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
  }, [
    columnsToRead,
    knownTotalRows,
    loading,
    metadata,
    pendingOffset,
    options.batchSize,
    options.maxRows,
    pageSize,
    source,
    windowRows.length,
    windowStart,
  ]);

  // Load metadata (skip if preloaded)
  useEffect(() => {
    if (initialMetadata !== undefined) return;

    let canceled = false;
    source
      .readMetadata()
      .then((meta) => {
        if (!canceled) setMetadata(meta);
      })
      .catch(() => {
        if (!canceled) setMetadata(null);
      });
    return () => {
      canceled = true;
    };
  }, [initialMetadata, source]);

  useEffect(() => {
    if (!metadata) return;
    setColumns(buildColumnInfo(metadata, windowRows, columnsToRead));
  }, [columnsToRead, metadata, windowRows]);

  // Cleanup notice timer
  useEffect(() => {
    return () => {
      if (noticeTimer.current) clearTimeout(noticeTimer.current);
    };
  }, []);

  const handleCopyError = () => {
    if (!error) return;
    const copied = copyToClipboard(error);
    setNotice(copied ? "copied error to clipboard" : "clipboard unavailable");
    if (noticeTimer.current) clearTimeout(noticeTimer.current);
    noticeTimer.current = setTimeout(() => setNotice(null), 2000);
  };

  return (
    <TableViewer
      grid={grid}
      title={filePath}
      offset={offset}
      setOffset={setPendingOffset}
      maxOffset={maxOffset}
      totalRows={effectiveTotal ?? undefined}
      pageSize={pageSize}
      loading={loading}
      error={error}
      notice={notice}
      metadata={metadata}
      onExit={onExit}
      onCopyError={handleCopyError}
    />
  );
}

type StaticAppProps = {
  rows: ParquetRow[];
  title: string;
  options: TuiOptions;
  onExit: () => void;
};

function StaticApp({ rows, title, options, onExit }: StaticAppProps) {
  const { height } = useTerminalDimensions();
  const pageSize = Math.max(1, height - RESERVED_LINES);

  const [offset, setOffset] = useState(0);

  const columns = useMemo(
    () => buildColumnInfo(null, rows, options.columns),
    [rows, options.columns],
  );

  const totalRows = rows.length;
  const maxOffset = Math.max(0, totalRows - pageSize);

  const visibleRows = useMemo(
    () => rows.slice(offset, offset + pageSize),
    [rows, offset, pageSize],
  );

  const grid: GridState = useMemo(() => ({ columns, rows: visibleRows }), [columns, visibleRows]);

  return (
    <TableViewer
      grid={grid}
      title={title}
      offset={offset}
      setOffset={setOffset}
      maxOffset={maxOffset}
      totalRows={totalRows}
      pageSize={pageSize}
      onExit={onExit}
    />
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

function renderFooterLine(hasError: boolean): string {
  const errorHint = hasError ? " | e view error | y copy error" : "";
  return `q exit | arrows/jk scroll | pgup/pgdn page | h/l col jump | mouse wheel scroll | click cell for detail | s/enter toggle panel${errorHint}`;
}

function renderFooter(hasError: boolean, notice: string | null) {
  const controls = renderFooterLine(hasError);

  return (
    <box flexDirection="column" width="100%">
      {notice ? (
        <text wrapMode="none" truncate fg={THEME.badge}>
          {notice}
        </text>
      ) : null}
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
  const columnWidths = columns.map((col) => {
    const longestCell = rows.reduce(
      (max, row) => {
        const value = formatCellValue(row[col.name]);
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
  const row = grid.rows[rowIndex];
  const value = row ? formatCellDetail(row[columnName]) : "";
  const absoluteRow = offset + rowIndex + 1;

  return `row ${absoluteRow} • ${columnName}\n${columnType}\n\n${value}`;
}

function buildErrorDetail(message: string): string {
  return `error\n\n${message}`;
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

function buildColumnInfo(
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

function formatCellDetail(value: unknown): string {
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

function formatCellValue(value: unknown): string {
  return formatCellDetail(value);
}

function resolveInitialTotal(
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

function copyToClipboard(value: string): boolean {
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
