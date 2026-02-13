import { useTerminalDimensions } from "@opentui/react";
import React, { useEffect, useMemo, useRef, useState } from "react";

import type {
  ParquetFileMetadata,
  ParquetReadOptions,
  ParquetRow,
} from "@parquetlens/parquet-reader";

import { BytesViewer } from "./bytes-viewer.js";
import { RESERVED_LINES } from "./constants.js";
import { LayoutViewer } from "./layout-viewer.js";
import { TableViewer } from "./table-viewer.js";
import type { ColumnInfo, GridState, TuiOptions, ViewerTab } from "./types.js";
import { buildColumnInfo, copyToClipboard } from "./utils.js";

type AppProps = {
  source: import("@parquetlens/parquet-reader").ParquetSource;
  filePath: string;
  options: TuiOptions;
  onExit: () => void;
  initialGrid?: GridState;
  initialMetadata?: ParquetFileMetadata | null;
  initialKnownTotal?: number | null;
};

export function App({
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
  const [activeTab, setActiveTab] = useState<ViewerTab>("table");
  const [loading, setLoading] = useState(!initialGrid);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [metadata, setMetadata] = useState<ParquetFileMetadata | null>(initialMetadata ?? null);
  const [knownTotalRows, setKnownTotalRows] = useState<number | null>(initialKnownTotal ?? null);
  const noticeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hasLayout = (metadata?.layout?.rowGroups.length ?? 0) > 0;

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
          if (rowsPage.length < limit && options.maxRows === undefined && knownTotalRows === null) {
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

  useEffect(() => {
    if (!hasLayout && activeTab !== "table") {
      setActiveTab("table");
    }
  }, [activeTab, hasLayout]);

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

  if (activeTab === "layout" && hasLayout) {
    return (
      <LayoutViewer
        title={filePath}
        pageSize={pageSize}
        loading={loading}
        error={error}
        notice={notice}
        metadata={metadata}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        hasLayout={hasLayout}
        onExit={onExit}
      />
    );
  }

  if (activeTab === "bytes" && hasLayout) {
    return (
      <BytesViewer
        title={filePath}
        pageSize={pageSize}
        loading={loading}
        error={error}
        notice={notice}
        metadata={metadata}
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        hasLayout={hasLayout}
        onExit={onExit}
        onCopyError={handleCopyError}
      />
    );
  }

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
      activeTab={activeTab}
      setActiveTab={setActiveTab}
      hasLayout={hasLayout}
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

export function StaticApp({ rows, title, options, onExit }: StaticAppProps) {
  const { height } = useTerminalDimensions();
  const pageSize = Math.max(1, height - RESERVED_LINES);

  const [offset, setOffset] = useState(0);
  const [activeTab, setActiveTab] = useState<ViewerTab>("table");

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
      activeTab={activeTab}
      setActiveTab={setActiveTab}
      hasLayout={false}
      onExit={onExit}
    />
  );
}
