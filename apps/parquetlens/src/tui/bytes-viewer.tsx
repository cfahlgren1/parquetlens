import { useKeyboard, useTerminalDimensions } from "@opentui/react";
import React, { useEffect, useMemo, useState } from "react";

import type { ParquetFileMetadata } from "@parquetlens/parquet-reader";

import {
  CONTENT_BORDER_WIDTH,
  PANEL_GAP,
  SCROLL_STEP,
  SIDEBAR_MIN_WIDTH,
  SIDEBAR_WIDTH_RATIO,
  THEME,
} from "./constants.js";
import { LayoutInfoRow, renderFooter, renderHeader } from "./shared.js";
import type { BytesModel, BytesSummary, BytesViewMode, ViewerTab } from "./types.js";
import {
  buildByteSegments,
  buildBytesModel,
  buildColumnTotals,
  buildErrorDetail,
  clampNumber,
  computeBytesSummary,
  cycleTab,
  formatBigInt,
  formatBytes,
  formatPercent,
  getAvailableTabs,
  getColumnColor,
  getMetadataFlags,
  getTabFromKeyName,
  padCell,
  scaleWidth,
} from "./utils.js";

type BytesViewerProps = {
  title: string;
  pageSize: number;
  loading?: boolean;
  error?: string | null;
  notice?: string | null;
  metadata?: ParquetFileMetadata | null;
  activeTab: ViewerTab;
  setActiveTab: React.Dispatch<React.SetStateAction<ViewerTab>>;
  hasLayout: boolean;
  onExit: () => void;
  onCopyError?: () => void;
};

export function BytesViewer({
  title,
  pageSize,
  loading = false,
  error = null,
  notice = null,
  metadata = null,
  activeTab,
  setActiveTab,
  hasLayout,
  onExit,
  onCopyError,
}: BytesViewerProps) {
  const { width } = useTerminalDimensions();
  const [rowOffset, setRowOffset] = useState(0);
  const [selection, setSelection] = useState<{ row: number; col: number } | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [viewMode, setViewMode] = useState<BytesViewMode>("chart");
  const model = useMemo(() => buildBytesModel(metadata?.layout), [metadata]);
  const rows = model?.rows ?? [];
  const columns = model?.columns ?? [];
  const maxOffset = Math.max(0, rows.length - pageSize);
  const maxRowGroupBytes = rows.reduce((max, row) => {
    return row.rowGroup.bytes > max ? row.rowGroup.bytes : max;
  }, 0n);
  const availableTabs = getAvailableTabs(hasLayout);
  const metaFlags = getMetadataFlags(metadata);

  const columnTotals = useMemo(() => (model ? buildColumnTotals(model) : []), [model]);
  const summary = useMemo(
    () => (model ? computeBytesSummary(model, columnTotals) : null),
    [model, columnTotals],
  );

  const barLabelWidth = useMemo(() => {
    if (rows.length === 0) return 6;
    const maxIndexWidth = Math.max(1, String(rows[rows.length - 1].rowGroup.index).length);
    const maxBytesWidth = rows.reduce((max, row) => {
      const len = formatBytes(row.rowGroup.bytes).length;
      return len > max ? len : max;
    }, 0);
    return 3 + maxIndexWidth + 2 + maxBytesWidth + 2;
  }, [rows]);

  const sidebarWidth = sidebarOpen
    ? Math.min(width, Math.max(SIDEBAR_MIN_WIDTH, Math.floor(width * SIDEBAR_WIDTH_RATIO)))
    : 0;
  const contentWidth = Math.max(
    0,
    width - (sidebarOpen ? sidebarWidth + PANEL_GAP : 0) - CONTENT_BORDER_WIDTH,
  );
  const chartWidth = Math.max(8, contentWidth - barLabelWidth);

  useEffect(() => {
    setRowOffset((current) => Math.min(current, maxOffset));
  }, [maxOffset]);

  useEffect(() => {
    if (rows.length === 0 || columns.length === 0) {
      setSelection(null);
      return;
    }
    setSelection((current) => ({
      row: Math.min(current?.row ?? 0, rows.length - 1),
      col: Math.min(current?.col ?? 0, columns.length - 1),
    }));
  }, [columns.length, rows.length]);

  useEffect(() => {
    if (error) {
      setSidebarOpen(true);
    }
  }, [error]);

  const moveSelectionRow = (delta: number) => {
    if (rows.length === 0) {
      return;
    }
    setSelection((current) => {
      const nextRow = clampNumber((current?.row ?? 0) + delta, 0, rows.length - 1);
      const nextCol = clampNumber(current?.col ?? 0, 0, Math.max(0, columns.length - 1));

      setRowOffset((currentOffset) => {
        if (nextRow < currentOffset) {
          return nextRow;
        }
        if (nextRow >= currentOffset + pageSize) {
          return Math.max(0, nextRow - pageSize + 1);
        }
        return currentOffset;
      });

      return { row: nextRow, col: nextCol };
    });
  };

  useKeyboard((key) => {
    const directTab = getTabFromKeyName(key.name, availableTabs);
    if (directTab) {
      setActiveTab(directTab);
      return;
    }

    if (key.name === "tab") {
      setActiveTab((current) => cycleTab(current, availableTabs, key.shift ? -1 : 1));
      return;
    }

    if (key.name === "[") {
      setActiveTab((current) => cycleTab(current, availableTabs, -1));
      return;
    }

    if (key.name === "]") {
      setActiveTab((current) => cycleTab(current, availableTabs, 1));
      return;
    }

    if (key.name === "t") {
      setViewMode((current) => (current === "chart" ? "totals" : "chart"));
      return;
    }

    if ((key.ctrl && key.name === "c") || key.name === "q" || key.name === "escape") {
      if (sidebarOpen && key.name === "escape") {
        setSidebarOpen(false);
        return;
      }
      onExit();
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

    if (key.name === "down" || key.name === "j") {
      moveSelectionRow(1);
      return;
    }

    if (key.name === "up" || key.name === "k") {
      moveSelectionRow(-1);
      return;
    }

    if (key.name === "pagedown" || key.name === "space") {
      moveSelectionRow(pageSize);
      return;
    }

    if (key.name === "pageup") {
      moveSelectionRow(-pageSize);
      return;
    }

    if (key.name === "home" || (key.name === "g" && !key.shift)) {
      setSelection((current) => ({ row: 0, col: current?.col ?? 0 }));
      setRowOffset(0);
      return;
    }

    if (key.name === "end" || (key.name === "g" && key.shift)) {
      const lastRow = Math.max(0, rows.length - 1);
      setSelection((current) => ({ row: lastRow, col: current?.col ?? 0 }));
      setRowOffset(Math.max(0, lastRow - pageSize + 1));
      return;
    }

    if (key.name === "left" || key.name === "h") {
      if (columns.length === 0) return;
      setSelection((current) => {
        if (!current) return { row: 0, col: 0 };
        return { row: current.row, col: Math.max(0, current.col - 1) };
      });
      setSidebarOpen(true);
      return;
    }

    if (key.name === "right" || key.name === "l") {
      if (columns.length === 0) return;
      setSelection((current) => {
        if (!current) return { row: 0, col: 0 };
        return { row: current.row, col: Math.min(columns.length - 1, current.col + 1) };
      });
      setSidebarOpen(true);
    }
  });

  const visibleRows = rows.slice(rowOffset, rowOffset + pageSize);
  const summaryText =
    rows.length > 0
      ? `rowgroups ${Math.min(rowOffset + 1, rows.length).toLocaleString()}-${Math.min(
          rowOffset + visibleRows.length,
          rows.length,
        ).toLocaleString()} of ${rows.length.toLocaleString()} | columns ${columns.length.toLocaleString()}`
      : "byte grid unavailable";
  const detailTitle = error ? "error detail" : "chunk detail";

  const summaryLine = summary
    ? `total ${formatBytes(summary.totalBytes)} | ${summary.rowGroupCount} row groups | ${summary.columnCount} columns${summary.largestColumn ? ` | largest: ${summary.largestColumn.name} (${summary.largestColumn.percent})` : ""}`
    : "";

  const sortedTotals = useMemo(() => {
    return [...columnTotals].sort((a, b) =>
      b.totalBytes > a.totalBytes ? 1 : b.totalBytes < a.totalBytes ? -1 : 0,
    );
  }, [columnTotals]);

  return (
    <box flexDirection="column" width="100%" height="100%" backgroundColor={THEME.background}>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderHeader({
          filePath: title,
          offset: rowOffset,
          rows: visibleRows.length,
          columns: columns.length,
          loading,
          error,
          optimized: metaFlags.optimized,
          createdBy: metaFlags.createdBy,
          activeTab,
          hasLayout,
          summaryText,
          onTabSelect: setActiveTab,
        })}
      </box>
      <box flexGrow={1} flexDirection="row" gap={PANEL_GAP}>
        <box
          backgroundColor={THEME.panel}
          border
          borderColor={THEME.border}
          flexGrow={1}
          width={sidebarOpen ? Math.max(0, width - sidebarWidth - PANEL_GAP) : "100%"}
          onMouseScroll={(event) => {
            if (!event.scroll) return;
            const step = Math.max(1, event.scroll.delta) * SCROLL_STEP;
            if (event.scroll.direction === "up") {
              moveSelectionRow(-step);
            } else if (event.scroll.direction === "down") {
              moveSelectionRow(step);
            }
          }}
        >
          {summary ? (
            <box flexDirection="row" backgroundColor={THEME.header}>
              <text wrapMode="none" truncate fg={THEME.text}>
                {` ${summaryLine}`}
              </text>
            </box>
          ) : null}
          <box flexDirection="row" backgroundColor={THEME.header} flexWrap="wrap">
            {columnTotals.map((col) => {
              const pct = summary ? formatPercent(col.totalBytes, summary.totalBytes) : "";
              const short = col.name.length <= 10 ? col.name : `${col.name.slice(0, 9)}…`;
              return (
                <box key={`legend-${col.columnIndex}`} flexDirection="row">
                  <text wrapMode="none" bg={col.color} fg={THEME.background}>
                    {" "}
                  </text>
                  <text wrapMode="none" fg={THEME.muted}>
                    {` ${short} ${pct}  `}
                  </text>
                </box>
              );
            })}
          </box>
          {viewMode === "chart" ? (
            <>
              {visibleRows.map((row, visibleIndex) => {
                const absoluteIndex = rowOffset + visibleIndex;
                const scaledRowWidth = scaleWidth(row.rowGroup.bytes, maxRowGroupBytes, chartWidth);
                const segments = buildByteSegments(
                  row.chunksByColumn,
                  row.rowGroup.bytes,
                  scaledRowWidth,
                );
                const usedWidth = segments.reduce((sum, segment) => sum + segment.width, 0);
                const trailingWidth = Math.max(0, scaledRowWidth - usedWidth);
                const remainingWidth = Math.max(0, chartWidth - scaledRowWidth);
                const isRowSelected = selection?.row === absoluteIndex;
                const indexStr = `rg ${row.rowGroup.index}`;
                const sizeStr = formatBytes(row.rowGroup.bytes);
                const label = `${indexStr}  ${sizeStr}  `;

                return (
                  <box
                    key={`bytes-row-${absoluteIndex}`}
                    flexDirection="row"
                    backgroundColor={
                      isRowSelected
                        ? THEME.header
                        : visibleIndex % 2 === 0
                          ? THEME.background
                          : THEME.stripe
                    }
                  >
                    <text wrapMode="none" truncate fg={THEME.muted}>
                      {padCell(label, barLabelWidth)}
                    </text>
                    <box flexDirection="row">
                      {segments.map((segment) => {
                        const isSelected =
                          selection?.row === absoluteIndex && selection.col === segment.columnIndex;
                        const segmentColor = getColumnColor(segment.columnIndex);
                        return (
                          <text
                            key={`segment-${absoluteIndex}-${segment.columnIndex}`}
                            wrapMode="none"
                            fg={isSelected ? THEME.background : segmentColor}
                            bg={isSelected ? segmentColor : undefined}
                            onMouseDown={() => {
                              setSelection({ row: absoluteIndex, col: segment.columnIndex });
                              setSidebarOpen(true);
                            }}
                          >
                            {"█".repeat(segment.width)}
                          </text>
                        );
                      })}
                      {trailingWidth > 0 ? (
                        <text wrapMode="none" fg={THEME.border}>
                          {" ".repeat(trailingWidth)}
                        </text>
                      ) : null}
                      {remainingWidth > 0 ? (
                        <text wrapMode="none" fg={THEME.border}>
                          {" ".repeat(remainingWidth)}
                        </text>
                      ) : null}
                    </box>
                  </box>
                );
              })}
              {Array.from({ length: Math.max(0, pageSize - visibleRows.length) }).map(
                (_, index) => (
                  <text key={`bytes-empty-${index}`} wrapMode="none" truncate fg={THEME.text}>
                    {" "}
                  </text>
                ),
              )}
            </>
          ) : (
            <scrollbox scrollY flexGrow={1} backgroundColor={THEME.panel}>
              {sortedTotals.map((col) => {
                const pct = summary ? formatPercent(col.totalBytes, summary.totalBytes) : "0.0%";
                const barW =
                  summary && summary.totalBytes > 0n
                    ? scaleWidth(col.totalBytes, summary.totalBytes, Math.max(8, contentWidth - 40))
                    : 0;
                return (
                  <box key={`total-${col.columnIndex}`} flexDirection="row">
                    <text wrapMode="none" bg={col.color} fg={THEME.background}>
                      {" "}
                    </text>
                    <text wrapMode="none" fg={THEME.text}>
                      {` ${col.name.length <= 16 ? col.name.padEnd(16) : `${col.name.slice(0, 15)}…`}`}
                    </text>
                    <text wrapMode="none" fg={THEME.muted}>
                      {` ${formatBytes(col.totalBytes).padStart(10)}  ${pct.padStart(6)}  `}
                    </text>
                    <text wrapMode="none" fg={col.color}>
                      {"█".repeat(barW)}
                    </text>
                  </box>
                );
              })}
            </scrollbox>
          )}
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
              click a bar segment for exact bytes
            </text>
            <scrollbox scrollY flexGrow={1} backgroundColor={THEME.panel}>
              {error ? (
                <text
                  wrapMode="word"
                  fg={THEME.text}
                  selectable
                  selectionBg={THEME.accent}
                  selectionFg={THEME.background}
                >
                  {buildErrorDetail(error)}
                </text>
              ) : (
                renderBytesDetail(selection, model, summary)
              )}
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
        {renderFooter(activeTab, !!error, notice, hasLayout)}
      </box>
    </box>
  );
}

function renderBytesDetail(
  selection: { row: number; col: number } | null,
  model: BytesModel | null,
  summary: BytesSummary | null,
): React.ReactNode {
  if (!model || model.rows.length === 0 || model.columns.length === 0 || !selection) {
    return (
      <text wrapMode="word" fg={THEME.muted}>
        select a bar segment to inspect exact byte ranges
      </text>
    );
  }

  const rowIndex = clampNumber(selection.row, 0, model.rows.length - 1);
  const colIndex = clampNumber(selection.col, 0, model.columns.length - 1);
  const row = model.rows[rowIndex];
  const columnName = model.columns[colIndex];
  const chunk = row.chunksByColumn[colIndex];
  const color = getColumnColor(colIndex);
  const totalFileBytes = summary?.totalBytes ?? 0n;

  if (!chunk) {
    return (
      <box flexDirection="column">
        <text wrapMode="none" fg={color}>
          {columnName}
        </text>
        <text wrapMode="none" fg={THEME.muted}>
          {`row group ${row.rowGroup.index}`}
        </text>
        <text wrapMode="none" fg={THEME.muted}>
          {"\nno chunk present"}
        </text>
      </box>
    );
  }

  return (
    <box flexDirection="column">
      <text wrapMode="none" fg={color}>
        {chunk.name}
      </text>
      <text wrapMode="none" fg={THEME.muted}>
        {`row group ${row.rowGroup.index}`}
      </text>
      <text wrapMode="none" fg={THEME.text}>
        {" "}
      </text>
      <LayoutInfoRow label="size" value={formatBytes(chunk.bytes)} />
      <LayoutInfoRow
        label="% of row group"
        value={formatPercent(chunk.bytes, row.rowGroup.bytes)}
      />
      {totalFileBytes > 0n ? (
        <LayoutInfoRow label="% of file" value={formatPercent(chunk.bytes, totalFileBytes)} />
      ) : null}
      {chunk.compression ? (
        <>
          <text wrapMode="none" fg={THEME.text}>
            {" "}
          </text>
          <LayoutInfoRow label="codec" value={chunk.compression} />
        </>
      ) : null}
      <text wrapMode="none" fg={THEME.text}>
        {" "}
      </text>
      {chunk.dictionaryRange ? (
        <>
          <LayoutInfoRow
            label="dictionary"
            value={`${formatBytes(chunk.dictionaryRange.bytes)} (${formatPercent(chunk.dictionaryRange.bytes, chunk.bytes)})`}
          />
          <LayoutInfoRow
            label="  range"
            value={`${formatBigInt(chunk.dictionaryRange.start)} → ${formatBigInt(chunk.dictionaryRange.end)}`}
            labelColor={THEME.muted}
            valueColor={THEME.muted}
          />
        </>
      ) : null}
      <LayoutInfoRow
        label="data"
        value={`${formatBytes(chunk.dataRange.bytes)} (${formatPercent(chunk.dataRange.bytes, chunk.bytes)})`}
      />
      <LayoutInfoRow
        label="  range"
        value={`${formatBigInt(chunk.dataRange.start)} → ${formatBigInt(chunk.dataRange.end)}`}
        labelColor={THEME.muted}
        valueColor={THEME.muted}
      />
      <text wrapMode="none" fg={THEME.text}>
        {" "}
      </text>
      <LayoutInfoRow
        label="byte range"
        value={`${formatBigInt(chunk.totalRange.start)} → ${formatBigInt(chunk.totalRange.end)}`}
      />
    </box>
  );
}
