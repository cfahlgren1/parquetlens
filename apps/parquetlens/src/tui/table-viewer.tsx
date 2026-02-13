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
import { renderFooter, renderHeader } from "./shared.js";
import type { GridState, ViewerTab } from "./types.js";
import {
  applyHorizontalScroll,
  buildDetail,
  buildErrorDetail,
  buildGridLines,
  clampScroll,
  cycleTab,
  findColumnIndex,
  findScrollStop,
  getAvailableTabs,
  getMetadataFlags,
  getTabFromKeyName,
} from "./utils.js";

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
  activeTab: ViewerTab;
  setActiveTab: React.Dispatch<React.SetStateAction<ViewerTab>>;
  hasLayout: boolean;
  onExit: () => void;
  onCopyError?: () => void;
};

export function TableViewer({
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
  activeTab,
  setActiveTab,
  hasLayout,
  onExit,
  onCopyError,
}: TableViewerProps) {
  const { width } = useTerminalDimensions();

  const [xOffset, setXOffset] = useState(0);
  const [selection, setSelection] = useState<{ row: number; col: number } | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const availableTabs = getAvailableTabs(hasLayout);

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
          activeTab,
          hasLayout,
          onTabSelect: setActiveTab,
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
        {renderFooter(activeTab, !!error, notice, hasLayout)}
      </box>
    </box>
  );
}
