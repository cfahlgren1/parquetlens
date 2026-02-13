import { useKeyboard } from "@opentui/react";
import React, { useEffect, useState } from "react";

import type {
  ParquetFileMetadata,
  ParquetLayout,
  ParquetRowGroupLayout,
} from "@parquetlens/parquet-reader";

import { PANEL_GAP, THEME } from "./constants.js";
import { LayoutCompactRangeRow, LayoutInfoRow, renderFooter, renderHeader } from "./shared.js";
import type { ViewerTab } from "./types.js";
import {
  clampNumber,
  cycleTab,
  formatBigInt,
  formatPercent,
  getAvailableTabs,
  getMetadataFlags,
  getTabFromKeyName,
} from "./utils.js";

type LayoutViewerProps = {
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
};

export function LayoutViewer({
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
}: LayoutViewerProps) {
  const layout = metadata?.layout;
  const rowGroups = layout?.rowGroups ?? [];
  const [selectedRowGroup, setSelectedRowGroup] = useState(0);
  const availableTabs = getAvailableTabs(hasLayout);
  const metaFlags = getMetadataFlags(metadata);
  const totalLayoutBytes = rowGroups.reduce((sum, rowGroup) => sum + rowGroup.bytes, 0n);
  const selectedGroup = rowGroups[selectedRowGroup] ?? null;

  useEffect(() => {
    setSelectedRowGroup((current) => clampNumber(current, 0, Math.max(0, rowGroups.length - 1)));
  }, [rowGroups.length]);

  const selectRowGroup = (index: number) => {
    if (rowGroups.length === 0) {
      return;
    }
    setSelectedRowGroup(clampNumber(index, 0, rowGroups.length - 1));
  };

  const moveSelection = (delta: number) => {
    selectRowGroup(selectedRowGroup + delta);
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

    if ((key.ctrl && key.name === "c") || key.name === "escape" || key.name === "q") {
      onExit();
      return;
    }

    if (key.name === "down" || key.name === "j" || key.name === "right" || key.name === "l") {
      moveSelection(1);
      return;
    }

    if (key.name === "up" || key.name === "k" || key.name === "left" || key.name === "h") {
      moveSelection(-1);
      return;
    }

    if (key.name === "pagedown" || key.name === "space") {
      moveSelection(10);
      return;
    }

    if (key.name === "pageup") {
      moveSelection(-10);
      return;
    }

    if (key.name === "home" || (key.name === "g" && !key.shift)) {
      setSelectedRowGroup(0);
      return;
    }

    if (key.name === "end" || (key.name === "g" && key.shift)) {
      setSelectedRowGroup(Math.max(0, rowGroups.length - 1));
    }
  });

  const summaryText =
    rowGroups.length > 0
      ? `rowgroups ${rowGroups.length.toLocaleString()} | selected rg ${selectedGroup?.index.toLocaleString() ?? "n/a"} | bytes ${formatBigInt(totalLayoutBytes)}`
      : "layout unavailable";
  const selectedGroupPosition = rowGroups.length > 0 ? selectedRowGroup + 1 : 0;
  const selectedGroupShare =
    selectedGroup && totalLayoutBytes > 0n
      ? formatPercent(selectedGroup.bytes, totalLayoutBytes)
      : "0.0%";

  return (
    <box flexDirection="column" width="100%" height="100%" backgroundColor={THEME.background}>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderHeader({
          filePath: title,
          offset: selectedRowGroup,
          rows: rowGroups.length,
          columns: selectedGroup?.columns.length ?? 0,
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
      <box flexGrow={1} flexDirection="column" gap={PANEL_GAP}>
        <box
          backgroundColor={THEME.panel}
          border
          borderColor={THEME.border}
          title="rowgroups"
          titleAlignment="left"
          flexDirection="row"
        >
          <text
            wrapMode="none"
            fg={selectedRowGroup > 0 ? THEME.accent : THEME.muted}
            onMouseDown={() => moveSelection(-1)}
          >
            {" [ prev ] "}
          </text>
          <text wrapMode="none" fg={THEME.text}>
            {`rowgroup ${selectedGroupPosition.toLocaleString()} of ${rowGroups.length.toLocaleString()} `}
          </text>
          <text wrapMode="none" fg={THEME.muted}>
            {`(${selectedGroup?.index.toLocaleString() ?? "n/a"})`}
          </text>
          <box flexGrow={1} />
          <text wrapMode="none" fg={THEME.muted}>
            {selectedGroup
              ? `${formatBigInt(selectedGroup.bytes)} bytes • ${selectedGroupShare}`
              : "no rowgroup"}
          </text>
          <text
            wrapMode="none"
            fg={selectedRowGroup < rowGroups.length - 1 ? THEME.accent : THEME.muted}
            onMouseDown={() => moveSelection(1)}
          >
            {" [ next ] "}
          </text>
        </box>
        <box
          flexGrow={1}
          backgroundColor={THEME.panel}
          border
          borderColor={THEME.border}
          title="layout"
          titleAlignment="left"
          onMouseScroll={(event) => {
            if (!event.scroll) return;
            const step = Math.max(1, event.scroll.delta);
            if (event.scroll.direction === "up") {
              moveSelection(-step);
            } else if (event.scroll.direction === "down") {
              moveSelection(step);
            }
          }}
        >
          <scrollbox scrollY flexGrow={1} backgroundColor={THEME.panel}>
            {!layout || !selectedGroup ? (
              <text wrapMode="word" fg={THEME.muted}>
                {"layout metadata unavailable"}
              </text>
            ) : (
              <box flexDirection="column">
                <box
                  flexDirection="column"
                  backgroundColor={THEME.header}
                  border
                  borderColor={THEME.border}
                >
                  <LayoutInfoRow label="PAR1" value="" labelColor={THEME.accent} />
                  <LayoutInfoRow label="start" value={formatBigInt(layout.magic.start)} />
                  <LayoutInfoRow label="bytes" value={formatBigInt(layout.magic.bytes)} />
                  <LayoutInfoRow label="end" value={formatBigInt(layout.magic.end)} />
                </box>

                <box
                  flexDirection="column"
                  backgroundColor={THEME.header}
                  border
                  borderColor={THEME.border}
                >
                  <LayoutInfoRow
                    label={`RowGroup ${selectedGroup.index}`}
                    value={`bytes ${formatBigInt(selectedGroup.bytes)}`}
                    labelColor={THEME.badge}
                  />
                  {selectedGroup.numRows !== undefined ? (
                    <LayoutInfoRow label="rows" value={formatBigInt(selectedGroup.numRows)} />
                  ) : null}
                </box>

                {selectedGroup.columns.map((chunk) => (
                  <box
                    key={`layout-column-${selectedGroup.index}-${chunk.name}`}
                    flexDirection="column"
                    backgroundColor={THEME.background}
                    border
                    borderColor={THEME.border}
                  >
                    <LayoutInfoRow
                      label={`Column '${chunk.name}'`}
                      value={`bytes ${formatBigInt(chunk.bytes)}`}
                      labelColor="#8be9fd"
                    />

                    {chunk.dictionaryRange ? (
                      <LayoutCompactRangeRow
                        title="Dictionary"
                        range={chunk.dictionaryRange}
                        titleColor="#ffb86c"
                      />
                    ) : null}

                    <LayoutCompactRangeRow
                      title="Data"
                      range={chunk.dataRange}
                      titleColor={THEME.text}
                    />
                  </box>
                ))}
              </box>
            )}
          </scrollbox>
        </box>
      </box>
      <box backgroundColor={THEME.header} border borderColor={THEME.border}>
        {renderFooter(activeTab, !!error, notice, hasLayout)}
      </box>
    </box>
  );
}
