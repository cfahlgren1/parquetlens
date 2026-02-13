import React from "react";

import { THEME } from "./constants.js";
import type { ViewerTab } from "./types.js";
import { formatBigInt, getAvailableTabs } from "./utils.js";

type LayoutInfoRowProps = {
  label: string;
  value: string;
  labelColor?: string;
  valueColor?: string;
};

export function LayoutInfoRow({
  label,
  value,
  labelColor = THEME.text,
  valueColor = THEME.text,
}: LayoutInfoRowProps) {
  return (
    <box flexDirection="row">
      <text wrapMode="none" fg={labelColor}>
        {label}
      </text>
      <box flexGrow={1} />
      <text wrapMode="none" fg={valueColor}>
        {value}
      </text>
    </box>
  );
}

type LayoutRangeCardProps = {
  title: string;
  titleColor: string;
  range: {
    start: bigint;
    bytes: bigint;
    end: bigint;
  };
};

export function LayoutCompactRangeRow({ title, titleColor, range }: LayoutRangeCardProps) {
  const inlineValue = `start ${formatBigInt(range.start)}  bytes ${formatBigInt(range.bytes)}  end ${formatBigInt(range.end)}`;

  return (
    <LayoutInfoRow
      label={title}
      value={inlineValue}
      labelColor={titleColor}
      valueColor={THEME.muted}
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
  activeTab: ViewerTab;
  hasLayout: boolean;
  summaryText?: string;
  onTabSelect: (tab: ViewerTab) => void;
};

export function renderHeader(props: HeaderProps) {
  const {
    filePath,
    offset,
    rows,
    columns,
    loading,
    error,
    maxRows,
    createdBy,
    optimized,
    activeTab,
    hasLayout,
    summaryText,
    onTabSelect,
  } = props;

  const start = rows > 0 ? offset + 1 : offset;
  const end = rows > 0 ? offset + rows : offset;
  const totalText = maxRows !== undefined ? `of ${maxRows.toLocaleString()}` : "";
  const fileName = filePath.split("/").pop() ?? filePath;
  const summary =
    summaryText ??
    `rows ${start.toLocaleString()}-${end.toLocaleString()} ${totalText} | cols ${columns.toLocaleString()}`;

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
      <text wrapMode="none" fg={THEME.text}>
        {summary}
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
      <box flexDirection="row" gap={1}>
        {renderTabChips(activeTab, hasLayout, onTabSelect)}
      </box>
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

function renderFooterLine(activeTab: ViewerTab, hasError: boolean, hasLayout: boolean): string {
  const tabHints = hasLayout ? " | 1 table 2 layout 3 bytes | tab/[ ] switch | click tabs" : "";
  const errorHint = hasError ? " | e view error | y copy error" : "";

  if (activeTab === "layout") {
    return `q exit | arrows/jk select rowgroup | pgup/pgdn jump | home/end | mouse wheel scroll${tabHints}`;
  }

  if (activeTab === "bytes") {
    return `q exit | arrows/jk row | h/l column | t toggle totals | click segment for detail | s/enter toggle panel${errorHint}${tabHints}`;
  }

  return `q exit | arrows/jk scroll | pgup/pgdn page | h/l col jump | mouse wheel scroll | click cell for detail | s/enter toggle panel${errorHint}${tabHints}`;
}

export function renderFooter(
  activeTab: ViewerTab,
  hasError: boolean,
  notice: string | null,
  hasLayout: boolean,
) {
  const controls = renderFooterLine(activeTab, hasError, hasLayout);

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

function renderTabChips(
  activeTab: ViewerTab,
  hasLayout: boolean,
  onTabSelect: (tab: ViewerTab) => void,
) {
  return getAvailableTabs(hasLayout).map((tab, index) => {
    const isActive = tab === activeTab;
    const label = `${index + 1} ${tab}`;

    return (
      <text
        key={`tab-chip-${tab}`}
        wrapMode="none"
        fg={isActive ? THEME.background : THEME.accent}
        bg={isActive ? THEME.accent : THEME.panel}
        onMouseDown={() => onTabSelect(tab)}
      >
        {isActive ? ` [${label}] ` : `  ${label}  `}
      </text>
    );
  });
}
