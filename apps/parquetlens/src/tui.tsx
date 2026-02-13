import { createCliRenderer } from "@opentui/core";
import { createRoot } from "@opentui/react";
import React from "react";

import type { ParquetReadOptions, ParquetRow } from "@parquetlens/parquet-reader";
import { openParquetSource } from "@parquetlens/parquet-reader";

import { App, StaticApp } from "./tui/app.js";
import { RESERVED_LINES } from "./tui/constants.js";
import type { GridState, TuiOptions } from "./tui/types.js";
import { buildColumnInfo, resolveInitialTotal } from "./tui/utils.js";

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
