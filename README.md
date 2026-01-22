# parquetlens

fast parquet previewer with a csvlens-style tui.

## quick start

```bash
corepack enable pnpm
pnpm install
pnpm -C apps/parquetlens dev -- ./data.parquet
```

the full-screen tui uses opentui and runs under bun (falls back to plain output if bun is missing).

## usage

```bash
parquetlens <file|-> [options]
```

options:

- `--limit <n>`: number of rows to show (default 20)
- `--columns <a,b,c>`: comma-separated column list
- `--schema`: print schema only
- `--no-schema`: skip schema output
- `--json`: output rows as json lines
- `--tui`: open interactive viewer (default)
- `--plain` / `--no-tui`: disable interactive viewer

examples:

```bash
parquetlens data.parquet
parquetlens data.parquet --columns city,state
parquetlens data.parquet --limit 100
parquetlens data.parquet --plain
parquetlens - < data.parquet
```

## tui controls

- `j/k` or arrows: row scroll
- `h/l`: column jump
- `pgup/pgdn`: page scroll
- mouse wheel: scroll
- click cell: open detail panel (select + copy text)
- `s` or `enter`: toggle detail panel
- `x` / `esc`: close panel (or quit if closed)
- `q`: quit
