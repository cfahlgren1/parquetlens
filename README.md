# parquetlens

fast parquet previewer with a csvlens-style tui.

<img width="1726" height="994" alt="image" src="https://github.com/user-attachments/assets/4ad68486-e544-4ef1-aa50-5ca855407259" />

## install

```bash
npm install -g parquetlens
```

or run directly with npx:

```bash
npx parquetlens data.parquet
```

## development

```bash
corepack enable pnpm
pnpm install
pnpm -C apps/parquetlens dev -- ./data.parquet
```

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
