# parquetlens

[![npm version](https://img.shields.io/npm/v/parquetlens)](https://npmjs.com/package/parquetlens)
[![license](https://img.shields.io/npm/l/parquetlens)](https://github.com/cfahlgren1/parquetlens/blob/main/LICENSE)

fast parquet previewer with a csvlens-style tui. like [csvlens](https://github.com/YS-L/csvlens) but for parquet files, with streaming support for large files and remote URLs.

<img width="1726" height="994" alt="parquetlens TUI showing a parquet file with columns for id, name, city, and state in an interactive table view" src="https://github.com/user-attachments/assets/4ad68486-e544-4ef1-aa50-5ca855407259" />

## usage

```bash
npm install -g parquetlens
```

or run directly with npx:

```bash
npx parquetlens data.parquet
```

requires Node.js 20+

## features

- **interactive TUI** - navigate large datasets with keyboard and mouse
- **streaming** - handles large files efficiently with lazy loading
- **remote files** - load parquet files directly from HTTP URLs
- **hugging face integration** - use `hf://` URLs to load datasets directly
- **SQL queries** - run SQL queries directly on parquet files
- **schema inspection** - view column types and metadata
- **detail panel** - expand cells to view and copy full content

## options

```bash
parquetlens <file|url|-> [options]
```

| Option                 | Description                              |
| ---------------------- | ---------------------------------------- |
| `--limit <n>`          | number of rows to show (default 20)      |
| `--columns <a,b,c>`    | comma-separated column list              |
| `--sql <query>`        | run SQL query (use `data` as table name) |
| `--schema`             | print schema only                        |
| `--no-schema`          | skip schema output                       |
| `--json`               | output rows as json lines                |
| `--tui`                | open interactive viewer (default)        |
| `--plain` / `--no-tui` | disable interactive viewer               |

## examples

```bash
# local file
parquetlens data.parquet

# select specific columns
parquetlens data.parquet --columns city,state

# limit rows
parquetlens data.parquet --limit 100

# plain text output (no TUI)
parquetlens data.parquet --plain

# remote file via HTTP
parquetlens https://example.com/data.parquet

# hugging face dataset
parquetlens hf://datasets/username/dataset/data/train.parquet

# run SQL query
parquetlens data.parquet --sql "SELECT city, COUNT(*) FROM data GROUP BY city"

# read from stdin (useful for piping from other tools)
cat data.parquet | parquetlens -
```

## tui controls

| Key                   | Action                          |
| --------------------- | ------------------------------- |
| `j` / `k` / `↑` / `↓` | scroll rows                     |
| `h` / `l`             | jump columns                    |
| `PgUp` / `PgDn`       | page scroll                     |
| mouse wheel           | scroll                          |
| click cell            | open detail panel               |
| `s` / `Enter`         | toggle detail panel             |
| `x` / `Esc`           | close panel (or quit if closed) |
| `q`                   | quit                            |

## development

```bash
corepack enable pnpm
pnpm install
pnpm -C apps/parquetlens dev -- ./data.parquet
```

## license

MIT
