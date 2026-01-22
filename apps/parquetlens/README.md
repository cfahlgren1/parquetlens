# parquetlens

A fast, interactive TUI for viewing Parquet files. Like [csvlens](https://github.com/YS-L/csvlens) but for Parquet.

![parquetlens](https://github.com/user-attachments/assets/4ad68486-e544-4ef1-aa50-5ca855407259)

## Install

```bash
npm install -g parquetlens
```

Or run directly with npx:

```bash
npx parquetlens data.parquet
```

## Usage

```bash
parquetlens <file|url|-> [options]
```

**Options:**

- `--limit <n>` - Number of rows to show (default: 20)
- `--columns <a,b,c>` - Comma-separated column list
- `--schema` - Print schema only
- `--no-schema` - Skip schema output
- `--json` - Output rows as JSON lines
- `--tui` - Open interactive viewer (default)
- `--plain` / `--no-tui` - Disable interactive viewer

**Examples:**

```bash
# View local file
parquetlens data.parquet

# View with column selection
parquetlens data.parquet --columns city,state

# Fetch from URL (e.g., Hugging Face datasets)
parquetlens https://huggingface.co/datasets/cfahlgren1/hub-stats/resolve/main/daily_papers.parquet

# Hugging Face shortcut
parquetlens hf://datasets/cfahlgren1/hub-stats/daily_papers.parquet

# Pipe from stdin
parquetlens - < data.parquet

# Plain output (no TUI)
parquetlens data.parquet --plain --limit 100
```

## TUI Controls

| Key             | Action                |
| --------------- | --------------------- |
| `j/k` or arrows | Scroll rows           |
| `h/l`           | Jump columns          |
| `PgUp/PgDn`     | Page scroll           |
| Mouse wheel     | Scroll                |
| Click cell      | Open detail panel     |
| `s` or `Enter`  | Toggle detail panel   |
| `x` or `Esc`    | Close panel (or quit) |
| `q`             | Quit                  |

## Features

- **Fast**: Uses duckdb-wasm with HTTP range requests
- **Interactive TUI**: Full-screen terminal UI with mouse support
- **URL Support**: Read parquet files from URLs (including `hf://`)
- **Column Types**: Shows Arrow schema types in headers
- **Cell Detail**: Click any cell to see full content
- **Streaming**: Reads only the rows you need

## License

MIT
