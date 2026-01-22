# Streaming Parquet via DuckDB-WASM HTTP Range Requests

## Overview

Replace full-file downloads with HTTP range requests using DuckDB-WASM's filesystem layer. DuckDB issues range requests for remote parquet files and keeps connections open for efficient pagination.

## Goals

1. **Fast initial load** - Fetch metadata + first page in ~100ms instead of downloading entire file
2. **Efficient pagination** - Only fetch row groups needed for current view
3. **Custom URL schemes** - Support `hf://` for Hugging Face datasets
4. **Backwards compatible** - Local files continue to work as before

## URL Scheme Support

| Scheme     | Example                                           | Resolves To                                                                 |
| ---------- | ------------------------------------------------- | --------------------------------------------------------------------------- |
| `http://`  | `http://example.com/data.parquet`                 | Direct URL                                                                  |
| `https://` | `https://example.com/data.parquet`                | Direct URL                                                                  |
| `hf://`    | `hf://datasets/username/repo/file.parquet`        | `https://huggingface.co/datasets/username/repo/resolve/main/file.parquet`   |
| `hf://`    | `hf://datasets/username/repo@branch/file.parquet` | `https://huggingface.co/datasets/username/repo/resolve/branch/file.parquet` |
| (none)     | `./data.parquet`                                  | Local file path                                                             |

## Implementation Plan

### Phase 1: URL Resolution Layer

**File:** `packages/parquet-reader/src/urls.ts`

```ts
export type ResolvedUrl = {
  url: string;
  headers?: Record<string, string>;
};

export function resolveParquetUrl(input: string): ResolvedUrl | null {
  if (input.startsWith("hf://")) {
    return resolveHuggingFaceUrl(input);
  }
  if (input.startsWith("http://") || input.startsWith("https://")) {
    return { url: input };
  }
  return null; // Local file
}

function resolveHuggingFaceUrl(input: string): ResolvedUrl {
  // hf://datasets/user/repo@branch/path/to/file.parquet
  // hf://models/user/repo/path/to/file.parquet
  const match = input.match(/^hf:\/\/(datasets|models)\/([^@\/]+)\/([^@\/]+)(?:@([^\/]+))?\/(.+)$/);
  if (!match) {
    throw new Error(`Invalid hf:// URL: ${input}`);
  }
  const [, type, user, repo, branch = "main", path] = match;
  return {
    url: `https://huggingface.co/${type}/${user}/${repo}/resolve/${branch}/${path}`,
  };
}
```

### Phase 2: DuckDB Streaming Reader

**File:** `packages/parquet-reader/src/index.ts`

Add new exports for URL-based reading:

```ts
import { DuckDBDataProtocol, DuckDBConnection, DuckDBBindings } from "@duckdb/duckdb-wasm/blocking";

export type ParquetSource = {
  readTable: (options?: ParquetReadOptions) => Promise<Table>;
  readMetadata: () => Promise<ParquetFileMetadata>;
  close: () => Promise<void>;
};

export async function openParquetSourceFromUrl(url: string): Promise<ParquetSource> {
  const resolved = resolveParquetUrl(url);
  if (!resolved) {
    throw new Error("Not a URL");
  }

  const db = await getDuckDb();
  const conn = db.connect();
  const fileName = buildDuckDbFileName(resolved.url);

  db.registerFileURL(fileName, resolved.url, DuckDBDataProtocol.HTTP, true);

  return createParquetSource(db, conn, fileName);
}
```

### Phase 3: Update CLI (main.ts)

Replace `fetchUrlToBuffer()` with streaming approach:

```ts
async function loadTable(input: string, readOptions: ParquetReadOptions): Promise<Table> {
  const resolved = resolveParquetUrl(input);

  if (resolved) {
    // Remote URL - use streaming
    const source = await openParquetFromUrl(input);
    try {
      return await source.readTable(readOptions);
    } finally {
      source.close();
    }
  }

  // Local file
  return readParquetTableFromPath(input, readOptions);
}
```

### Phase 4: Update TUI (tui.tsx)

Modify to accept DuckDB-backed sources and re-fetch pages efficiently:

```ts
type SourceType = { type: "input"; input: string; source: ParquetSource };

// In App component, keep the ParquetSource open
// and call source.readTable({ offset, limit }) for each page
```

### Phase 5: Remove Old Code

Delete:

- `fetchUrlToBuffer()`
- `fetchUrlToTempFile()`
- Full-file download logic for URLs

## File Changes Summary

| File                                   | Changes                                                 |
| -------------------------------------- | ------------------------------------------------------- |
| `packages/parquet-reader/src/urls.ts`  | New file - URL resolution                               |
| `packages/parquet-reader/src/index.ts` | Add DuckDB-backed `openParquetSource*()` helpers        |
| `apps/parquetlens/src/main.ts`         | Use streaming for URLs, remove download logic           |
| `apps/parquetlens/src/tui.tsx`         | Accept URL sources, keep connection open for pagination |
| `apps/parquetlens/src/main.test.ts`    | Add local parquet fixture test                          |

## Example Usage After Implementation

```bash
# Hugging Face datasets
parquetlens hf://datasets/cfahlgren1/hub-stats/daily_papers.parquet

# With branch
parquetlens hf://datasets/cfahlgren1/hub-stats@main/daily_papers.parquet

# Direct HTTPS (existing)
parquetlens https://huggingface.co/datasets/cfahlgren1/hub-stats/resolve/main/daily_papers.parquet

# Local files (unchanged)
parquetlens ./data.parquet
```

## Future Enhancements

- [ ] `s3://` URL support
- [ ] `gs://` (Google Cloud Storage) support
- [ ] Authentication headers for private repos (`HF_TOKEN`)
- [ ] Caching of metadata for repeated access
- [ ] Progress indicator during initial metadata fetch
