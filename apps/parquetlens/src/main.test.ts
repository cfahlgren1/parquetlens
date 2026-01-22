import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CLI_PATH = path.join(__dirname, "../dist/main.js");
const FIXTURE_PATH = path.join(__dirname, "../test/fixtures/sample.parquet");

function runCli(
  args: string[],
  timeout = 30000,
): Promise<{ stdout: string; stderr: string; code: number }> {
  return new Promise((resolve) => {
    const proc = spawn("node", [CLI_PATH, ...args], {
      timeout,
      stdio: ["ignore", "pipe", "pipe"], // Don't wait for stdin
    });
    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    proc.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    proc.on("close", (code) => {
      resolve({ stdout, stderr, code: code ?? 1 });
    });

    proc.on("error", () => {
      resolve({ stdout, stderr, code: 1 });
    });
  });
}

describe("parquetlens CLI", () => {
  it("shows help with --help", async () => {
    const { stdout, code } = await runCli(["--help"]);
    expect(code).toBe(0);
    expect(stdout).toContain("parquetlens");
    expect(stdout).toContain("--limit");
    expect(stdout).toContain("--columns");
    expect(stdout).toContain("url");
  });

  it("shows error for missing file", async () => {
    const { stderr, code } = await runCli(["nonexistent-file.parquet", "--plain"]);
    expect(code).toBe(1);
    expect(stderr.length).toBeGreaterThan(0);
  });

  it("shows error for unknown option", async () => {
    const { stderr, code } = await runCli(["--unknown-flag"]);
    expect(code).toBe(1);
    expect(stderr).toContain("unknown option");
  });
});

describe("URL support", () => {
  it("includes URL examples in help", async () => {
    const { stdout } = await runCli(["--help"]);
    expect(stdout).toContain("https://");
    expect(stdout).toContain("huggingface.co");
    expect(stdout).toContain("hf://");
  });
});

describe("local parquet fixture", () => {
  it("reads rows from a local parquet file", async () => {
    const { stdout, code } = await runCli([FIXTURE_PATH, "--json", "--no-schema", "--limit", "2"]);

    expect(code).toBe(0);
    const lines = stdout
      .trim()
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    const rows = lines.map((line) => JSON.parse(line));
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0]?.city).toBe("Seattle");
  });
});

describe("SQL queries", () => {
  it("runs a simple SELECT query", async () => {
    const { stdout, code } = await runCli([
      FIXTURE_PATH,
      "--sql",
      "SELECT * FROM data LIMIT 2",
      "--json",
    ]);

    expect(code).toBe(0);
    const lines = stdout
      .trim()
      .split("\n")
      .filter(Boolean);
    const rows = lines.map((line) => JSON.parse(line));
    expect(rows.length).toBe(2);
    expect(rows[0]).toHaveProperty("city");
  });

  it("runs a query with WHERE clause", async () => {
    const { stdout, code } = await runCli([
      FIXTURE_PATH,
      "--sql",
      "SELECT * FROM data WHERE city = 'Seattle'",
      "--json",
    ]);

    expect(code).toBe(0);
    const lines = stdout
      .trim()
      .split("\n")
      .filter(Boolean);
    const rows = lines.map((line) => JSON.parse(line));
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.city === "Seattle")).toBe(true);
  });

  it("runs a query with aggregation", async () => {
    const { stdout, code } = await runCli([
      FIXTURE_PATH,
      "--sql",
      "SELECT city, COUNT(*) as count FROM data GROUP BY city ORDER BY count DESC",
      "--json",
    ]);

    expect(code).toBe(0);
    const lines = stdout
      .trim()
      .split("\n")
      .filter(Boolean);
    const rows = lines.map((line) => JSON.parse(line));
    expect(rows.length).toBeGreaterThan(0);
    expect(rows[0]).toHaveProperty("city");
    expect(rows[0]).toHaveProperty("count");
  });

  it("runs a query selecting specific columns", async () => {
    const { stdout, code } = await runCli([
      FIXTURE_PATH,
      "--sql",
      "SELECT city, state FROM data LIMIT 1",
      "--json",
    ]);

    expect(code).toBe(0);
    const lines = stdout
      .trim()
      .split("\n")
      .filter(Boolean);
    const rows = lines.map((line) => JSON.parse(line));
    expect(rows.length).toBe(1);
    expect(Object.keys(rows[0])).toEqual(["city", "state"]);
  });

  it("shows --sql in help", async () => {
    const { stdout, code } = await runCli(["--help"]);
    expect(code).toBe(0);
    expect(stdout).toContain("--sql");
    expect(stdout).toContain("data");
  });

  it("handles SQL errors gracefully", async () => {
    const { stderr, code } = await runCli([
      FIXTURE_PATH,
      "--sql",
      "SELECT * FROM nonexistent_table",
    ]);

    expect(code).toBe(1);
    expect(stderr.length).toBeGreaterThan(0);
  });
});
