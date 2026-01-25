import { describe, expect, it } from "vitest";

import { safeStringify } from "./formatting.js";

describe("safeStringify", () => {
  it("renders bigint arrays as JSON-friendly strings", () => {
    const value = [1n, 2n, 3n];

    expect(safeStringify(value)).toBe("[\"1\",\"2\",\"3\"]");
  });

  it("renders arrays of objects with nested maps", () => {
    const value = [{ tags: new Map([["alpha", "beta"]]) }];

    expect(safeStringify(value)).toBe("[{\"tags\":{\"alpha\":\"beta\"}}]");
  });
});
