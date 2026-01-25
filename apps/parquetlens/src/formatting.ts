export function safeStringify(value: unknown): string {
  try {
    return (
      JSON.stringify(value, (_key, current) => {
        if (typeof current === "bigint") {
          return current.toString();
        }

        if (current instanceof Date) {
          return current.toISOString();
        }

        if (current instanceof Uint8Array) {
          return Array.from(current);
        }

        if (current instanceof Map) {
          return Object.fromEntries(current.entries());
        }

        if (current instanceof Set) {
          return Array.from(current.values());
        }

        return current;
      }) ?? ""
    );
  } catch {
    return String(value);
  }
}
