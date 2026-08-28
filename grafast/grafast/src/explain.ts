/**
 * Returns the explain scopes requested by a client and permitted by the
 * server, or `undefined` when no explains should be generated.
 */
export function getExplain(
  explainAllowed: boolean | readonly string[] | undefined,
  explainHeaders: string[] | string | undefined,
): string[] | undefined {
  if (
    explainAllowed === false ||
    (Array.isArray(explainAllowed) && explainAllowed.length === 0)
  ) {
    return undefined;
  }
  const explainHeader = Array.isArray(explainHeaders)
    ? explainHeaders.join(",")
    : explainHeaders;
  if (typeof explainHeader !== "string") {
    return undefined;
  }
  const explainParts = explainHeader
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  const requested =
    explainAllowed === true || explainAllowed === undefined
      ? explainParts
      : // Assumption: explainAllowed is relatively short (and unique).
        explainAllowed.filter((part) => explainParts.includes(part));

  if (requested.length === 0) {
    return undefined;
  }

  const scopes = new Set<string>();
  for (const scope of requested) {
    const parts = scope.split(":");
    if (parts[0] === "sql") {
      scopes.add("sql");
      if (parts[1] === "explain") {
        scopes.add("sql:explain");
      }
    }
    scopes.add(scope);
  }
  return [...scopes];
}

export function hasExplain(
  explain: boolean | readonly string[] | undefined,
  scope: string,
): boolean {
  return (
    explain === true || (Array.isArray(explain) && explain.includes(scope))
  );
}
