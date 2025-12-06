export function inspect(
  obj: any,
  options?: { depth?: number; breakLength?: number; compact?: boolean },
): string {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

export default { inspect };

