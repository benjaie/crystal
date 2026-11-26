export function* arrayContinuation(items: any[], start: number) {
  for (let i = start; i < items.length; i++) {
    yield items[i];
  }
}
