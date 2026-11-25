import type { RequestTools } from "./bucket.ts";
import { flagError } from "./error.ts";
import type { Step } from "./step.ts";
import { consume, isPromiseLike, terminateIterable } from "./utils.ts";

export function listStreamRequested(step: Step, request: RequestTools) {
  const conditions = step._listStreamConditions;
  return (
    conditions === true ||
    (conditions !== null &&
      conditions.some((name) => request.variableValues[name] !== false))
  );
}

/** An array can contain rejected promises even in its unread tail. Observe
 * them immediately and expose each settled entry through next(). */
export function arrayContinuation(items: any[], start: number) {
  let tail: any[] | null = null;
  for (let i = start; i < items.length; i++) {
    const item = items[i];
    if (isPromiseLike(item)) {
      tail ??= items.slice(start);
      tail[i - start] = Promise.resolve(item).catch(flagError);
    }
  }
  if (tail !== null) {
    return (async function* (values) {
      yield* values;
    })(tail);
  }
  return (function* () {
    for (let i = start; i < items.length; i++) {
      yield items[i];
    }
  })();
}

const lastItems = new WeakMap<object, Promise<unknown>>();

export function observedLastItem(iterator: object) {
  return lastItems.get(iterator);
}

/** Keep only the last visible item while the output plan consumes the list. */
export function trackLastItem(
  source: Iterator<any> | AsyncIterator<any>,
  signal: AbortSignal,
): AsyncIterator<any> {
  const completion = Promise.withResolvers<unknown>();
  completion.promise.catch(() => {});
  let last: unknown;
  let done = false;
  const iterator: AsyncIterator<any> = {
    async next() {
      try {
        const result = await source.next();
        if (result.done) {
          done = true;
          completion.resolve(last);
        } else {
          last = result.value;
        }
        return result;
      } catch (e) {
        done = true;
        completion.reject(e);
        throw e;
      }
    },
    async return() {
      try {
        // The output path can disappear while deferred page info is still live.
        while (!done && !signal.aborted) {
          await iterator.next();
        }
      } finally {
        if (signal.aborted) {
          completion.reject(signal.reason);
        }
        await source.return?.();
      }
      return { done: true, value: undefined };
    },
  };
  lastItems.set(iterator, completion.promise);
  return iterator;
}

/** Independent projections can all be skipped. Start the shared fallback
 * read only when the first surviving projection executes. */
export class MaterializedList {
  private result: Promise<unknown[]> | null = null;
  private iterator: AsyncIterator<unknown> | Iterator<unknown> | null = null;
  private closing: Promise<void> | null = null;
  private readonly cancel: () => void;
  private readonly source: AsyncIterable<unknown> | Iterable<unknown>;
  private readonly request: RequestTools;
  private readonly chargeRows: boolean;
  constructor(
    source: AsyncIterable<unknown> | Iterable<unknown>,
    request: RequestTools,
    chargeRows: boolean,
  ) {
    this.source = source;
    this.request = request;
    this.chargeRows = chargeRows;
    this.cancel = () => consume(this.close());
    if (request.abortSignal.aborted) {
      this.cancel();
    } else {
      request.abortSignal.addEventListener("abort", this.cancel, {
        once: true,
      });
    }
  }

  read() {
    return (this.result ??= this.collect());
  }

  private close() {
    return (this.closing ??= (async () => {
      if (this.iterator === null) {
        terminateIterable(this.source);
      } else {
        await this.iterator.return?.();
      }
    })());
  }

  private async collect() {
    const rows: unknown[] = [];
    const { abortSignal } = this.request;
    try {
      abortSignal.throwIfAborted();
      const iterator = (this.iterator =
        Symbol.asyncIterator in this.source
          ? this.source[Symbol.asyncIterator]()
          : this.source[Symbol.iterator]());
      for (;;) {
        const result = await iterator.next();
        abortSignal.throwIfAborted();
        if (result.done) {
          break;
        }
        if (this.chargeRows) {
          this.request.retainStreamRows(1);
        }
        rows.push(result.value);
      }
    } catch (e) {
      await this.close();
      throw e;
    } finally {
      abortSignal.removeEventListener("abort", this.cancel);
    }
    return rows;
  }
}

// Batch caches may return the same loader result for several parent indices,
// including in later buckets. Retain one replayable result per source.
const cachedLists = new WeakMap<
  RequestTools,
  WeakMap<AsyncIterable<unknown>, MaterializedList>
>();

export function readCachedList(
  source: AsyncIterable<unknown>,
  request: RequestTools,
  chargeRows: boolean,
) {
  let lists = cachedLists.get(request);
  if (lists === undefined) {
    lists = new WeakMap();
    cachedLists.set(request, lists);
  }
  let list = lists.get(source);
  if (list === undefined) {
    list = new MaterializedList(source, request, chargeRows);
    lists.set(source, list);
  }
  return list.read();
}
