export interface PgStreamOptions {
  pageSize: number;
  maxPages: number;
  consumerIdleTimeout: number;
}

export const defaultPgStreamOptions: Readonly<PgStreamOptions> = Object.freeze({
  pageSize: 100,
  maxPages: 3,
  consumerIdleTimeout: 100,
});

interface Page<T, K> {
  rows: readonly T[];
  next: K | undefined;
}

/** A request-local, repeatable source. Consumers never retain database clients. */
export function sharedStream<T, K>(
  fetchPage: (key: K | null) => Promise<Page<T, K>>,
  options: Pick<PgStreamOptions, "maxPages" | "consumerIdleTimeout">,
  signal?: AbortSignal,
): AsyncIterable<T> {
  interface Consumer {
    key: string | undefined;
    active: boolean;
    lastActive: number;
    closed: boolean;
  }
  interface CachedPage {
    promise: Promise<Page<T, K>>;
    ready: boolean;
  }
  const consumers = new Set<Consumer>();
  // Insertion order records page age; cache hits don't change it.
  const pages = new Map<string, CachedPage>();
  const listeners = new Set<() => void>();
  const notify = () => {
    for (const listener of [...listeners]) listener();
  };
  const keyFor = (key: K | null) => JSON.stringify(key);

  function wait(delay: number) {
    return new Promise<void>((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const wake = () => {
        if (timer !== undefined) clearTimeout(timer);
        listeners.delete(wake);
        resolve();
      };
      listeners.add(wake);
      if (Number.isFinite(delay)) timer = setTimeout(wake, Math.max(1, delay));
    });
  }

  async function getPage(
    key: K | null,
    consumer: Consumer,
  ): Promise<Page<T, K> | undefined> {
    const id = keyFor(key);
    while (!consumer.closed) {
      const existing = pages.get(id);
      if (existing) return existing.promise;
      if (pages.size < options.maxPages) {
        const entry: CachedPage = {
          ready: false,
          promise: Promise.resolve().then(() => fetchPage(key)),
        };
        pages.set(id, entry);
        entry.promise.then(
          () => {
            entry.ready = true;
            notify();
          },
          () => {
            pages.delete(id);
            notify();
          },
        );
        return entry.promise;
      }

      let unwanted: string | undefined;
      let idle: string | undefined;
      let delay = Infinity;
      const now = performance.now();
      for (const [pageKey, page] of pages) {
        // In-flight requests are shared, never evicted to make a duplicate fetch.
        if (!page.ready) continue;
        const interested = [...consumers].filter((c) => c.key === pageKey);
        if (interested.length === 0) {
          unwanted = pageKey; // Prefer the newest unwanted page.
        } else if (!interested.some((c) => c.active)) {
          const remaining = Math.max(
            ...interested.map(
              (c) => options.consumerIdleTimeout - (now - c.lastActive),
            ),
          );
          if (remaining <= 0)
            idle ??= pageKey; // Oldest eligible page.
          else delay = Math.min(delay, remaining);
        }
      }
      const evict = unwanted ?? idle;
      if (evict !== undefined) {
        pages.delete(evict);
        continue;
      }
      // Activity and interest changes wake us immediately. A timer only asks
      // us to recheck; it is never proof that a consumer is still idle.
      await wait(delay);
    }
    return undefined;
  }

  return {
    [Symbol.asyncIterator]() {
      let nextKey: K | null | undefined = null;
      let rows: readonly T[] = [];
      let position = 0;
      let pending = 0;
      let tail: Promise<unknown> = Promise.resolve();
      const consumer: Consumer = {
        key: keyFor(null),
        active: false,
        lastActive: performance.now(),
        closed: false,
      };
      consumers.add(consumer);
      const cancellations = new Set<() => void>();
      function close(cancel = true) {
        if (consumer.closed) return;
        consumer.closed = true;
        consumer.key = undefined;
        consumers.delete(consumer);
        rows = [];
        signal?.removeEventListener("abort", abort);
        if (cancel) {
          for (const cancelNext of cancellations) cancelNext();
          cancellations.clear();
        }
        notify();
      }
      const abort = () => close();
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) close();
      async function advance(): Promise<IteratorResult<T>> {
        try {
          while (!consumer.closed && position >= rows.length) {
            if (nextKey === undefined) {
              close();
              break;
            }
            const page = await getPage(nextKey, consumer);
            if (consumer.closed || page === undefined) break;
            rows = page.rows;
            position = 0;
            nextKey = page.next;
            consumer.key = nextKey === undefined ? undefined : keyFor(nextKey);
            notify();
          }
          return consumer.closed
            ? { done: true, value: undefined }
            : { done: false, value: rows[position++] };
        } catch (error) {
          close(false);
          throw error;
        }
      }
      return {
        next() {
          if (consumer.closed)
            return Promise.resolve({ done: true as const, value: undefined });
          pending++;
          consumer.active = true;
          notify();
          const cancelled = Promise.withResolvers<IteratorResult<T>>();
          const cancelNext = () =>
            cancelled.resolve({ done: true, value: undefined });
          cancellations.add(cancelNext);
          const result = tail.then(advance);
          tail = result.catch(() => {});
          return Promise.race([result, cancelled.promise]).finally(() => {
            cancellations.delete(cancelNext);
            pending--;
            consumer.lastActive = performance.now();
            consumer.active = pending > 0;
            notify();
          });
        },
        return() {
          close();
          return Promise.resolve({ done: true as const, value: undefined });
        },
        throw(error: unknown) {
          close();
          return Promise.reject(error);
        },
      };
    },
  };
}
