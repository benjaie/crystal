import { sharedStream } from "../src/sharedStream.ts";

async function collect<T>(source: AsyncIterable<T>) {
  const rows: T[] = [];
  for await (const row of source) rows.push(row);
  return rows;
}

function setup(maxPages = 3, rowsPerPage = 1) {
  const fetch = jest.fn(async (key: number | null) => {
    const page = key ?? 0;
    return {
      rows: Array.from(
        { length: rowsPerPage },
        (_, i) => page * rowsPerPage + i,
      ),
      next: page < 4 ? page + 1 : undefined,
    };
  });
  return {
    fetch,
    source: sharedStream(fetch, { maxPages, consumerIdleTimeout: 100 }),
  };
}

afterEach(() => jest.useRealTimers());

test("creates independent lazy iterators and shares in-flight pages", async () => {
  const page = Promise.withResolvers<{ rows: number[]; next: undefined }>();
  const started = Promise.withResolvers<void>();
  const fetch = jest.fn(() => {
    started.resolve();
    return page.promise;
  });
  const source = sharedStream(fetch, { maxPages: 3, consumerIdleTimeout: 100 });
  const a = source[Symbol.asyncIterator]();
  const b = source[Symbol.asyncIterator]();
  expect(a).not.toBe(b);
  expect(fetch).not.toHaveBeenCalled();
  const first = a.next();
  const second = b.next();
  await started.promise;
  expect(fetch).toHaveBeenCalledTimes(1);
  page.resolve({ rows: [1, 2], next: undefined });
  expect(await first).toEqual({ value: 1, done: false });
  expect(await second).toEqual({ value: 1, done: false });
  expect((await a.next()).value).toBe(2);
  expect((await b.next()).value).toBe(2);
  await a.return?.();
  await b.return?.();
});

test("allows an idle consumer's wanted page to be evicted after the idle duration", async () => {
  jest.useFakeTimers();
  const { source, fetch } = setup(1);
  const stopped = source[Symbol.asyncIterator]();
  const fast = source[Symbol.asyncIterator]();
  await fast.next();
  let finished = false;
  const next = fast.next().then((v) => {
    finished = true;
    return v;
  });
  await jest.advanceTimersByTimeAsync(99);
  expect(finished).toBe(false);
  await jest.advanceTimersByTimeAsync(1);
  expect((await next).value).toBe(1);
  expect(fetch.mock.calls.map(([key]) => key)).toEqual([null, 1]);
  expect((await stopped.next()).value).toBe(0);
  await fast.return?.();
  await stopped.return?.();
});

test("consumer activity renews protection instead of imposing a fixed wait deadline", async () => {
  jest.useFakeTimers();
  const { source } = setup(1, 3);
  const slow = source[Symbol.asyncIterator]();
  const fast = source[Symbol.asyncIterator]();
  await slow.next();
  for (let i = 0; i < 6; i++) await fast.next();
  let finished = false;
  const next = fast.next().then((v) => {
    finished = true;
    return v;
  });
  await jest.advanceTimersByTimeAsync(90);
  await slow.next();
  await jest.advanceTimersByTimeAsync(90);
  expect(finished).toBe(false);
  await slow.next();
  await jest.advanceTimersByTimeAsync(99);
  expect(finished).toBe(false);
  await jest.advanceTimersByTimeAsync(1);
  expect((await next).value).toBe(6);
  await slow.return?.();
  await fast.return?.();
});

test("evicts the newest unwanted page, preserving early pages for a stopped consumer", async () => {
  const { source, fetch } = setup();
  const stopped = source[Symbol.asyncIterator]();
  const fast = source[Symbol.asyncIterator]();
  for (let i = 0; i < 4; i++) await fast.next();
  expect((await stopped.next()).value).toBe(0);
  expect((await stopped.next()).value).toBe(1);
  expect(fetch.mock.calls.map(([key]) => key)).toEqual([null, 1, 2, 3]);
  await stopped.return?.();
  await fast.return?.();
});

test("cancelling a stopped consumer wakes a fetch waiting for cache space", async () => {
  jest.useFakeTimers();
  const { source } = setup(1);
  const stopped = source[Symbol.asyncIterator]();
  const fast = source[Symbol.asyncIterator]();
  await fast.next();
  const next = fast.next();
  await stopped.return?.();
  expect((await next).value).toBe(1);
  expect(jest.getTimerCount()).toBe(0);
  await fast.return?.();
});

test("errors close affected iterators and a new iterator can retry", async () => {
  const fetch = jest
    .fn()
    .mockRejectedValueOnce(new Error("fetch failed"))
    .mockResolvedValue({ rows: [42], next: undefined });
  const source = sharedStream<number, number>(fetch, {
    maxPages: 1,
    consumerIdleTimeout: 100,
  });
  await expect(collect(source)).rejects.toThrow("fetch failed");
  expect(await collect(source)).toEqual([42]);
});

test("cancels pending next without cancelling a sibling's shared fetch", async () => {
  const page = Promise.withResolvers<{ rows: number[]; next: undefined }>();
  const source = sharedStream(() => page.promise, {
    maxPages: 1,
    consumerIdleTimeout: 100,
  });
  const a = source[Symbol.asyncIterator]();
  const b = source[Symbol.asyncIterator]();
  const nextA = a.next();
  const nextB = b.next();
  await a.return?.();
  expect(await nextA).toEqual({ done: true, value: undefined });
  page.resolve({ rows: [42], next: undefined });
  expect((await nextB).value).toBe(42);
  await b.return?.();
});

test("serializes overlapping next calls and aborts registered consumers", async () => {
  const controller = new AbortController();
  const source = sharedStream(
    async () => ({ rows: [1, 2, 3], next: undefined }),
    { maxPages: 1, consumerIdleTimeout: 100 },
    controller.signal,
  );
  const iterator = source[Symbol.asyncIterator]();
  expect(await Promise.all([iterator.next(), iterator.next()])).toEqual([
    { value: 1, done: false },
    { value: 2, done: false },
  ]);
  controller.abort();
  expect((await iterator.next()).done).toBe(true);
});

test("uses monotonic time when the wall clock changes", async () => {
  jest.useFakeTimers();
  const { source } = setup(1);
  const stopped = source[Symbol.asyncIterator]();
  const fast = source[Symbol.asyncIterator]();
  await fast.next();
  let finished = false;
  const next = fast.next().then((value) => {
    finished = true;
    return value;
  });
  await jest.advanceTimersByTimeAsync(50);
  jest.setSystemTime(Date.now() - 60_000);
  await jest.advanceTimersByTimeAsync(49);
  expect(finished).toBe(false);
  await jest.advanceTimersByTimeAsync(1);
  expect((await next).value).toBe(1);
  await stopped.return?.();
  await fast.return?.();
});

test("never evicts an in-flight page when a pending next outlasts the idle timeout", async () => {
  jest.useFakeTimers();
  const page = Promise.withResolvers<{ rows: number[]; next: undefined }>();
  const fetch = jest.fn(() => page.promise);
  const source = sharedStream(fetch, { maxPages: 1, consumerIdleTimeout: 100 });
  const a = source[Symbol.asyncIterator]();
  const b = source[Symbol.asyncIterator]();
  const nextA = a.next();
  await jest.advanceTimersByTimeAsync(1000);
  const nextB = b.next();
  await jest.advanceTimersByTimeAsync(1000);
  expect(fetch).toHaveBeenCalledTimes(1);
  page.resolve({ rows: [1], next: undefined });
  expect((await nextA).value).toBe(1);
  expect((await nextB).value).toBe(1);
  await a.return?.();
  await b.return?.();
});
