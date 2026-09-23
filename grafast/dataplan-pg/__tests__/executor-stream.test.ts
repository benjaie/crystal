import { constant } from "grafast";
import { Pool } from "pg";

import { makePgAdaptorWithPgClient } from "../src/adaptors/pg.ts";
import { PgExecutor } from "../src/executor.ts";
import { createTestDatabase, dropTestDatabase } from "./sharedHelpers.ts";

let databaseName: string;
let connectionString: string;
beforeAll(async () => {
  ({ databaseName, connectionString } = await createTestDatabase());
});
afterAll(() => dropTestDatabase(databaseName));

async function withStreams(
  callback: (helpers: {
    open: (count?: number, text?: string) => Promise<AsyncIterator<number[]>>;
    query: (text: string) => Promise<unknown>;
    pool: Pool;
    statements: string[];
  }) => Promise<void>,
) {
  // Acquisition also has a deadline: a regression must release already-open
  // streams rather than leaving the test (and its database) hanging forever.
  const pool = new Pool({
    connectionString,
    max: 1,
    connectionTimeoutMillis: 1000,
  });
  const statements: string[] = [];
  pool.on("connect", (client) => {
    const query = client.query;
    client.query = function (options: any, ...args: any[]) {
      statements.push(typeof options === "string" ? options : options.text);
      return query.call(this, options, ...args);
    } as typeof query;
  });
  const context = {
    pgSettings: null,
    withPgClient: makePgAdaptorWithPgClient(pool),
  };
  // This executor is used directly at execution time, not exported in a schema.
  // eslint-disable-next-line graphile-export/export-instances
  const executor = new PgExecutor({
    name: "test",
    context: () => constant(context),
  });
  const iterators: AsyncIterator<number[]>[] = [];
  const inputs = [{ context, queryValues: [] }];
  const openings: Promise<AsyncIterator<number[]>>[] = [];
  try {
    await callback({
      pool,
      statements,
      open(count = 1000, text = "select i from generate_series(1, $1::int) i") {
        const opening = (async () => {
          const { streams } = await executor.executeStream<never, number[]>(
            inputs,
            {
              text,
              rawSqlValues: [count],
              eventEmitter: undefined,
            },
          );
          const stream = await streams[0];
          const iterator = stream[Symbol.asyncIterator]();
          iterators.push(iterator);
          return iterator;
        })();
        openings.push(opening);
        return opening;
      },
      query: (text) =>
        executor.executeWithoutCache(inputs, {
          text,
          rawSqlValues: [],
          eventEmitter: undefined,
        }),
    });
  } finally {
    await Promise.allSettled(openings);
    await Promise.all(iterators.map((iterator) => iterator.return?.()));
    // Acquiring again proves that all stream callbacks were released.
    try {
      const client = await pool.connect();
      try {
        expect((await client.query("select * from pg_cursors")).rows).toEqual(
          [],
        );
      } finally {
        client.release();
      }
    } finally {
      await pool.end();
    }
  }
}

async function consume(iterator: AsyncIterator<number[]>) {
  const rows: number[] = [];
  for (;;) {
    const result = await iterator.next();
    if (result.done) return rows;
    rows.push(result.value[0]);
  }
}

test("initializes independent large streams before consuming any of them", async () => {
  await withStreams(async ({ open, statements }) => {
    const streams = await Promise.all(
      [1000, 999, 998, 997].map((n) => open(n)),
    );
    // Raw executor SQL has no keyset metadata, so it uses materialization.
    expect(statements.some((s) => /^(declare|fetch|begin)\b/.test(s))).toBe(
      false,
    );
    const rows = await Promise.all(streams.map(consume));
    expect(rows.map((r) => r.length)).toEqual([1000, 999, 998, 997]);
    rows.forEach((r) =>
      expect(r).toEqual(Array.from({ length: r.length }, (_, i) => i + 1)),
    );
  });
});

test("runs nested streams and deferred database work while a parent is paused", async () => {
  await withStreams(async ({ open, query }) => {
    const parent = await open();
    expect((await parent.next()).value).toEqual([1]);
    await expect(query("select 42")).resolves.toMatchObject({
      values: [[[42]]],
    });
    const child = await open(999);
    expect(await consume(child)).toHaveLength(999);
    expect(await consume(parent)).toHaveLength(999);
  });
});

test("cancels an unconsumed materialized stream without affecting a sibling", async () => {
  await withStreams(async ({ open }) => {
    const [a, b] = await Promise.all([open(), open(999)]);
    await a.return?.();
    expect(await consume(b)).toHaveLength(999);
  });
});

test("database errors do not affect materialized sibling streams", async () => {
  const log = jest.spyOn(console, "error").mockImplementation(() => {});
  try {
    await withStreams(async ({ open, query }) => {
      const parent = await open();
      await expect(query("select 1 / 0")).rejects.toThrow("division by zero");
      await expect(
        open(1, "select missing_column from generate_series(1, $1::int)"),
      ).rejects.toThrow("does not exist");
      await expect(
        open(1000, "select 1 / (500 - i) from generate_series(1, $1::int) i"),
      ).rejects.toThrow("division by zero");
      expect(await consume(parent)).toHaveLength(1000);
    });
  } finally {
    log.mockRestore();
  }
});

test("cancellation settles a pending next call and releases the client", async () => {
  await withStreams(async ({ open, pool }) => {
    const stream = await open();
    const next = stream.next();
    await stream.return?.();
    await next;
    await expect(pool.query("select 42 as value")).resolves.toMatchObject({
      rows: [{ value: 42 }],
    });
  });
});
