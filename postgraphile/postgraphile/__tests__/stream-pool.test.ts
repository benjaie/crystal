import type { PgOrderSpec, PgSelectQueryBuilder } from "@dataplan/pg";
import { TYPES } from "@dataplan/pg";
import { makePgService } from "@dataplan/pg/adaptors/pg";
import { execute, hookArgs } from "grafast";
import { parse, validate } from "grafast/graphql";
import { StreamDeferPlugin } from "graphile-build";
import {
  EXPORTABLE,
  makeAddPgTableOrderByPlugin,
  makeWrapPlansPlugin,
  orderByAscDesc,
} from "graphile-utils";
import { Pool } from "pg";

import {
  createTestDatabase,
  dropTestDatabase,
} from "../../../grafast/dataplan-pg/__tests__/sharedHelpers.ts";
import { makeSchema } from "../src/index.ts";
import AmberPreset from "../src/presets/amber.ts";

const statements: string[] = [];
beforeEach(() => {
  statements.length = 0;
});
let databaseName: string;
let pool: Pool;
let built: Awaited<ReturnType<typeof makeSchema>>;
beforeAll(async () => {
  const database = await createTestDatabase();
  databaseName = database.databaseName;
  // A queued acquisition must fail promptly if streaming regresses, allowing
  // execution to finish and clean up its already-initialized iterators.
  pool = new Pool({
    connectionString: database.connectionString,
    max: 1,
    connectionTimeoutMillis: 1000,
  });
  pool.on("connect", (client) => {
    const query = client.query;
    client.query = function (options: any, ...args: any[]) {
      statements.push(typeof options === "string" ? options : options.text);
      return query.call(this, options, ...args);
    } as typeof query;
  });
  await pool.query(`
    create table public.items (id int primary key, parent_id int references public.items, rank int);
    create index on public.items (parent_id);
    create index on public.items (rank, id);
    insert into public.items select i, 1, case when i % 4 = 0 then null else i % 7 end from generate_series(1, 1000) i;
  `);
  built = await makeSchema({
    extends: [AmberPreset],
    plugins: [
      StreamDeferPlugin,
      makeAddPgTableOrderByPlugin(
        { schemaName: "public", tableName: "items" },
        ({ sql }) => {
          const expressions = EXPORTABLE(
            (TYPES, sql) =>
              (
                queryBuilder: PgSelectQueryBuilder,
              ): Omit<PgOrderSpec, "direction">[] => [
                {
                  fragment: sql`(${queryBuilder.alias}.rank + 1)`,
                  codec: TYPES.int,
                  nullable: true,
                },
                {
                  attribute: "id",
                  callback: (fragment) => [
                    sql`(${fragment} % 13)`,
                    TYPES.int,
                    false,
                  ],
                },
                {
                  fragment: sql`(-${queryBuilder.alias}.id)`,
                  codec: TYPES.int,
                  nullable: false,
                },
              ],
            [TYPES, sql],
          );
          return {
            ...orderByAscDesc("EXPRESSION_NULLS_FIRST", expressions, {
              unique: true,
              nulls: "first",
            }),
            ...orderByAscDesc("EXPRESSION_NULLS_LAST", expressions, {
              unique: true,
              nulls: "last",
            }),
          };
        },
      ),
    ],
    pgServices: [makePgService({ pool, schemas: ["public"], pubsub: false })],
  });
});
afterAll(async () => {
  await pool?.end();
  await dropTestDatabase(databaseName);
});

async function run(source: string, schemaResult = built) {
  const args = { schema: schemaResult.schema, document: parse(source) };
  expect(validate(schemaResult.schema, args.document)).toEqual([]);
  await hookArgs(args, schemaResult.resolvedPreset, {});
  const result = await execute(args, schemaResult.resolvedPreset);
  expect(Symbol.asyncIterator in result).toBe(true);
  const payloads: any[] = [];
  for await (const payload of result as AsyncIterable<any>) {
    payloads.push(payload);
  }
  expect(payloads.flatMap((p) => p.errors ?? [])).toEqual([]);
  expect(payloads.at(-1)).toEqual({ hasNext: false });
  expect((await pool.query("select * from pg_cursors")).rows).toEqual([]);
  return payloads;
}

test("Amber streams four independent large connections through a one-client pool", async () => {
  const payloads = await run(`{
    a: allItems(first: 1000) { nodes @stream(initialCount: 1) { id } }
    b: allItems(first: 999) { nodes @stream(initialCount: 1) { id } }
    c: allItems(first: 998) { nodes @stream(initialCount: 1) { id } }
    d: allItems(first: 997) { nodes @stream(initialCount: 1) { id } }
  }`);
  expect(payloads).toHaveLength(3992);
  expect(
    statements.filter((s) => s.includes("limit 100")).length,
  ).toBeGreaterThanOrEqual(36);
  expect(statements.some((s) => /^(declare|fetch|begin)\b/.test(s))).toBe(
    false,
  );
  expect(statements.some((s) => s.includes(" > "))).toBe(true);
  expect(payloads[0].data).toEqual(
    Object.fromEntries(
      ["a", "b", "c", "d"].map((key) => [
        key,
        { nodes: [{ id: "WyJJdGVtIiwxXQ==" }] },
      ]),
    ),
  );
});

test.each([
  [0, [100, 100, 50]],
  [2, [100, 100, 50]],
  [150, [100, 100, 50]],
  [300, [100, 100, 50]],
] as const)(
  "shares fixed-size SQL pages independently of initialCount: %s",
  async (initialCount, limits) => {
    const args = {
      schema: built.schema,
      document: parse(`{
      allItems(first: 250) { nodes @stream(initialCount: ${initialCount}) { rowId } }
    }`),
    };
    await hookArgs(args, built.resolvedPreset, {});
    const result = await execute(args, built.resolvedPreset);
    const payloads: any[] = [];
    if (Symbol.asyncIterator in result) {
      for await (const payload of result) payloads.push(payload);
    } else {
      payloads.push(result);
    }
    expect(payloads.flatMap((p) => p.errors ?? [])).toEqual([]);
    expect(payloads[0].data.allItems.nodes).toHaveLength(
      Math.min(initialCount, 250),
    );
    expect(nodeIds(payloads)).toEqual(
      Array.from({ length: 250 }, (_, i) => i + 1),
    );
    expect(
      statements.flatMap((s) => {
        const match = /\nlimit (\d+);/.exec(s);
        return match ? [Number(match[1])] : [];
      }),
    ).toEqual(limits);
  },
);

test("nested database work and deferred streams work with one client", async () => {
  const payloads = await run(`{
    a: allItems(first: 1000) {
      nodes @stream(initialCount: 1) { id itemByParentId { id } }
    }
    ... @defer {
      b: allItems(first: 999) { nodes @stream(initialCount: 1) { id } }
    }
  }`);
  expect(payloads[0].data.a.nodes).toEqual([
    { id: "WyJJdGVtIiwxXQ==", itemByParentId: { id: "WyJJdGVtIiwxXQ==" } },
  ]);
  expect(
    payloads.some((p) => p.data?.b?.nodes?.[0]?.id === "WyJJdGVtIiwxXQ=="),
  ).toBe(true);
  expect(payloads).toHaveLength(2000);
});

test("initializes nested large streams without retaining the only client", async () => {
  const payloads = await run(`{
    allItems(first: 1000) {
      nodes @stream(initialCount: 1) {
        id
        itemsByParentId(first: 999) {
          nodes @stream(initialCount: 1) { id }
        }
      }
    }
  }`);
  expect(payloads[0].data.allItems.nodes[0].itemsByParentId.nodes).toEqual([
    { id: "WyJJdGVtIiwxXQ==" },
  ]);
  expect(payloads).toHaveLength(1999);
});

function nodeIds(payloads: any[]) {
  return [
    ...payloads[0].data.allItems.nodes.map((node: any) => node.rowId),
    ...payloads
      .filter((p) => p.path?.[0] === "allItems" && p.path?.[1] === "nodes")
      .map((p) => p.data.rowId),
  ];
}

test.each([
  ["ASC", "FIRST"],
  ["ASC", "LAST"],
  ["DESC", "FIRST"],
  ["DESC", "LAST"],
])(
  "streams unique expressions ordered %s nulls %s",
  async (direction, nulls) => {
    const expected = await pool.query(
      `select id from public.items order by (rank + 1) ${direction.toLowerCase()} nulls ${nulls.toLowerCase()}, (id % 13) ${direction.toLowerCase()}, (-id) ${direction.toLowerCase()} limit 857 offset 11`,
    );
    const payloads = await run(`{
    allItems(first: 857, offset: 11, orderBy: EXPRESSION_NULLS_${nulls}_${direction}) {
      nodes @stream(initialCount: 1) { rowId }
    }
  }`);
    expect(nodeIds(payloads)).toEqual(expected.rows.map((r) => r.id));
    const batches = statements.filter((s) => s.includes("limit 100"));
    expect(batches).toHaveLength(8);
    expect(batches.some((s) => s.includes("is not distinct from"))).toBe(true);
    expect(batches.some((s) => s.includes("% 13) ="))).toBe(true);
  },
);

test.each(["ASC", "DESC"])(
  "keyset batches preserve nullable %s ordering and offset",
  async (direction) => {
    const expected = await pool.query(
      `select id from public.items order by rank ${direction.toLowerCase()}, id asc limit 857 offset 11`,
    );
    const payloads = await run(
      `{ allItems(first: 857, offset: 11, orderBy: [RANK_${direction}, PRIMARY_KEY_ASC]) { nodes @stream(initialCount: 3) { rowId } } }`,
    );
    expect(nodeIds(payloads)).toEqual(expected.rows.map((r) => r.id));
    expect(statements.some((s) => s.includes("is not distinct from"))).toBe(
      true,
    );
  },
);

test.each(["ASC", "DESC"])(
  "keyset batches respect supplied cursors with nullable %s ordering",
  async (direction) => {
    const args = {
      schema: built.schema,
      document:
        parse(`{ allItems(orderBy: [RANK_${direction}, PRIMARY_KEY_ASC]) {
        edges { cursor node { rowId } }
      } }`),
    };
    await hookArgs(args, built.resolvedPreset, {});
    const result: any = await execute(args, built.resolvedPreset);
    expect(result.errors).toBeUndefined();
    const edges = result.data.allItems.edges;
    const payloads = await run(`{
      allItems(first: 900, offset: 7, orderBy: [RANK_${direction}, PRIMARY_KEY_ASC],
        after: "${edges[49].cursor}", before: "${edges[850].cursor}") {
        nodes @stream(initialCount: 0) { rowId }
      }
    }`);
    expect(nodeIds(payloads)).toEqual(
      edges.slice(57, 850).map((e: any) => e.node.rowId),
    );
    expect(
      statements.filter((s) => s.includes("limit 100")).length,
    ).toBeGreaterThan(1);
  },
);

test("materializes backward pagination rather than incorrectly seeking forwards", async () => {
  const payloads = await run(
    `{ allItems(last: 250) { nodes @stream(initialCount: 1) { rowId } } }`,
  );
  expect(nodeIds(payloads)).toEqual(
    Array.from({ length: 250 }, (_, i) => 751 + i),
  );
  expect(statements.some((s) => s.includes("limit 100"))).toBe(false);
});

test("shares SQL pages between nodes, edges, and immediate pageInfo", async () => {
  const payloads = await run(`{ allItems(first: 250) {
    nodes @stream(initialCount: 1) { rowId }
    edges @stream(initialCount: 2) { node { rowId } }
    pageInfo { hasNextPage endCursor }
  } }`);
  expect(nodeIds(payloads)).toEqual(
    Array.from({ length: 250 }, (_, i) => i + 1),
  );
  const edges = [
    ...payloads[0].data.allItems.edges,
    ...payloads.filter((p) => p.path?.[1] === "edges").map((p) => p.data),
  ];
  expect(edges.map((e) => e.node.rowId)).toEqual(nodeIds(payloads));
  expect(payloads[0].data.allItems.pageInfo.hasNextPage).toBe(true);
  expect(
    JSON.parse(
      Buffer.from(
        payloads[0].data.allItems.pageInfo.endCursor,
        "base64",
      ).toString(),
    ).at(-1),
  ).toBe(250);
  expect(
    statements.filter((s) => /from "public"\."items"/.test(s)),
  ).toHaveLength(3);
});

test("stops fetching keyset pages when the response is cancelled", async () => {
  const args = {
    schema: built.schema,
    document: parse(
      `{ allItems(first: 1000) { nodes @stream(initialCount: 1) { rowId } } }`,
    ),
  };
  await hookArgs(args, built.resolvedPreset, {});
  const result = (await execute(
    args,
    built.resolvedPreset,
  )) as AsyncGenerator<any>;
  expect((await result.next()).value.data.allItems.nodes).toEqual([
    { rowId: 1 },
  ]);
  await result.return(undefined);
  // A query already in flight can finish; no further pages should start.
  await pool.query("select 1");
  const pageCount = statements.filter((s) => s.includes("limit 100")).length;
  expect(pageCount).toBeLessThan(10);
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(statements.filter((s) => s.includes("limit 100"))).toHaveLength(
    pageCount,
  );
  expect((await pool.query("select * from pg_cursors")).rows).toEqual([]);
});

test("shares pages with deferred pageInfo without materializing the connection", async () => {
  const payloads = await run(`{ allItems(first: 900) {
    edges @stream(initialCount: 3) { node { rowId } }
    nodes @stream(initialCount: 2) { rowId }
    ... @defer { pageInfo { hasNextPage hasPreviousPage startCursor endCursor } }
  } }`);
  expect(payloads[0].data.allItems.nodes).toHaveLength(2);
  expect(payloads[0].data.allItems.edges).toHaveLength(3);
  expect(payloads[0].data.allItems.pageInfo).toBeUndefined();
  expect(nodeIds(payloads)).toEqual(
    Array.from({ length: 900 }, (_, i) => i + 1),
  );
  const info = payloads.find((p) => p.data?.pageInfo)?.data.pageInfo;
  expect(info).toMatchObject({ hasNextPage: true, hasPreviousPage: false });
  expect(
    JSON.parse(Buffer.from(info.startCursor, "base64").toString()).at(-1),
  ).toBe(1);
  expect(
    JSON.parse(Buffer.from(info.endCursor, "base64").toString()).at(-1),
  ).toBe(900);
  // Nine full pages and one lookahead row, shared across all consumers.
  expect(
    statements.filter((s) => /from "public"\."items"/.test(s)),
  ).toHaveLength(10);
});

test("immediate pageInfo can finish when streamed nodes have stopped at initialCount", async () => {
  const payloads = await run(`{ allItems(first: 900) {
    nodes @stream(initialCount: 2) { rowId }
    pageInfo { endCursor hasNextPage }
  } }`);
  expect(payloads[0].data.allItems.nodes).toHaveLength(2);
  expect(payloads[0].data.allItems.pageInfo.hasNextPage).toBe(true);
  expect(
    JSON.parse(
      Buffer.from(
        payloads[0].data.allItems.pageInfo.endCursor,
        "base64",
      ).toString(),
    ).at(-1),
  ).toBe(900);
  expect(nodeIds(payloads)).toEqual(
    Array.from({ length: 900 }, (_, i) => i + 1),
  );
});

test("pageInfo planned before nodes still uses independent iterators", async () => {
  const payloads = await run(`{ allItems(first: 250) {
    pageInfo { endCursor startCursor hasNextPage }
    nodes @stream(initialCount: 2) { rowId }
  } }`);
  expect(payloads[0].data.allItems.pageInfo.hasNextPage).toBe(true);
  expect(nodeIds(payloads)).toHaveLength(250);
});

test.each([0, 100, 1000, 1100])(
  "streamed pageInfo handles first: %s and end of data",
  async (first) => {
    const args = {
      schema: built.schema,
      document: parse(`{ allItems(first: ${first}) {
    nodes @stream(initialCount: 0) { rowId }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  } }`),
    };
    await hookArgs(args, built.resolvedPreset, {});
    const result = await execute(args, built.resolvedPreset);
    const payloads: any[] = [];
    if (Symbol.asyncIterator in result) {
      for await (const payload of result) payloads.push(payload);
    } else payloads.push(result);
    expect(payloads.flatMap((p) => p.errors ?? [])).toEqual([]);
    const info = payloads[0].data.allItems.pageInfo;
    expect(info.hasNextPage).toBe(first > 0 && first < 1000);
    expect(info.hasPreviousPage).toBe(false);
    expect(nodeIds(payloads)).toHaveLength(Math.min(first, 1000));
    if (first === 0)
      expect(info).toMatchObject({ startCursor: null, endCursor: null });
  },
);

test("configures PgSelect page size, cache capacity, and idle timeout", async () => {
  const smallCacheSchema = await makeSchema({
    extends: [AmberPreset],
    plugins: [
      StreamDeferPlugin,
      makeWrapPlansPlugin({
        Query: {
          allItems(plan) {
            const $connection = plan() as any;
            $connection.getSubplan().setStreamOptions({
              pageSize: 37,
              maxPages: 1,
              consumerIdleTimeout: 5,
            });
            return $connection;
          },
        },
      }),
    ],
    pgServices: [makePgService({ pool, schemas: ["public"], pubsub: false })],
  });
  statements.length = 0;
  const immediate = await run(
    `{ allItems(first: 120) {
    nodes @stream(initialCount: 2) { rowId }
    pageInfo { endCursor hasNextPage }
  } }`,
    smallCacheSchema,
  );
  expect(nodeIds(immediate)).toEqual(
    Array.from({ length: 120 }, (_, i) => i + 1),
  );
  expect(immediate[0].data.allItems.pageInfo.hasNextPage).toBe(true);
  const immediateQueries = statements.filter((s) =>
    /from "public"\."items"/.test(s),
  );
  expect(immediateQueries.length).toBeGreaterThan(4);
  expect(immediateQueries[0]).toContain("limit 37");

  statements.length = 0;
  const deferred = await run(
    `{ allItems(first: 120) {
    nodes @stream(initialCount: 2) { rowId }
    edges @stream(initialCount: 3) { node { rowId } }
    ... @defer { pageInfo { startCursor endCursor hasNextPage } }
  } }`,
    smallCacheSchema,
  );
  expect(nodeIds(deferred)).toHaveLength(120);
  // Deferred work can register after the one-page cache has already advanced.
  // Refetching remains correct and does not retain a database client.
  expect(
    statements.filter((s) => /from "public"\."items"/.test(s)).length,
  ).toBeGreaterThanOrEqual(4);
});

test("a non-streamed field and a streamed field independently traverse shared rows", async () => {
  const payloads = await run(`{ allItems(first: 250) {
    nodes @stream(initialCount: 0) { rowId }
    edges { node { rowId } }
    ... @defer { pageInfo { endCursor hasNextPage } }
  } }`);
  const expected = Array.from({ length: 250 }, (_, i) => i + 1);
  expect(
    payloads[0].data.allItems.edges.map((edge: any) => edge.node.rowId),
  ).toEqual(expected);
  expect(nodeIds(payloads)).toEqual(expected);
});
