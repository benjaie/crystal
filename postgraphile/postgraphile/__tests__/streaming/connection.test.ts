import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";

import { makePgService } from "@dataplan/pg/adaptors/pg";
import type { TestDb } from "@localrepo/test-helpers";
import { createTestDatabaseWithPool } from "@localrepo/test-helpers";
import { execute, hookArgs, isAsyncIterable } from "grafast";
import type { DocumentNode, ExecutionPatchResult } from "grafast/graphql";
import { parse, validate, visit } from "grafast/graphql";
import { StreamDeferPlugin } from "graphile-build";
import type { PoolClient } from "pg";

import { makeSchema } from "../../dist/index.js";
import AmberPreset from "../../dist/presets/amber.js";
import { makeV4Preset } from "../../dist/presets/v4.js";

type Variables = Record<string, boolean | number>;
const observe = (client: PoolClient) => jest.spyOn(client, "query");
let db: TestDb;
let queries: ReturnType<typeof observe>;
let api: Awaited<ReturnType<typeof makeSchema>>;
let document: DocumentNode;
let ordinary: DocumentNode;

beforeAll(async () => {
  db = await createTestDatabaseWithPool();
  db.pgPool.options.max = 1;
  db.pgPool.on("connect", (client) => {
    queries = observe(client);
  });
  await db.pgPool.query(
    await readFile(join(__dirname, "../kitchen-sink-data.sql"), "utf8"),
  );
  await db.pgPool.query(`
    insert into c.person(id, person_full_name, email)
      select i, 'Author ' || i, i || '@example.test' from generate_series(100, 125) i;
    insert into a.post(id, author_id, headline, body)
      select 10000 + i * 10 + j, i, 'Headline ' || i, 'Body ' || j
      from generate_series(100, 125) i, generate_series(1, i % 6) j;
    insert into a.post(id, author_id, headline, body)
      select 30000 + 2000 * i + j, i, 'Long page ' || j, null
      from generate_series(1, 2) i, generate_series(1, 1205) j;
  `);
  api = await makeSchema({
    extends: [AmberPreset, makeV4Preset({ simpleCollections: "both" })],
    plugins: [StreamDeferPlugin],
    pgServices: [makePgService({ pool: db.pgPool, schemas: ["a", "b", "c"] })],
    schema: { muteWarnings: true },
    gather: { muteWarnings: true },
  });
  document = parse(
    await readFile(join(__dirname, "connection.graphql"), "utf8"),
  );
  ordinary = visit(document, {
    Directive(node) {
      if (["defer", "stream"].includes(node.name.value)) return null;
    },
    VariableDefinition(node) {
      if (/^(defer|stream|initialCount)/.test(node.variable.name.value)) {
        return null;
      }
    },
  });
  for (const doc of [document, ordinary]) {
    expect(validate(api.schema, doc)).toEqual([]);
  }
}, 30000);

afterAll(async () => {
  await db?.release();
});

async function start(
  doc: DocumentNode,
  variableValues: Variables,
  cap?: number,
) {
  queries.mockClear();
  const resolvedPreset =
    cap === undefined
      ? api.resolvedPreset
      : {
          ...api.resolvedPreset,
          grafast: {
            ...api.resolvedPreset.grafast,
            maxMaterializedStreamRows: cap,
          },
        };
  const args = await hookArgs({
    schema: api.schema,
    resolvedPreset,
    requestContext: {},
    document: doc,
    variableValues,
  });
  return execute(args);
}

function executedSql() {
  return queries.mock.calls.map(
    ([query]: [string | { text: string }, ...unknown[]]) =>
      typeof query === "string" ? query : query.text,
  );
}

async function run(doc: DocumentNode, variableValues: Variables, cap?: number) {
  const result = await start(doc, variableValues, cap);
  let data: any;
  if (isAsyncIterable(result)) {
    let complete = false;
    for await (const payload of result as AsyncIterable<ExecutionPatchResult>) {
      if (payload.errors) throw payload.errors[0];
      if (complete) throw new Error("Unexpected payload after hasNext: false");
      if (data === undefined) {
        data = payload.data;
      } else if (payload.path != null) {
        const parent = payload.path
          .slice(0, -1)
          .reduce<any>((value, key) => value[key], data);
        const key = payload.path[payload.path.length - 1];
        if (typeof key === "number") {
          if (!Array.isArray(parent) || parent.length !== key) {
            throw new Error(`Out-of-order patch at ${payload.path}`);
          }
          parent.push(payload.data);
        } else {
          Object.assign(parent[key], payload.data);
        }
      }
      complete = payload.hasNext === false;
    }
    if (!complete) throw new Error("Missing final incremental payload");
  } else {
    if (result.errors) throw result.errors[0];
    data = result.data;
  }
  return { data, sql: executedSql() };
}

const exportVariables = {
  first: 100000,
  stream1: true,
  skip3: true,
  skip4: true,
};

it.each([
  ["nodes", exportVariables],
  ["deferred nodes", { ...exportVariables, defer2: true }],
  ["deferred outer fragment", { ...exportVariables, defer1: true }],
  [
    "two prefixes and deferred page",
    {
      first: 101,
      stream1: true,
      stream2: true,
      initialCount1: 1,
      initialCount2: 2,
      defer4: true,
    },
  ],
  [
    "upfront lookahead",
    { first: 101, stream1: true, skip3: true, skip5: true, skip6: true },
  ],
] as Array<[string, Variables]>)(
  "streams %s across 25 parents",
  async (_name, vars) => {
    const expected = await run(ordinary, vars);
    const { data, sql } = await run(document, vars, 0);
    expect(data.myOrganizations).toHaveLength(25);
    expect(data).toEqual(expected.data);
    expect(sql.filter((q) => q.startsWith("declare "))).toHaveLength(25);
    expect(sql.filter((q) => q.startsWith("close "))).toHaveLength(25);
    expect(sql.some((q) => q.startsWith("fetch forward 100 "))).toBe(true);
  },
);

it("cancels an export and releases the only client with unread pages", async () => {
  const result = await start(
    document,
    { ...exportVariables, initialCount1: 1 },
    0,
  );
  if (!isAsyncIterable(result))
    throw new Error("Expected incremental delivery");
  const iterator = result[Symbol.asyncIterator]();
  try {
    const initial = await iterator.next();
    expect(initial.value).toMatchObject({ hasNext: true });
  } finally {
    await iterator.return?.();
  }
  await db.pgPool.query("select 1");
  const sql = executedSql();
  const fetches = sql.filter((q) => q.startsWith("fetch forward"));
  expect(fetches.length).toBeGreaterThan(0);
  // Two parents have over 1,200 rows each. Cancelling at the initial payload
  // must not exhaust them to release the transaction.
  for (const fetch of new Set(fetches)) {
    expect(fetches.filter((q) => q === fetch).length).toBeLessThan(12);
  }
  expect(sql.filter((q) => q.startsWith("declare ")).length).toBe(
    sql.filter((q) => q.startsWith("close ")).length,
  );
});

it("batches small sibling pages in a single posts query", async () => {
  const vars = { ...exportVariables, first: 2 };
  const expected = await run(ordinary, vars);
  const { data, sql } = await run(document, vars);
  expect(data).toEqual(expected.data);
  expect(sql.filter((q) => q.includes('"headline"'))).toHaveLength(1);
  expect(sql.some((q) => q.startsWith("declare "))).toBe(false);
});

it("only charges the fallback budget for enabled streams", async () => {
  const vars = { first: 3 };
  const expected = await run(ordinary, vars);
  expect((await run(document, vars, 2)).data).toEqual(expected.data);
  await expect(run(document, { ...vars, stream1: true }, 2)).rejects.toThrow(
    "Materialized stream row limit exceeded",
  );
});

it("returns just the required page boundaries and flags", async () => {
  for (const first of [0, 1, 3, 100000]) {
    const expected = (await run(ordinary, { first })).data.myOrganizations;
    for (let mask = 0; mask < 8; mask++) {
      const skip5 = Boolean(mask & 1);
      const skip6 = Boolean(mask & 2);
      const skip7 = Boolean(mask & 4);
      const { data, sql } = await run(document, {
        first,
        skip2: true,
        skip3: true,
        skip5,
        skip6,
        skip7,
      });
      for (const [i, { members }] of data.myOrganizations.entries()) {
        const page = { ...expected[i].members.pageInfo };
        if (skip5) delete page.startCursor;
        if (skip6) delete page.endCursor;
        if (skip7) delete page.hasNextPage;
        expect(members).toEqual({
          totalCount: expected[i].members.totalCount,
          pageInfo: page,
        });
      }
      if (first > 1 && skip6 && (skip5 || skip7)) {
        if (skip5 && skip7) {
          // The only remaining reference to posts is the requested totalCount.
          expect(sql.join("\n").match(/"a"\."post"/g)).toHaveLength(1);
        } else {
          const limit = skip5 ? `limit 1 offset ${first}` : "limit 1";
          expect(sql.join("\n")).toContain(`as page ${limit}`);
        }
      }
    }
  }
});

function variablesFor(mask: number): Variables {
  const variables: Variables = {
    first: 2,
    initialCount1: mask % 3,
    initialCount2: (mask >>> 2) % 4,
  };
  for (let i = 1; i <= 7; i++) {
    variables[`skip${i}`] = Boolean(mask & (1 << (i - 1)));
    variables[`defer${i}`] = Boolean(mask & (1 << (i + 6)));
  }
  variables.stream1 = Boolean(mask & 16384);
  variables.stream2 = Boolean(mask & 32768);
  return variables;
}

it(
  "assembles conditional payloads in delivery order",
  async () => {
    const exhaustive = process.env.POSTGRAPHILE_STREAM_SWEEP === "all";
    const baselines = new Map<number, unknown>();
    for (let i = 0; i < (exhaustive ? 65536 : 128); i++) {
      const mask = (i * 40503 + 13109) & 65535;
      const skipMask = mask & 127;
      const variables = variablesFor(mask);
      if (!baselines.has(skipMask)) {
        baselines.set(skipMask, (await run(ordinary, variables)).data);
      }
      let deadline: NodeJS.Timeout | undefined;
      try {
        const result = await Promise.race([
          run(document, variables),
          new Promise<never>((_, reject) => {
            deadline = setTimeout(
              () => reject(new Error("Timed out waiting for payloads")),
              6000,
            );
          }),
        ]);
        const expected = baselines.get(skipMask);
        if (!isDeepStrictEqual(result.data, expected)) {
          expect(result.data).toEqual(expected);
        }
      } catch (e) {
        throw new Error(
          `Failed at mask ${mask}, variables ${JSON.stringify(variables)}`,
          { cause: e },
        );
      } finally {
        clearTimeout(deadline);
      }
    }
  },
  process.env.POSTGRAPHILE_STREAM_SWEEP === "all" ? 1800000 : 30000,
);

(process.env.POSTGRAPHILE_STREAM_LARGE === "1" ? it : it.skip)(
  "streams 100,001 rows through a zero-row fallback budget",
  async () => {
    await db.pgPool.query(`
      insert into a.post(id, author_id, headline, body)
        select 100000 + j, 1, 'Export ' || j, null from generate_series(1, 100010) j
    `);
    const vars = { ...exportVariables, first: 100001 };
    const expected = await run(ordinary, vars);
    const { data, sql } = await run(document, vars, 0);
    expect(data.myOrganizations[0].members.nodes).toHaveLength(vars.first);
    expect(isDeepStrictEqual(data, expected.data)).toBe(true);
    expect(
      sql.filter((q) => q.startsWith("fetch forward")).length,
    ).toBeGreaterThan(1000);
  },
  60000,
);
