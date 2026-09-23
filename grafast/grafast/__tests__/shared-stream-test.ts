/* eslint-disable graphile-export/exhaustive-deps, graphile-export/export-methods, graphile-export/export-plans, graphile-export/export-instances, graphile-export/export-subclasses, graphile-export/no-nested */
import { expect } from "chai";
import { resolvePreset } from "graphile-config";
import { it } from "mocha";

import {
  connection,
  constant,
  grafast,
  lambda,
  last,
  loadMany,
  makeGrafastSchema,
} from "../dist/index.js";
import { resolveStreamDefer } from "./incrementalUtils.ts";

const expected = Array.from({ length: 12 }, (_, i) => i + 1);
async function run(source: string) {
  let closed = 0;
  let yielded = 0;
  const loader = (keys: readonly unknown[]) =>
    keys.map(() =>
      (async function* () {
        try {
          for (const value of expected) {
            yielded++;
            yield value;
          }
        } finally {
          closed++;
        }
      })(),
    );
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `
      type Query { groups: [NumberGroup!] numbers: [Int!] connection(first: Int): NumberConnection }
      type NumberGroup { numbers: [Int!] }
      type NumberConnection { nodes: [Int!] edges: [NumberEdge!] pageInfo: PageInfo! }
      type NumberEdge { node: Int! cursor: String! }
      type PageInfo { hasNextPage: Boolean! hasPreviousPage: Boolean! startCursor: String endCursor: String }
    `,
    objects: {
      NumberGroup: { plans: { numbers: ($key) => loadMany($key, loader) } },
      Query: {
        plans: {
          groups() {
            return constant([1, 1]);
          },
          numbers() {
            return loadMany(constant(1), loader);
          },
          connection(_, { $first }) {
            const $connection = connection(loadMany(constant(1), loader));
            $connection.setFirst($first);
            return $connection;
          },
        },
      },
      NumberConnection: {
        plans: {
          nodes: ($connection) => $connection.nodes(),
          edges: ($connection) => $connection.edges(),
          pageInfo: ($connection) => $connection.pageInfo(),
        },
      },
      NumberEdge: {
        plans: {
          node: ($edge) => $edge.node(),
          cursor: ($edge) => $edge.cursor(),
        },
      },
      PageInfo: {
        plans: {
          hasNextPage: ($info) => $info.get("hasNextPage"),
          hasPreviousPage: ($info) => $info.get("hasPreviousPage"),
          startCursor: ($info) => $info.get("startCursor"),
          endCursor: ($info) => $info.get("endCursor"),
        },
      },
    },
  });
  const result = await grafast({
    schema,
    source,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  const payloads: any[] = [];
  if (Symbol.asyncIterator in result) {
    for await (const payload of result) payloads.push(payload);
  } else {
    payloads.push(result);
  }
  expect(payloads.flatMap((p) => p.errors ?? [])).to.deep.equal([]);
  const merged = resolveStreamDefer(payloads);
  expect(merged.errors).not.to.exist;
  return { closed, yielded, payloads, data: merged.data as any };
}

it("materializes a shared connection for streamed edges and nodes", async () => {
  const { yielded, closed, data } = await run(
    `{ connection { edges @stream(initialCount: 1) { node } nodes @stream(initialCount: 3) } }`,
  );
  expect(data.connection.nodes).to.deep.equal(expected);
  expect(data.connection.edges.map((e: any) => e.node)).to.deep.equal(expected);
  expect(yielded).to.equal(12);
  expect(closed).to.equal(1);
});
it("materializes before resolving immediate pageInfo", async () => {
  const { closed, data, payloads } = await run(
    `{ connection(first: 5) { nodes @stream(initialCount: 1) pageInfo { hasNextPage hasPreviousPage startCursor endCursor } } }`,
  );
  expect(payloads[0].data.connection.pageInfo).to.deep.equal({
    hasNextPage: true,
    hasPreviousPage: false,
    startCursor: "MA==",
    endCursor: "NA==",
  });
  expect(data.connection.nodes).to.deep.equal(expected.slice(0, 5));
  expect(closed).to.equal(1);
});
it("preserves both independently streamed aliases", async () => {
  const { data } = await run(
    `{ a: numbers @stream(initialCount: 1) b: numbers @stream(initialCount: 2) }`,
  );
  expect(data.a).to.deep.equal(expected);
  expect(data.b).to.deep.equal(expected);
});
it("still delivers a single consumer incrementally", async () => {
  const { data, payloads } = await run(
    `{ connection { nodes @stream(initialCount: 1) } }`,
  );
  expect(payloads[0].data.connection.nodes).to.deep.equal([1]);
  expect(payloads.length).to.be.greaterThan(1);
  expect(data.connection.nodes).to.deep.equal(expected);
});

it("does not share a one-shot iterator between repeated loader keys", async () => {
  const { data } = await run(`{ groups { numbers @stream(initialCount: 1) } }`);
  expect(data.groups).to.deep.equal([
    { numbers: expected },
    { numbers: expected },
  ]);
});

it("materializes a shared one-shot source at the former Distributor boundary", async () => {
  let yielded = 0;
  let closed = 0;
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { shared: Shared } type Shared { numbers: [Int!] last: Int }`,
    objects: {
      Query: {
        plans: {
          shared() {
            const $source = lambda(constant(null), () =>
              (function* () {
                try {
                  for (const value of expected) {
                    yielded++;
                    yield value;
                  }
                } finally {
                  closed++;
                }
              })(),
            );
            $source.cloneStreams = true;
            return $source;
          },
        },
      },
      Shared: {
        plans: {
          numbers: ($source) => $source,
          last: ($source) => last($source, false),
        },
      },
    },
  });
  const result = await grafast({
    schema,
    source: `{ shared { numbers @stream(initialCount: 1) last } }`,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  const payloads: any[] = [];
  if (Symbol.asyncIterator in result) {
    for await (const payload of result) payloads.push(payload);
  } else payloads.push(result);
  expect(resolveStreamDefer(payloads).data).to.deep.equal({
    shared: { numbers: expected, last: 12 },
  });
  expect(yielded).to.equal(12);
  expect(closed).to.equal(1);
});

it("materializes a source used by a consumer repeated across list items", async () => {
  let source: ReturnType<typeof lambda>;
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { shared: Shared } type Shared { groups: [Group!] } type Group { numbers: [Int!] }`,
    objects: {
      Query: {
        plans: {
          shared() {
            source = lambda(constant(null), () =>
              (function* () {
                yield* expected;
              })(),
            );
            source.cloneStreams = true;
            return source;
          },
        },
      },
      Shared: { plans: { groups: () => constant([1, 2]) } },
      Group: { plans: { numbers: () => source } },
    },
  });
  const result = await grafast({
    schema,
    source: `{ shared { groups { numbers @stream(initialCount: 1) } } }`,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  const payloads: any[] = [];
  if (Symbol.asyncIterator in result) {
    for await (const payload of result) payloads.push(payload);
  } else payloads.push(result);
  const merged = resolveStreamDefer(payloads);
  expect(merged.errors).not.to.exist;
  expect(merged.data).to.deep.equal({
    shared: { groups: [{ numbers: expected }, { numbers: expected }] },
  });
});
