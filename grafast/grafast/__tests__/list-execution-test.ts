/* eslint-disable graphile-export/export-methods, graphile-export/export-plans, graphile-export/export-subclasses, graphile-export/no-nested */
import { expect } from "chai";
import { resolvePreset } from "graphile-config";
import { parse } from "graphql";
import { it } from "mocha";

import type { ExecutionDetails } from "../dist/index.js";
import {
  constant,
  execute,
  grafast,
  lambda,
  makeGrafastSchema,
  Step,
} from "../dist/index.js";
import { resolveStreamDefer } from "./incrementalUtils.ts";

async function collect(result: any) {
  const payloads: any[] = [];
  if (Symbol.asyncIterator in result) {
    for await (const payload of result) payloads.push(payload);
  } else payloads.push(result);
  return { payloads, merged: resolveStreamDefer(payloads) };
}

for (const kind of ["array", "iterable", "asyncIterable"] as const) {
  for (const initialCount of [0, 2, 10]) {
    it(`list layers honour initialCount ${initialCount} for ${kind}`, async () => {
      let childCalls = 0;
      let pullsAtFirstChild: number | undefined;
      let pulls = 0;
      let closed = 0;
      const rows = [1, 2, 3, 4];
      const source =
        kind === "array"
          ? rows
          : kind === "iterable"
            ? {
                *[Symbol.iterator]() {
                  try {
                    for (const row of rows) {
                      pulls++;
                      yield row;
                    }
                  } finally {
                    closed++;
                  }
                },
              }
            : {
                async *[Symbol.asyncIterator]() {
                  try {
                    for (const row of rows) {
                      pulls++;
                      yield row;
                    }
                  } finally {
                    closed++;
                  }
                },
              };
      const schema = makeGrafastSchema({
        enableDeferStream: true,
        typeDefs: `type Query { rows: [Row!] } type Row { value: Int! }`,
        objects: {
          Query: { plans: { rows: () => constant(source, false) } },
          Row: {
            plans: {
              value: ($row) =>
                lambda($row, (row) => {
                  pullsAtFirstChild ??= pulls;
                  childCalls++;
                  return row;
                }),
            },
          },
        },
      });
      const result = await grafast({
        schema,
        source: `{ rows @stream(initialCount: ${initialCount}) { value } }`,
        resolvedPreset: resolvePreset({}),
        requestContext: {},
      });
      expect(childCalls).to.equal(Math.min(4, initialCount));
      if (kind !== "array" && initialCount > 0) {
        expect(pullsAtFirstChild).to.equal(Math.min(4, initialCount));
      }
      const { payloads, merged } = await collect(result);
      expect(payloads[0].data.rows).to.have.length(Math.min(4, initialCount));
      expect(merged.errors).not.to.exist;
      expect(merged.data).to.deep.equal({
        rows: rows.map((value) => ({ value })),
      });
      expect(childCalls).to.equal(4);
      if (kind !== "array") expect(closed).to.equal(1);
    });
  }
}

for (const directive of ["", "@stream(if: false, initialCount: 1)"]) {
  it(`fully consumes an async iterable for immediate delivery ${directive}`, async () => {
    const schema = makeGrafastSchema({
      enableDeferStream: true,
      typeDefs: `type Query { numbers: [Int!] }`,
      objects: {
        Query: {
          plans: {
            numbers: () =>
              constant(
                {
                  async *[Symbol.asyncIterator]() {
                    yield 1;
                    yield 2;
                    yield 3;
                  },
                },
                false,
              ),
          },
        },
      },
    });
    const result = await grafast({
      schema,
      source: `{ numbers ${directive} }`,
      resolvedPreset: resolvePreset({}),
      requestContext: {},
    });
    expect(Symbol.asyncIterator in result).to.equal(false);
    expect(result).to.deep.include({ data: { numbers: [1, 2, 3] } });
  });
}

it("preserves the producer's iterable in execution values for other dependents", async () => {
  const source = {
    async *[Symbol.asyncIterator]() {
      yield 1;
      yield 2;
      yield 3;
    },
  };
  class SourceStep extends Step {
    isSyncAndSafe = true;
    execute({ indexMap }: ExecutionDetails) {
      return indexMap(() => source);
    }
  }
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { shared: Shared } type Shared { numbers: [Int!] sameSource: Boolean! }`,
    objects: {
      Query: { plans: { shared: () => new SourceStep() } },
      Shared: {
        plans: {
          numbers: ($source) => $source,
          sameSource: ($source) => lambda($source, (value) => value === source),
        },
      },
    },
  });
  const result = await grafast({
    schema,
    source: `{ shared { numbers @stream(initialCount: 1) sameSource } }`,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  const { merged } = await collect(result);
  expect(merged.errors).not.to.exist;
  expect(merged.data).to.deep.equal({
    shared: { numbers: [1, 2, 3], sameSource: true },
  });
});

it("closes a list iterator when initial traversal throws", async () => {
  let closed = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          return Promise.reject(new Error("fetch failed"));
        },
        return() {
          closed++;
          return Promise.resolve({ done: true as const, value: undefined });
        },
      };
    },
  };
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { numbers: [Int!] }`,
    objects: { Query: { plans: { numbers: () => constant(source, false) } } },
  });
  const { merged } = await collect(
    await grafast({
      schema,
      source: `{ numbers @stream(initialCount: 1) }`,
      resolvedPreset: resolvePreset({}),
      requestContext: {},
    }),
  );
  expect(merged.data).to.deep.equal({ numbers: null });
  expect(merged.errors?.[0].message).to.equal("fetch failed");
  expect(closed).to.equal(1);
});

it("consumes nested streams independently alongside deferred work", async () => {
  let opened = 0;
  let closed = 0;
  const source = {
    async *[Symbol.asyncIterator]() {
      opened++;
      try {
        yield 1;
        yield 2;
        yield 3;
      } finally {
        closed++;
      }
    },
  };
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { groups: [Group!] } type Group { numbers: [Int!] extra: Int! }`,
    objects: {
      Query: { plans: { groups: () => constant([1, 2]) } },
      Group: {
        plans: {
          numbers: () => constant(source, false),
          extra: ($group) => $group,
        },
      },
    },
  });
  const { payloads, merged } = await collect(
    await grafast({
      schema,
      source: `{ groups @stream(initialCount: 1) { numbers @stream(initialCount: 1) ... @defer { extra } } }`,
      resolvedPreset: resolvePreset({}),
      requestContext: {},
    }),
  );
  expect(payloads[0].data).to.deep.equal({ groups: [{ numbers: [1] }] });
  expect(merged.errors).not.to.exist;
  expect(merged.data).to.deep.equal({
    groups: [
      { numbers: [1, 2, 3], extra: 1 },
      { numbers: [1, 2, 3], extra: 2 },
    ],
  });
  expect(opened).to.equal(2);
  expect(closed).to.equal(2);
});

it("cancels a pending incremental next without waiting for the producer", async () => {
  let closed = 0;
  const started = Promise.withResolvers<void>();
  const source = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          started.resolve();
          return new Promise<IteratorResult<number>>(() => {});
        },
        return() {
          closed++;
          return Promise.resolve({ done: true as const, value: undefined });
        },
      };
    },
  };
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { numbers: [Int!] }`,
    objects: { Query: { plans: { numbers: () => constant(source, false) } } },
  });
  const result = await grafast({
    schema,
    source: `{ numbers @stream(initialCount: 0) }`,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  expect(Symbol.asyncIterator in result).to.equal(true);
  if (!(Symbol.asyncIterator in result))
    throw new Error("Expected incremental results");
  const iterator = result[Symbol.asyncIterator]();
  await iterator.next();
  await started.promise;
  await iterator.return?.();
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(closed).to.equal(1);
});

it("keeps string output synchronous while consuming an async iterable immediately", async () => {
  const source = {
    async *[Symbol.asyncIterator]() {
      yield 1;
      yield 2;
    },
  };
  const schema = makeGrafastSchema({
    typeDefs: `type Query { numbers: [Int!] }`,
    objects: { Query: { plans: { numbers: () => constant(source, false) } } },
  });
  const result = await execute({
    schema,
    document: parse(`{ numbers }`),
    outputDataAsString: true,
    resolvedPreset: resolvePreset({}),
    requestContext: {},
  });
  expect(result).to.deep.equal({ data: '{"numbers":[1,2]}' });
});

for (const initialCount of [0, 3]) {
  it(`reports a rejected item promise at its list index (initialCount ${initialCount})`, async () => {
    const source = {
      *[Symbol.iterator]() {
        yield 1;
        yield Promise.reject(new Error("bad item"));
        yield 3;
      },
    };
    const schema = makeGrafastSchema({
      enableDeferStream: true,
      typeDefs: `type Query { numbers: [Int] }`,
      objects: { Query: { plans: { numbers: () => constant(source, false) } } },
    });
    const { merged } = await collect(
      await grafast({
        schema,
        source: `{ numbers @stream(initialCount: ${initialCount}) }`,
        resolvedPreset: resolvePreset({}),
        requestContext: {},
      }),
    );
    expect(merged.data).to.deep.equal({ numbers: [1, null, 3] });
    expect(merged.errors?.[0].message).to.equal("bad item");
    expect(merged.errors?.[0].path).to.deep.equal(["numbers", 1]);
  });
}

it("closes a paused iterator when initial non-null item completion fails", async () => {
  let pulled = 0;
  let closed = 0;
  const source = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          pulled++;
          return Promise.resolve({ done: false as const, value: null });
        },
        return() {
          closed++;
          return Promise.resolve({ done: true as const, value: undefined });
        },
      };
    },
  };
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `type Query { numbers: [Int!] }`,
    objects: { Query: { plans: { numbers: () => constant(source, false) } } },
  });
  const { merged } = await collect(
    await grafast({
      schema,
      source: `{ numbers @stream(initialCount: 1) }`,
      resolvedPreset: resolvePreset({}),
      requestContext: {},
    }),
  );
  expect(merged.data).to.deep.equal({ numbers: null });
  expect(merged.errors).to.have.length(1);
  expect(pulled).to.equal(1);
  expect(closed).to.equal(1);
});

it("chooses the runtime list traversal for abstract type branches", async () => {
  const schema = makeGrafastSchema({
    enableDeferStream: true,
    typeDefs: `interface Node { id: Int! } type A implements Node { id: Int! numbers: [Int!] } type B implements Node { id: Int! numbers: [Int!] } type Query { nodes: [Node!] }`,
    objects: {
      Query: {
        plans: {
          nodes: () =>
            constant([
              { __typename: "A", id: 1 },
              { __typename: "B", id: 2 },
            ]),
        },
      },
      A: { plans: { numbers: () => constant([1, 2, 3]) } },
      B: { plans: { numbers: () => constant([4, 5, 6]) } },
    },
  });
  const { payloads, merged } = await collect(
    await grafast({
      schema,
      source: `{ nodes { id ... on A { numbers } ... on B { numbers @stream(initialCount: 1, label: "B") } } }`,
      resolvedPreset: resolvePreset({}),
      requestContext: {},
    }),
  );
  expect(payloads[0].data).to.deep.equal({
    nodes: [
      { id: 1, numbers: [1, 2, 3] },
      { id: 2, numbers: [4] },
    ],
  });
  expect(merged.errors).not.to.exist;
  expect(merged.data).to.deep.equal({
    nodes: [
      { id: 1, numbers: [1, 2, 3] },
      { id: 2, numbers: [4, 5, 6] },
    ],
  });
});
