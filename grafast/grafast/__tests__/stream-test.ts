/* eslint-disable graphile-export/exhaustive-deps, graphile-export/export-methods, graphile-export/export-plans, graphile-export/export-instances, graphile-export/export-subclasses, graphile-export/no-nested */
import { IncomingMessage } from "node:http";
import { Socket } from "node:net";

import { expect } from "chai";
import { resolvePreset } from "graphile-config";
import type { AsyncExecutionResult } from "graphql";
import { it } from "mocha";

import type {
  ExecutionDetails,
  ExecutionResults,
  PromiseOrDirect,
} from "../dist/index.js";
import {
  connection,
  constant,
  flagError,
  grafast,
  lambda,
  loadMany,
  loadOne,
  makeGrafastSchema,
  Step,
} from "../dist/index.js";

const resolvedPreset = resolvePreset({});
const requestContext = {};

class SyncListCallbackStep<TIn, TOut extends any[]> extends Step<TOut> {
  isSyncAndSafe = false;
  private callback: (val: TIn) => PromiseOrDirect<TOut>;
  constructor($dep: Step<TIn>, callback: (val: TIn) => PromiseOrDirect<TOut>) {
    super();
    this.callback = callback;
    this.addDependency($dep);
  }
  execute({
    indexMap,
    values: [values0],
    stream,
  }: ExecutionDetails<[TIn]>): ExecutionResults<TOut> {
    const { callback } = this;
    return indexMap((i) => {
      const entry = values0.at(i);
      if (!stream) {
        return callback(entry);
      } else {
        return (async function* () {
          const data = await callback(entry);
          yield* data;
        })();
      }
    });
  }
}

const makeSchema = (useStreamableStep = false) => {
  return makeGrafastSchema({
    typeDefs: /* GraphQL */ `
      type OtherThing {
        id: Int
      }
      type Thing {
        id: Int
        anotherList: [OtherThing!]
      }
      type Query {
        list: [Thing!]
      }
    `,
    objects: {
      Query: {
        plans: {
          list() {
            return constant([1, 2]);
          },
        },
      },
      Thing: {
        plans: {
          id($i: Step<number>) {
            return $i;
          },
          anotherList($i: Step<number>) {
            const cb = (i: number) => [i + 0, i + 1, i + 2];
            if (useStreamableStep) {
              return new SyncListCallbackStep($i, cb);
            } else {
              return lambda($i, cb);
            }
          },
        },
      },
      OtherThing: {
        plans: {
          id($i: Step<number>) {
            return $i;
          },
        },
      },
    },
    enableDeferStream: true,
  });
};

it("streams with streamable step", async () => {
  const source = /* GraphQL */ `
    {
      list {
        id
        anotherList @stream(initialCount: 2) {
          id
        }
      }
    }
  `;
  const schema = makeSchema(true);
  const stream = (await grafast({
    schema,
    source,
    resolvedPreset,
    requestContext,
  })) as AsyncGenerator<AsyncExecutionResult>;
  let payloads: AsyncExecutionResult[] = [];
  for await (const payload of stream) {
    payloads.push(payload);
  }
  expect(payloads).to.have.length(4);
  expect(payloads[0]).to.deep.equal({
    data: {
      list: [
        { id: 1, anotherList: [{ id: 1 }, { id: 2 }] },
        { id: 2, anotherList: [{ id: 2 }, { id: 3 }] },
      ],
    },
    hasNext: true,
  });
  expect(payloads[1]).to.deep.equal({
    path: ["list", 0, "anotherList", 2],
    data: { id: 3 },
    hasNext: true,
  });
  expect(payloads[2]).to.deep.equal({
    path: ["list", 1, "anotherList", 2],
    data: { id: 4 },
    hasNext: true,
  });
  expect(payloads[3]).to.deep.equal({
    hasNext: false,
  });
});

it("streams with non-streamable step", async () => {
  const source = /* GraphQL */ `
    {
      list {
        id
        anotherList @stream(initialCount: 2) {
          id
        }
      }
    }
  `;
  const schema = makeSchema(false);
  const stream = (await grafast({
    schema,
    source,
    resolvedPreset,
    requestContext,
  })) as AsyncGenerator<AsyncExecutionResult>;
  let payloads: AsyncExecutionResult[] = [];
  for await (const payload of stream) {
    payloads.push(payload);
  }
  expect(payloads).to.have.length(4);
  expect(payloads[0]).to.deep.equal({
    data: {
      list: [
        { id: 1, anotherList: [{ id: 1 }, { id: 2 }] },
        { id: 2, anotherList: [{ id: 2 }, { id: 3 }] },
      ],
    },
    hasNext: true,
  });
  expect(payloads[1]).to.deep.equal({
    path: ["list", 0, "anotherList", 2],
    data: { id: 3 },
    hasNext: true,
  });
  expect(payloads[2]).to.deep.equal({
    path: ["list", 1, "anotherList", 2],
    data: { id: 4 },
    hasNext: true,
  });
  expect(payloads[3]).to.deep.equal({
    hasNext: false,
  });
});

it("bounds source pulls while the client is paused and closes on return", async () => {
  let pulls = 0;
  const closed = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  async function* source() {
    try {
      for (let i = 0; i < 1000; i++) {
        pulls++;
        started.resolve();
        yield i;
      }
    } finally {
      closed.resolve();
    }
  }
  const schema = makeGrafastSchema({
    typeDefs: "type Query { rows: [Int!]! }",
    objects: { Query: { plans: { rows: () => lambda(constant(0), source) } } },
    enableDeferStream: true,
  });
  const stream = await grafast({
    schema,
    source: "{ rows @stream }",
    resolvedPreset,
    requestContext,
  });
  if (!(Symbol.asyncIterator in stream)) {
    throw new Error("Expected streamed rows");
  }
  const iterator = stream[Symbol.asyncIterator]();
  expect((await iterator.next()).value?.data).to.deep.equal({ rows: [] });
  await started.promise;
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(pulls).to.be.within(1, 101);
  await iterator.return?.();
  await closed.promise;
  expect(pulls).to.be.lessThan(1000);
});

it("replays an async loadMany result for repeated parent keys", async () => {
  const calls: number[][] = [];
  const load = (keys: readonly number[]) => {
    calls.push([...keys]);
    return keys.map((key) =>
      (async function* () {
        yield key;
        yield key + 10;
      })(),
    );
  };
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { parents: [Parent!]! } type Parent { rows: [Int!]! }",
    enableDeferStream: true,
    objects: {
      Query: { plans: { parents: () => constant([1, 1, 2]) } },
      Parent: { plans: { rows: ($key: Step<number>) => loadMany($key, load) } },
    },
  });
  const stream = await grafast({
    schema,
    source: "{ parents { rows @stream } }",
  });
  if (!(Symbol.asyncIterator in stream)) {
    throw new Error("Expected streamed rows");
  }
  const rows: number[][] = [[], [], []];
  for await (const payload of stream) {
    if ("path" in payload && payload.path != null) {
      const [, parent, , item] = payload.path;
      rows[Number(parent)][Number(item)] = Number(payload.data);
    }
  }
  expect(calls).to.deep.equal([[1, 2]]);
  expect(rows).to.deep.equal([
    [1, 11],
    [1, 11],
    [2, 12],
  ]);
});

it("preserves null entries and errors in the streamed tail", async () => {
  const schema = makeGrafastSchema({
    typeDefs: "type Query { rows: [Int]! }",
    enableDeferStream: true,
    objects: {
      Query: {
        plans: {
          rows: () =>
            lambda(constant(0), () => [
              1,
              null,
              flagError(new Error("row failed")),
              4,
            ]),
        },
      },
    },
  });
  const stream = await grafast({
    schema,
    source: "{ rows @stream(initialCount: 1) }",
  });
  if (!(Symbol.asyncIterator in stream)) {
    throw new Error("Expected streamed rows");
  }
  const data: unknown[] = [];
  const errors: unknown[] = [];
  for await (const payload of stream) {
    if ("path" in payload && payload.path != null) {
      data[Number(payload.path[1])] = payload.data;
    } else if (payload.data != null) {
      data.push(...(payload.data as { rows: unknown[] }).rows);
    }
    for (const error of payload.errors ?? []) {
      errors.push([error.path, error.message]);
    }
  }
  expect(data).to.deep.equal([1, null, null, 4]);
  expect(errors).to.deep.equal([[["rows", 2], "row failed"]]);
});

it("reads metadata from an HTTP request without consuming its body", async () => {
  const message = new IncomingMessage(new Socket());
  message.method = "POST";
  message.url = "/upload";
  message.push("body");
  message.push(null);
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { request: Request } type Request { method: String, url: String }",
    objects: {
      Query: { plans: { request: () => lambda(constant(0), () => message) } },
    },
  });
  try {
    const result = await grafast({
      schema,
      source: "{ request { method url } }",
    });
    expect(result).to.deep.equal({
      data: { request: { method: "POST", url: "/upload" } },
    });
    expect(message.readableDidRead).to.equal(false);
  } finally {
    message.destroy();
  }
});

it("only pulls the skipped rows, page and requested lookahead from a connection", async () => {
  let pulls = 0;
  let closed = 0;
  async function* rows() {
    try {
      for (;;) {
        if (++pulls > 4) throw new Error("Read beyond the requested page");
        yield pulls;
      }
    } finally {
      closed++;
    }
  }
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { page(first: Int, offset: Int): Page } type Page { nodes: [Int!]!, pageInfo: PageInfo! } type PageInfo { hasNextPage: Boolean! }",
    enableDeferStream: true,
    objects: {
      Query: {
        plans: {
          page: (_, fieldArgs) =>
            connection(lambda(constant(0), rows) as unknown as Step<number[]>, {
              fieldArgs,
            }),
        },
      },
    },
  });
  for (const query of [
    "{ page(first: 1, offset: 2) { nodes } }",
    "{ page(first: 1, offset: 2) { nodes @stream ... @defer { pageInfo { hasNextPage } } } }",
  ]) {
    pulls = closed = 0;
    const result = await grafast({
      schema,
      source: query,
      resolvedPreset: resolvePreset({
        grafast: { maxMaterializedStreamRows: 1 },
      }),
    });
    if (Symbol.asyncIterator in result) {
      const patches = [];
      for await (const patch of result) patches.push(patch);
      expect(patches.map((patch) => patch.data)).to.deep.include(3);
      expect(patches.map((patch) => patch.data)).to.deep.include({
        pageInfo: { hasNextPage: true },
      });
      expect(pulls).to.equal(4);
    } else {
      expect(result).to.deep.equal({ data: { page: { nodes: [3] } } });
      expect(pulls).to.equal(3);
    }
    expect(closed).to.equal(1);
  }
});

it("retains the final page of a forward-only source in input order", async () => {
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { page(last: Int): Page } type Page { nodes: [Int!]!, pageInfo: PageInfo! } type PageInfo { hasPreviousPage: Boolean! }",
    enableDeferStream: true,
    objects: {
      Query: {
        plans: {
          page: (_, fieldArgs) => {
            const $rows = lambda(null, async function* () {
              yield* [1, 2, 3, 4, 5];
            });
            return connection($rows as unknown as Step<number[]>, {
              fieldArgs,
            });
          },
        },
      },
    },
  });
  const result = await grafast({
    schema,
    source:
      "{ page(last: 2) { nodes @stream(initialCount: 3) pageInfo { hasPreviousPage } } }",
    resolvedPreset: resolvePreset({
      grafast: { maxMaterializedStreamRows: 2 },
    }),
  });
  expect(result).to.deep.equal({
    data: { page: { nodes: [4, 5], pageInfo: { hasPreviousPage: true } } },
  });
});

it("applies a zero fallback budget only to enabled streams", async () => {
  async function* rows() {
    yield 1;
    yield 2;
  }
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { rows: [Int!]!, parents: [Parent!]! } type Parent { rows: [Int!]! }",
    enableDeferStream: true,
    objects: {
      Query: {
        plans: {
          rows: () => lambda(constant(0), rows),
          parents: () => constant([1, 1]),
        },
      },
      Parent: {
        plans: {
          rows: ($key: Step<number>) =>
            loadMany($key, (keys) => keys.map(rows)),
        },
      },
    },
  });
  const resolvedPreset = resolvePreset({
    grafast: { maxMaterializedStreamRows: 0 },
  });
  for (const directive of ["", "@stream(if: false)"]) {
    const result = await grafast({
      schema,
      resolvedPreset,
      source: `{ rows ${directive} parents { rows ${directive} } }`,
    });
    expect(result).to.deep.equal({
      data: { rows: [1, 2], parents: [{ rows: [1, 2] }, { rows: [1, 2] }] },
    });
  }
  const source =
    "query($stream: Boolean! = true) { parents { rows @stream(if: $stream) } }";
  const disabled = await grafast({
    schema,
    resolvedPreset,
    source,
    variableValues: { stream: false },
  });
  expect(disabled).to.deep.equal({
    data: { parents: [{ rows: [1, 2] }, { rows: [1, 2] }] },
  });
  const enabled = await grafast({ schema, resolvedPreset, source });
  if (Symbol.asyncIterator in enabled) {
    throw new Error("Expected the shared list to exceed its fallback budget");
  }
  expect(enabled.errors?.[0].message).to.equal(
    "Materialized stream row limit exceeded",
  );
});

for (const initialCount of [0, 1]) {
  it(`settles all array items before returning a stream with initialCount ${initialCount}`, async () => {
    const started = Promise.withResolvers<void>();
    const pending = Promise.withResolvers<number>();
    const failed = Promise.withResolvers<number>();
    const rows = Object.freeze([
      Promise.resolve(1),
      failed.promise,
      pending.promise,
    ]);
    const schema = makeGrafastSchema({
      typeDefs: "type Query { rows: [Int]! }",
      enableDeferStream: true,
      objects: {
        Query: {
          plans: {
            rows: () =>
              lambda(constant(0), async () => {
                started.resolve();
                return rows;
              }),
          },
        },
      },
    });
    let completed = false;
    const execution = Promise.resolve(
      grafast({
        schema,
        source: `{ rows @stream(initialCount: ${initialCount}) }`,
      }),
    ).then((result) => {
      completed = true;
      return result;
    });
    await started.promise;
    failed.reject(new Error("bad row"));
    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(completed).to.equal(false);
    } finally {
      pending.resolve(3);
    }

    const result = await execution;
    if (!(Symbol.asyncIterator in result)) {
      throw new Error("Expected streamed rows");
    }
    const payloads = [];
    for await (const payload of result) {
      payloads.push(payload);
    }
    expect(payloads.shift()).to.deep.equal({
      data: { rows: initialCount === 0 ? [] : [1] },
      hasNext: true,
    });
    if (initialCount === 0) {
      expect(payloads.shift()).to.deep.equal({
        path: ["rows", 0],
        data: 1,
        hasNext: true,
      });
    }
    const failure = payloads.shift();
    expect(failure?.data).to.equal(null);
    expect(
      failure?.errors?.map((error) => [error.message, error.path]),
    ).to.deep.equal([["bad row", ["rows", 1]]]);
    expect(payloads).to.deep.equal([
      { path: ["rows", 2], data: 3, hasNext: true },
      { hasNext: false },
    ]);
  });
}

async function streamRowsWithDeferredDetail(
  rows: () => AsyncGenerator<number>,
  loadDetail: (ids: readonly number[]) => Promise<readonly number[]>,
) {
  const schema = makeGrafastSchema({
    typeDefs:
      "type Query { rows: [Row!]! } type Row { id: Int!, detail: Int! }",
    enableDeferStream: true,
    objects: {
      Query: { plans: { rows: () => lambda(constant(0), rows) } },
      Row: {
        plans: {
          id: ($id: Step<number>) => $id,
          detail: ($id: Step<number>) => loadOne($id, loadDetail),
        },
      },
    },
  });
  const result = await grafast({
    schema,
    source: "{ rows @stream { id ... @defer { detail } } }",
  });
  if (!(Symbol.asyncIterator in result)) {
    throw new Error("Expected streamed rows");
  }
  return result[Symbol.asyncIterator]();
}

it("advances a parent stream while batching and bounding its nested defers", async () => {
  const first = Promise.withResolvers<void>();
  const second = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const sizes: number[] = [];
  let pulled = 0;
  async function* rows() {
    while (pulled < 201) {
      yield pulled++;
    }
  }
  const iterator = await streamRowsWithDeferredDetail(rows, async (ids) => {
    sizes.push(ids.length);
    if (ids[0] === 0) {
      await first.promise;
    } else if (ids[0] === 100) {
      secondStarted.resolve();
      await second.promise;
    }
    return ids;
  });
  try {
    expect((await iterator.next()).value?.data).to.deep.equal({ rows: [] });
    for (let id = 0; id < 200; id++) {
      expect((await iterator.next()).value).to.deep.equal({
        path: ["rows", id],
        data: { id },
        hasNext: true,
      });
    }
    await secondStarted.promise;
    expect(pulled).to.equal(200);
    expect(sizes).to.deep.equal([100, 100]);

    second.resolve();
    const details: number[] = [];
    let lastParentDelivered = false;
    while (details.length < 101) {
      const { value } = await iterator.next();
      if (value == null || !("path" in value)) {
        throw new Error(
          "Expected a row or a deferred detail before completion",
        );
      }
      const id = value.path?.[1] as number;
      if (id === 200 && !lastParentDelivered) {
        expect(value).to.deep.equal({
          path: ["rows", id],
          data: { id },
          hasNext: true,
        });
        lastParentDelivered = true;
      } else {
        expect(value).to.deep.equal({
          path: ["rows", id],
          data: { detail: id },
          hasNext: true,
        });
        details.push(id);
      }
    }
    expect(details).to.have.members(
      Array.from({ length: 101 }, (_, i) => i + 100),
    );
    expect(sizes).to.deep.equal([100, 100, 1]);

    first.resolve();
    for (let id = 0; id < 100; id++) {
      expect((await iterator.next()).value).to.deep.equal({
        path: ["rows", id],
        data: { detail: id },
        hasNext: true,
      });
    }
    expect((await iterator.next()).value).to.deep.equal({ hasNext: false });
    expect((await iterator.next()).done).to.equal(true);
  } finally {
    first.resolve();
    second.resolve();
    await iterator.return?.();
  }
});

it("closes the parent source when cancelled with two deferred batches pending", async () => {
  const release = Promise.withResolvers<void>();
  const closed = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  let pulled = 0;
  let batches = 0;
  async function* rows() {
    try {
      while (true) {
        yield pulled++;
      }
    } finally {
      closed.resolve();
    }
  }
  const iterator = await streamRowsWithDeferredDetail(rows, async (ids) => {
    if (++batches === 2) {
      started.resolve();
    }
    await release.promise;
    return ids;
  });
  try {
    await iterator.next(); // Initial payload
    for (let i = 0; i < 200; i++) {
      await iterator.next();
    }
    await started.promise;
    await iterator.return?.();
    await closed.promise;
    expect(pulled).to.equal(200);
  } finally {
    release.resolve();
    await iterator.return?.();
  }
});
