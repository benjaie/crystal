import type { ExecutionPatchResult } from "grafast/graphql";

import { runTestQuery } from "./helpers.ts";

const fields = `
  nodes @stream(if: $nodes, initialCount: 1) { id }
  edges @stream(if: $edges, initialCount: 2) { cursor node { body } }
  ... @defer(if: $defer) {
    pageInfo { startCursor endCursor hasNextPage hasPreviousPage }
  }`;

const variables = "($nodes: Boolean!, $edges: Boolean!, $defer: Boolean!)";

function assemble(result: Awaited<ReturnType<typeof runTestQuery>>) {
  const data = structuredClone(result.payloads?.[0].data ?? result.data);
  for (const payload of result.payloads?.slice(1) ?? []) {
    const path = (payload as ExecutionPatchResult).path!;
    let target = data!;
    for (const key of path.slice(0, -1)) {
      target = target[key];
    }
    const key = path[path.length - 1];
    if (target[key] == null) {
      target[key] = payload.data;
    } else {
      Object.assign(target[key], payload.data);
    }
  }
  return data;
}

it.each([0, 1, 3, 1000])(
  "preserves the connection with first: %i",
  async (first) => {
    const connection = `allMessagesConnection(first: ${first})`;
    const ordinary = await runTestQuery(
      `{ ${connection} { ${fields.replace(/@(stream|defer)\([^)]*\)/g, "")} } }`,
      {},
    );
    expect(ordinary.errors).toBeUndefined();
    for (let mask = 0; mask < 8; mask++) {
      const result = await runTestQuery(
        `query ${variables} { ${connection} { ${fields} } }`,
        {
          variableValues: {
            nodes: Boolean(mask & 1),
            edges: Boolean(mask & 2),
            defer: Boolean(mask & 4),
          },
        },
      );
      expect(result.errors).toBeUndefined();
      if (first >= 3 && mask === 7) {
        expect(
          result.payloads![0].data!.allMessagesConnection.nodes,
        ).toHaveLength(2);
        expect(
          result.queries.some((q) => q.text.startsWith("select 1 from (")),
        ).toBe(false);
      }
      expect(assemble(result)).toEqual(ordinary.data);
      if (first > 0 && mask === 7) {
        expect(result.queries.some((q) => q.text.startsWith("declare "))).toBe(
          true,
        );
      }
    }
  },
);

it("probes an upfront page flag before opening its cursor", async () => {
  const fields = "nodes @stream { id } pageInfo { hasNextPage }";
  const query = `{ allMessagesConnection(first: 2) { ${fields} } }`;
  const ordinary = await runTestQuery(query.replace("@stream", ""), {});
  const streamed = await runTestQuery(query, { maxMaterializedStreamRows: 0 });
  expect(streamed.errors).toBeUndefined();
  expect(assemble(streamed)).toEqual(ordinary.data);
  expect(
    streamed.queries.findIndex((q) => q.text.startsWith("select 1 from (")),
  ).toBe(0);
  expect(streamed.queries.some((q) => q.text.startsWith("declare "))).toBe(
    true,
  );
});

it("enforces the fallback row budget for upfront endCursor", async () => {
  const result = await runTestQuery(
    `{ allMessagesConnection(first: 3) { nodes @stream { id } pageInfo { endCursor } } }`,
    {
      maxMaterializedStreamRows: 2,
      checkErrorSnapshots: true,
    },
  );
  expect(result.errors?.[0].message).toContain(
    "Materialized stream row limit exceeded",
  );
});

it("uses the streamed lookahead when only hasPreviousPage is needed upfront", async () => {
  const query = `{ allMessagesConnection(first: 2) {
    nodes @stream { id }
    pageInfo { hasPreviousPage ... @defer { hasNextPage } }
  } }`;
  const ordinary = await runTestQuery(query.replace(/@stream|@defer/g, ""), {});
  const result = await runTestQuery(query, { maxMaterializedStreamRows: 0 });
  expect(result.errors).toBeUndefined();
  expect(assemble(result)).toEqual(ordinary.data);
  expect(result.queries.some((q) => q.text.startsWith("declare "))).toBe(true);
  expect(result.queries.some((q) => q.text.startsWith("select 1 from ("))).toBe(
    false,
  );
});

it("preserves nested, conditional delivery groups for each parent", async () => {
  for (let mask = 0; mask < 32; mask++) {
    const defer = (shift: number) =>
      `@defer(if: ${Boolean(mask & (1 << shift))})`;
    const stream = (shift: number) =>
      `@stream(if: ${Boolean(mask & (1 << shift))}, initialCount: ${shift % 3})`;
    const query = `{ forums(first: 3) {
      name members: messagesConnection(first: 2) {
        ... ${defer(0)} {
          ... ${defer(1)} { nodes ${stream(3)} { body } }
          ... ${defer(2)} { links: edges ${stream(4)} { cursor node { id } } }
          ... ${defer(4)} { pageInfo {
            ... ${defer(1)} { hasNextPage startCursor }
            endCursor @skip(if: ${Boolean(mask & 8)})
          } }
        }
        totalCount
      }
    } }`;
    const plain = await runTestQuery(
      query.replace(/@(stream|defer)\([^)]*\)/g, ""),
      {},
    );
    const incremental = await runTestQuery(query, {});
    expect(plain.errors).toBeUndefined();
    expect(incremental.errors).toBeUndefined();
    expect(assemble(incremental)).toEqual(plain.data);
  }
});

it("executes nested selects while the parent cursor holds the test client", async () => {
  const query = `{
    allMessagesConnection(first: 5) {
      nodes @stream(initialCount: 1) { id }
      edges @stream(initialCount: 2) { cursor node { author { username } } }
      ... @defer { pageInfo { endCursor hasNextPage } }
    }
  }`;
  const options = { path: __filename, deoptimize: true };
  const plain = await runTestQuery(
    query.replace(/@stream\([^)]*\)|@defer/g, ""),
    {},
    options,
  );
  const result = await runTestQuery(query, {}, options);
  expect(result.errors).toBeUndefined();
  expect(assemble(result)).toEqual(plain.data);
  expect(result.queries.some((q) => q.text.startsWith("declare "))).toBe(true);
});

it("streams one list with an upfront start cursor and deferred end cursor", async () => {
  const query = `{ allMessagesConnection(first: 3) {
    pageInfo { startCursor ... @defer { endCursor hasNextPage } }
    nodes @stream { id }
  } }`;
  const plain = await runTestQuery(query.replace(/@defer|@stream/g, ""), {});
  const result = await runTestQuery(query, { maxMaterializedStreamRows: 0 });
  expect(result.errors).toBeUndefined();
  expect(assemble(result)).toEqual(plain.data);
  expect(result.payloads![0].data!.allMessagesConnection.nodes).toHaveLength(1);
  expect(result.queries.some((q) => q.text.startsWith("declare "))).toBe(true);
});

it("can skip either row projection and either page boundary in a cached plan", async () => {
  const query = `query ($skipNodes: Boolean!, $skipEdges: Boolean!, $skipEnd: Boolean!,
    $deferRows: Boolean!, $deferPage: Boolean!, $stream: Boolean!, $prefix: Int!) {
    allMessagesConnection(first: 3) {
      ... @defer(if: $deferPage) {
        pageInfo {
          ... @skip(if: $skipEnd) @defer(if: $deferRows) { endCursor }
          ... @skip(if: $skipNodes) { startCursor }
          hasNextPage
        }
      }
      ... @defer(if: $deferRows) {
        nodes @skip(if: $skipNodes) @stream(if: $stream, initialCount: $prefix) { id }
        edges @skip(if: $skipEdges) @stream(if: $stream) { cursor }
      }
    }
  }`;
  const plain = query
    .replace(/@(stream|defer)\([^)]*\)/g, "")
    .replace(/\$(deferRows|deferPage|stream): Boolean!,/g, "")
    .replace(/,\s*\$prefix: Int!/, "");
  for (let mask = 0; mask < 64; mask++) {
    const options = {
      variableValues: {
        skipNodes: Boolean(mask & 1),
        skipEdges: Boolean(mask & 2),
        skipEnd: Boolean(mask & 4),
        deferRows: Boolean(mask & 8),
        deferPage: Boolean(mask & 16),
        stream: Boolean(mask & 32),
        prefix: mask % 5,
      },
    };
    const ordinary = await runTestQuery(plain, options);
    const incremental = await runTestQuery(query, options);
    expect(ordinary.errors).toBeUndefined();
    expect(incremental.errors).toBeUndefined();
    expect(assemble(incremental)).toEqual(ordinary.data);
  }
});
