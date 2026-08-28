import { fetch } from "@whatwg-node/fetch";

import { makeExampleServer } from "./exampleServer.ts";

let server: Awaited<ReturnType<typeof makeExampleServer>> | null = null;

afterEach(() => {
  server?.release();
});

test("response body contains expected error object when function provided as grafast context option throws an error", async () => {
  const maskError = jest.fn((error) => error);
  server = await makeExampleServer({
    grafserv: {
      graphqlOverGET: true,
      graphqlPath: "/graphql",
      dangerouslyAllowAllCORSRequests: true,
      maskError,
    },
    grafast: {
      context: () => {
        throw new Error("a particular error");
      },
    },
  });
  const res = await fetch(server!.url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/graphql-response+json",
    },
    body: JSON.stringify({ query: "{ __typename }" }),
  });
  const responseBody = await res.json();
  expect(responseBody).toHaveProperty(
    "errors[0].message",
    "a particular error",
  );
  expect(maskError).toHaveBeenCalledTimes(1);
});

test("only returns requested explain scopes", async () => {
  server = await makeExampleServer({
    grafserv: {
      graphqlPath: "/graphql",
      dangerouslyAllowAllCORSRequests: true,
    },
    grafast: {
      explain: true,
    },
  });

  const request = (explain?: string) =>
    fetch(server!.url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/graphql-response+json",
        ...(explain ? { "x-graphql-explain": explain } : null),
      },
      body: JSON.stringify({ query: "{ hello }" }),
    }).then((res) => res.json());

  const withoutExplain = await request();
  expect(withoutExplain.extensions).toBeUndefined();

  const sqlOnly = await request("sql");
  expect(sqlOnly.extensions.explain.operations).toEqual([]);

  const planOnly = await request("plan");
  expect(planOnly.extensions.explain.operations).toEqual([
    expect.objectContaining({ type: "plan" }),
  ]);
});
