import type { Step } from "grafast";
import {
  constant,
  context,
  each,
  execute,
  grafast,
  makeGrafastSchema,
  object,
} from "grafast";
import { ExecutionResult, parse } from "grafast/graphql";
import { resolvePreset } from "graphile-config";
import { Pool } from "pg";
import sql from "pg-sql2";

import { makePgAdaptorWithPgClient } from "../src/adaptors/pg.ts";
import { EXPORTABLE } from "../src/datasource.ts";
import { makeExampleSchema } from "../src/examples/exampleSchema.ts";
import { PgExecutor } from "../src/executor.ts";
import type {
  PgClientQuery,
  PgExecutorContext,
  WithPgClient,
} from "../src/index.ts";
import {
  makePgResourceOptions,
  makeRegistry,
  makeRegistryBuilder,
  pgSelect,
  TYPES,
} from "../src/index.ts";
import {
  createTestDatabase,
  dropTestDatabase,
  withTestWithPgClient,
} from "./sharedHelpers.ts";

test("concurrent identical reads with different pgSettings stay isolated", async () => {
  const { connectionString, databaseName } = await createTestDatabase();
  const pool = new Pool({ connectionString });
  try {
    const baseWithPgClient = makePgAdaptorWithPgClient(pool);
    let leases = 0;
    const withPgClient: WithPgClient = (pgSettings, callback) => {
      leases++;
      return baseWithPgClient(pgSettings, callback);
    };
    const executor = EXPORTABLE(
      (PgExecutor, context, object) =>
        new PgExecutor({
          name: "settingsIsolationTest",
          context: () => {
            const $context = context();
            return object({
              pgSettings: $context.get("pgSettings"),
              withPgClient: $context.get("withPgClient"),
            }) as Step<PgExecutorContext>;
          },
        }),
      [PgExecutor, context, object],
    );
    const resourceOptions = makePgResourceOptions({
      executor,
      codec: TYPES.text,
      from: sql`(select current_setting('jwt.claims.user_id')::text)`,
      name: "current_user_id",
    });
    const registry = EXPORTABLE(
      (executor, makeRegistry, makeRegistryBuilder, resourceOptions) =>
        makeRegistry(
          makeRegistryBuilder()
            .addExecutor(executor)
            .addResource(resourceOptions)
            .getRegistryConfig(),
        ),
      [executor, makeRegistry, makeRegistryBuilder, resourceOptions],
    );
    const schema = makeGrafastSchema({
      typeDefs: "type Query { currentUserId: String }",
      objects: {
        Query: {
          plans: {
            currentUserId: EXPORTABLE(
              (pgSelect, registry) =>
                function currentUserId() {
                  return pgSelect({
                    resource: registry.pgResources.current_user_id,
                    identifiers: [],
                  }).single();
                },
              [pgSelect, registry],
            ),
          },
        },
      },
    });
    const resolvedPreset = resolvePreset({
      grafast: {
        context(requestContext) {
          return {
            pgSettings: {
              "jwt.claims.user_id": (requestContext as { userId: string })
                .userId,
            },
            withPgClient,
          };
        },
      },
    });
    const source = "{ currentUserId }";
    const variableValues = {};
    const run = (userId: string) =>
      grafast({
        schema,
        source,
        variableValues,
        resolvedPreset,
        requestContext: { userId },
      });

    const [first, second] = await Promise.all([run("101"), run("202")]);

    expect(first).toMatchObject({ data: { currentUserId: "101" } });
    expect(second).toMatchObject({ data: { currentUserId: "202" } });
    expect(leases).toBe(2);
  } finally {
    await pool.end();
    await dropTestDatabase(databaseName);
  }
});

test("repeats batched selects without query values using generate_series", async () => {
  const { connectionString, databaseName } = await createTestDatabase();
  const pool = new Pool({ connectionString });
  try {
    const queries: PgClientQuery[] = [];
    await withTestWithPgClient(pool, queries, true, async (withPgClient) => {
      const executor = EXPORTABLE(
        (PgExecutor, context, object) =>
          new PgExecutor({
            name: "generateSeriesTest",
            context: () => {
              const $context = context();
              return object({
                pgSettings: $context.get("pgSettings"),
                withPgClient: $context.get("withPgClient"),
              }) as Step<PgExecutorContext>;
            },
          }),
        [PgExecutor, context, object],
      );
      const resource = makePgResourceOptions({
        executor,
        codec: TYPES.text,
        from: sql`(select current_setting('jwt.claims.user_id')::text)`,
        name: "current_user_id",
      });
      const registry = EXPORTABLE(
        (executor, makeRegistry, makeRegistryBuilder, resource) =>
          makeRegistry(
            makeRegistryBuilder()
              .addExecutor(executor)
              .addResource(resource)
              .getRegistryConfig(),
          ),
        [executor, makeRegistry, makeRegistryBuilder, resource],
      );
      const schema = makeGrafastSchema({
        typeDefs: "type Query { currentUserIds: [String] }",
        objects: {
          Query: {
            plans: {
              currentUserIds: EXPORTABLE(
                (constant, each, pgSelect, registry) =>
                  function currentUserIds() {
                    return each(constant([0, 1]), ($item) => {
                      const $select = pgSelect({
                        resource: registry.pgResources.current_user_id,
                        identifiers: [],
                        forceIdentity: true,
                      });
                      // Keep the select in this batched layer without adding a query value.
                      ($select as any).addDependency($item);
                      return $select.single();
                    });
                  },
                [constant, each, pgSelect, registry],
              ),
            },
          },
        },
      });

      const result = (await execute({
        schema,
        document: parse("{ currentUserIds }"),
        contextValue: {
          pgSettings: { "jwt.claims.user_id": "101" },
          withPgClient,
        },
      })) as ExecutionResult<any, any>;

      expect(result).toMatchObject({
        data: { currentUserIds: ["101", "101"] },
      });
      expect(queries).toContainEqual(
        expect.objectContaining({
          text: expect.stringContaining("generate_series"),
        }),
      );
    });
  } finally {
    await pool.end();
    await dropTestDatabase(databaseName);
  }
});

test("a connection page and totalCount share one client lease", async () => {
  const { connectionString, databaseName } = await createTestDatabase();
  const pool = new Pool({ connectionString });
  try {
    const queries: PgClientQuery[] = [];
    await withTestWithPgClient(pool, queries, true, async (withPgClient) => {
      let leases = 0;
      const countedWithPgClient: WithPgClient = (pgSettings, callback) => {
        leases++;
        return withPgClient(pgSettings, callback);
      };
      const result = (await execute({
        schema: makeExampleSchema(),
        document: parse(`
          {
            allMessagesConnection(first: 3) {
              nodes { body }
              totalCount
            }
          }
        `),
        contextValue: {
          pgSettings: { "app.test_setting": "queue" },
          withPgClient: countedWithPgClient,
        },
      })) as ExecutionResult<any, any>;

      expect(result.errors).toBeUndefined();
      expect(result.data?.allMessagesConnection?.totalCount).toBe(6);
      expect(result.data?.allMessagesConnection?.nodes).toHaveLength(3);
      expect(leases).toBe(1);
      expect(queries.filter((q) => q.name)).toHaveLength(2);
      expect(queries.filter((q) => q.text === "begin")).toHaveLength(1);
      expect(queries.filter((q) => q.text === "commit")).toHaveLength(1);
    });
  } finally {
    await pool.end();
    await dropTestDatabase(databaseName);
  }
});

test("root fields with the same SQL share a prepared statement client", async () => {
  const { connectionString, databaseName } = await createTestDatabase();
  const pool = new Pool({ connectionString });
  try {
    const queries: PgClientQuery[] = [];
    await withTestWithPgClient(pool, queries, true, async (withPgClient) => {
      let leases = 0;
      const countedWithPgClient: WithPgClient = (pgSettings, callback) => {
        leases++;
        return withPgClient(pgSettings, callback);
      };
      const result = (await execute({
        schema: makeExampleSchema(),
        document: parse(`
          {
            featured: allMessagesConnection(
              first: 1,
              condition: { featured: true }
            ) { nodes { body } }
            ordinary: allMessagesConnection(
              first: 1,
              condition: { featured: false }
            ) { nodes { body } }
          }
        `),
        contextValue: {
          pgSettings: { "app.test_setting": "queue" },
          withPgClient: countedWithPgClient,
        },
      })) as ExecutionResult<any, any>;

      expect(result.errors).toBeUndefined();
      expect(result.data?.featured?.nodes).toHaveLength(1);
      expect(result.data?.ordinary?.nodes).toHaveLength(1);
      const statements = queries.filter((q) => q.name);
      expect(statements).toHaveLength(2);
      expect(statements[0].name).toBe(statements[1].name);
      expect(leases).toBe(1);
    });
  } finally {
    await pool.end();
    await dropTestDatabase(databaseName);
  }
});

test("a failed read rolls back before queued reads get a new lease", async () => {
  const { connectionString, databaseName } = await createTestDatabase();
  const pool = new Pool({ connectionString });
  try {
    await withTestWithPgClient(pool, [], true, async (withPgClient) => {
      let leases = 0;
      const countedWithPgClient: WithPgClient = (pgSettings, callback) => {
        leases++;
        return withPgClient(pgSettings, callback);
      };
      const context = {
        pgSettings: { "app.test_setting": "queue" },
        withPgClient: countedWithPgClient,
      };
      const executor = EXPORTABLE(
        (PgExecutor) =>
          new PgExecutor({
            name: "queueErrorTest",
            context: () => null as any,
          }),
        [PgExecutor],
      );
      const executionAffinity = Symbol("same select");
      const run = (text: string) =>
        executor.executeWithCache([{ context, queryValues: [] }], {
          text,
          rawSqlValues: [],
          name: text,
          affinity: executionAffinity,
          eventEmitter: undefined,
        });

      const results = await Promise.allSettled([
        run("select 1"),
        run("select 1 / 0"),
        run("select 2"),
      ]);

      expect(results.map((r) => r.status)).toEqual([
        "fulfilled",
        "rejected",
        "fulfilled",
      ]);
      expect(leases).toBe(2);
    });
  } finally {
    await pool.end();
    await dropTestDatabase(databaseName);
  }
});
