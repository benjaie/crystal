import type { PgCodec, PgRegistry } from "@dataplan/pg";

import { getPgMultiTenancyColumns } from "../src/multiTenancy.ts";
import { PgBasicsPlugin } from "../src/plugins/PgBasicsPlugin.ts";

function registryFixture(withTenantForeignKey: boolean, codec?: PgCodec) {
  const things =
    codec ??
    ({
      name: "things",
      extensions: { pg: { schemaName: "public", name: "things" } },
    } as PgCodec);
  const gyms = {
    name: "gyms",
    extensions: { pg: { schemaName: "public", name: "gyms" } },
  } as PgCodec;
  const registry = {
    pgExecutors: {},
    pgCodecs: { things, gyms },
    pgResources: {},
    pgRelations: withTenantForeignKey
      ? {
          things: {
            gym: {
              localCodec: things,
              remoteResource: { codec: gyms },
              localAttributes: ["gym_id"],
              remoteAttributes: ["id"],
              isReferencee: false,
            },
          },
        }
      : {},
  } as unknown as PgRegistry;
  return { registry, codec: things };
}

function makeBuild(registry: PgRegistry, implicit = true) {
  const hook = PgBasicsPlugin.schema?.hooks?.build;
  if (typeof hook !== "function") throw new Error("Missing build hook");
  return hook(
    {
      input: { pgRegistry: registry },
      options: {
        pgMultiTenancy: [
          { table: "gyms", columns: ["gym_id"], implicit: [implicit] },
        ],
      },
      graphql: {},
      lib: { dataplanPg: {}, dataplanJson: {} },
      versions: {},
      extend: Object.assign,
    } as never,
    {} as never,
    {} as never,
  ) as GraphileBuild.Build;
}

test.each([false, true])(
  "tenant classification stays within the build (reuse codec: %s)",
  (reuseCodec) => {
    const first = registryFixture(true);
    const oldBuild = makeBuild(first.registry);
    expect([...oldBuild.getPgMultiTenancyColumns(first.codec)]).toEqual([
      "gym_id",
    ]);
    expect([...oldBuild.getPgImplicitMultiTenancyColumns(first.codec)]).toEqual(
      ["gym_id"],
    );

    const second = registryFixture(false, reuseCodec ? first.codec : undefined);
    const newBuild = makeBuild(second.registry);
    expect([...newBuild.getPgMultiTenancyColumns(second.codec)]).toEqual([]);
    expect([
      ...newBuild.getPgImplicitMultiTenancyColumns(second.codec),
    ]).toEqual([]);
    expect([...oldBuild.getPgImplicitMultiTenancyColumns(first.codec)]).toEqual(
      ["gym_id"],
    );
    expect([
      ...getPgMultiTenancyColumns(
        second.codec,
        second.registry,
        oldBuild.options.pgMultiTenancy,
      ),
    ]).toEqual([]);
    expect([
      ...getPgMultiTenancyColumns(
        first.codec,
        first.registry,
        oldBuild.options.pgMultiTenancy,
      ),
    ]).toEqual(["gym_id"]);
  },
);

test("builds using the same registry can have different implicit flags", () => {
  const { registry, codec } = registryFixture(true);
  const implicitBuild = makeBuild(registry);
  const explicitBuild = makeBuild(registry, false);
  expect([...explicitBuild.getPgMultiTenancyColumns(codec)]).toEqual([
    "gym_id",
  ]);
  expect([...explicitBuild.getPgImplicitMultiTenancyColumns(codec)]).toEqual(
    [],
  );
  expect([...implicitBuild.getPgImplicitMultiTenancyColumns(codec)]).toEqual([
    "gym_id",
  ]);
});
