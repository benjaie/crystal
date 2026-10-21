#!/usr/bin/env node
const pg = require("pg");
const fsp = require("fs").promises;
const child_process = require("child_process");
const { PgSimplifyInflectionPreset } = require("./dist/index.js");
const { TenantPreset } = require("../graphile-tenants/dist/index.js");
const { makeSchema } = require("postgraphile");
const { grafast } = require("grafast");
const { makePgService } = require("@dataplan/pg/adaptors/pg");
const { PostGraphileAmberPreset } = require("postgraphile/presets/amber");
const { makeV4Preset } = require("postgraphile/presets/v4");
const { printSchema, lexicographicSortSchema } = require("graphql");
const { parse: parseConnectionString } = require("pg-connection-string");
const assert = require("assert");

/**
 * We go beyond what Jest snapshots allow; so we have to manage it ourselves.
 * If UPDATE_SNAPSHOTS is set then we'll write updated snapshots, otherwise
 * we'll do the default behaviour of comparing to existing snapshots.
 */
const UPDATE_SNAPSHOTS = process.env.UPDATE_SNAPSHOTS === "1";

const ROOT_CONNECTION_STRING =
  process.env.ROOT_DATABASE_URL || "postgres:///postgres";
const CONNECTION_STRING =
  process.env.TEST_SIMPLIFY_DATABASE_URL ||
  "postgres:///pg_simplify_inflectors";
const DATABASE_NAME = parseConnectionString(CONNECTION_STRING).database;
const ROOT = `${__dirname}/tests`;

const withPool = async (connectionString, cb) => {
  const pool = new pg.Pool({
    connectionString,
  });
  try {
    return await cb(pool);
  } finally {
    pool.end();
  }
};

const withClient = async (pool, cb) => {
  const client = await pool.connect();
  try {
    return await cb(client);
  } finally {
    try {
      // Just in case there's any shenanigans
      await client.query("ROLLBACK");
      await client.query("RESET ALL");
    } catch (e) {
      /* IGNORE */
    }

    await client.release();
  }
};

const escapeIdentifier = (str) => `"${str.replace(/"/g, '""')}"`;

async function withCleanDb(cb) {
  await withPool(ROOT_CONNECTION_STRING, async (pool) => {
    await pool.query(
      `DROP DATABASE IF EXISTS ${escapeIdentifier(DATABASE_NAME)};`,
    );
    await pool.query(`CREATE DATABASE ${escapeIdentifier(DATABASE_NAME)};`);
  });
  await withPool(CONNECTION_STRING, async (pool) => cb(pool));
}

async function getSettings(dir) {
  try {
    const json = await fsp.readFile(`${ROOT}/${dir}/settings.json`, "utf8");
    return JSON.parse(json);
  } catch {
    return {};
  }
}

async function getSchema(
  client,
  withSimplify,
  settings,
  withTenants = false,
  keepPool = false,
) {
  const pgServices = [
    makePgService({
      connectionString: CONNECTION_STRING,
      schemas: ["app_public"],
      ...(keepPool ? { pgSettings: (ctx) => ctx.pgSettings } : null),
    }),
  ];
  const result = await makeSchema({
    extends: [
      PostGraphileAmberPreset,
      makeV4Preset({
        simpleCollections: "both",
        ...settings,
      }),
      ...(withSimplify ? [PgSimplifyInflectionPreset] : []),
      ...(withTenants ? [TenantPreset] : []),
    ],
    pgServices,
  });
  // TODO: solve this better!
  // Hack to release the pool
  if (!keepPool) {
    pgServices[0].adaptorSettings.pool.end();
    return result.schema;
  }
  return {
    ...result,
    close: () => pgServices[0].adaptorSettings.pool.end(),
  };
}

async function runTests(pool, dir) {
  const schema = await fsp.readFile(`${ROOT}/${dir}/schema.sql`, "utf8");
  const settings = await getSettings(dir);
  await withClient(pool, async (client) => {
    await client.query(`
      set search_path to public;
      create extension if not exists pgcrypto;

      drop schema if exists app_public cascade;
      create schema app_public;
      set search_path to app_public, public;
    `);
    await client.query(schema);

    const before = await getSchema(client, false, settings);
    const after = await getSchema(client, true, settings);
    if (dir === "gym_franchise") {
      const allocationFields = after.getType("Allocation").getFields();
      assert(
        allocationFields.subscription && allocationFields.family,
        "expected tenancy-aware Allocation.subscription and Allocation.family fields",
      );
      const withoutTenancySettings = {
        ...settings,
        graphileBuildOptions: {
          ...settings.graphileBuildOptions,
          pgMultiTenancy: undefined,
        },
      };
      const withoutTenancy = await getSchema(
        client,
        true,
        withoutTenancySettings,
      );
      assert(
        withoutTenancy.getType("Allocation").getFields().familyGymSubscription,
        "expected existing composite relationship naming with tenancy disabled",
      );
      assert(
        after.getQueryType().getFields().familyByName,
        "expected tenancy columns to be omitted from unique lookup names",
      );
      assert(
        withoutTenancy.getQueryType().getFields().familyByGymIdAndName,
        "expected unique lookup naming to remain unchanged when tenancy is disabled",
      );
      const tenantsWithoutScopes = await getSchema(
        client,
        false,
        withoutTenancySettings,
        true,
      );
      assert.equal(
        printSchema(lexicographicSortSchema(tenantsWithoutScopes)),
        printSchema(lexicographicSortSchema(before)),
        "@graphile/tenants must preserve the schema when scopes are absent",
      );
      const gymLinkFields = after.getType("GymLink").getFields();
      assert(
        gymLinkFields.targetGym,
        "expected explicit target_gym_id relationship to retain its name",
      );
      const membershipFields = after.getType("Membership").getFields();
      assert(
        membershipFields.family,
        "expected the target primary key to remain meaningful in a forward relationship",
      );
      assert(
        membershipFields.membershipAllocations,
        "expected tenancy columns to be omitted from reverse relationship names",
      );
      assert(
        after.getType("Family").getFields().memberships,
        "expected reverse relationships to omit all contextual tenancy columns",
      );
      assert(
        after.getType("Family").getFields().gymId &&
          after.getType("Family").getFields().gym,
        "Simplify Inflection must not hide tenant columns or relationships",
      );
      const joinFields = after.getType("MembershipAllocation").getFields();
      assert(
        joinFields.membership && joinFields.allocation,
        "expected many-to-many join relationships to retain entity identifiers",
      );
      const variantReferenceFields = after
        .getType("VariantReference")
        .getFields();
      assert(
        variantReferenceFields.productVariant,
        "expected unrelated composite foreign keys to retain their key names",
      );

      const tenantsSettings = {
        ...settings,
        disableDefaultMutations: false,
      };
      const conflictingSettings = {
        ...tenantsSettings,
        graphileBuildOptions: {
          ...tenantsSettings.graphileBuildOptions,
          pgMultiTenancy: [
            { table: "gyms", columns: ["gym_id"], implicit: [true] },
            {
              table: "families",
              columns: ["gym_id", "family_id"],
              implicit: [false, false],
            },
          ],
        },
      };
      await assert.rejects(
        getSchema(client, false, conflictingSettings, true),
        /Conflicting pgMultiTenancy implicit classification/,
      );
      const tenantsOnly = await getSchema(client, false, tenantsSettings, true);
      const combined = await getSchema(client, true, tenantsSettings, true);
      for (const schema of [tenantsOnly, combined]) {
        const familyFields = schema.getType("Family").getFields();
        assert(!familyFields.gymId, "implicit Family.gym_id must be hidden");
        assert(
          !Object.keys(familyFields).some((name) => /^gym(?:By|$)/i.test(name)),
          "direct implicit Gym relation must be hidden",
        );
        assert(
          schema.getType("Allocation").getFields().familyId,
          "non-implicit family_id must remain available",
        );
        assert(
          schema.getType("GymSubscription").getFields().family ||
            schema.getType("GymSubscription").getFields()
              .familyByGymIdAndFamilyId,
          "composite business relation to Family must remain visible",
        );
        assert(
          !schema.getType("GymSubscription").getFields().gym,
          "direct implicit Gym relation must be hidden",
        );
        const allocationRelationNames = Object.keys(
          schema.getType("Allocation").getFields(),
        );
        assert(
          allocationRelationNames.some((name) => /subscription/i.test(name)) &&
            allocationRelationNames.some((name) => /family/i.test(name)),
          "business relations containing implicit keys must remain visible",
        );
        assert(
          Object.keys(schema.getType("GymLink").getFields()).some((name) =>
            /targetGym/i.test(name),
          ),
          "cross-tenant targetGym relation must remain visible",
        );
        const joinRelationNames = Object.keys(
          schema.getType("MembershipAllocation").getFields(),
        );
        assert(
          joinRelationNames.some((name) => /membership/i.test(name)) &&
            joinRelationNames.some((name) => /allocation/i.test(name)),
          "many-to-many relationships must retain both business identifiers",
        );
        assert(
          Object.keys(schema.getType("VariantReference").getFields()).some(
            (name) => /productVariant/i.test(name),
          ),
          "unrelated composite foreign keys must remain visible",
        );
        for (const type of Object.values(schema.getTypeMap())) {
          if (
            typeof type.getFields === "function" &&
            /Family.*(?:Input|Patch|Condition|Filter)/.test(type.name)
          ) {
            assert(
              !type.getFields().gymId,
              `${type.name}.gymId must be omitted from generated inputs`,
            );
          }
          if (
            typeof type.getValues === "function" &&
            /Family.*OrderBy/.test(type.name)
          ) {
            assert(
              !type.getValues().some((value) => /GYM_ID/.test(value.name)),
              `${type.name} must not expose implicit gym_id ordering`,
            );
          }
        }
        const queryFieldNames = Object.keys(schema.getQueryType().getFields());
        assert(
          queryFieldNames.some((name) => /familyByName/i.test(name)),
          "implicit scoped unique columns must be omitted from lookup fields",
        );
        assert(
          !queryFieldNames.some((name) => /familyByGymIdAndId/i.test(name)),
          "redundant scoped unique identity must not create a duplicate lookup",
        );
        const mutationFields = schema.getMutationType().getFields();
        let scopedMutations = 0;
        for (const [name, field] of Object.entries(mutationFields)) {
          if (!/(?:update|delete).*family.*name/i.test(name)) continue;
          scopedMutations++;
          const inputArg = field.args.find((arg) => arg.name === "input");
          const inputFields = inputArg?.type.ofType?.getFields?.();
          assert(inputFields, `expected ${name} to have its generated input`);
          assert(
            !inputFields.gymId,
            `${name} must not require the implicit gym_id argument`,
          );
        }
        assert(
          scopedMutations >= 2,
          "expected both scoped unique update and delete mutations",
        );
      }
      const ordinaryAllocationFields = Object.keys(
        before.getType("Allocation").getFields(),
      );
      const tenantsOnlyAllocationFields = Object.keys(
        tenantsOnly.getType("Allocation").getFields(),
      );
      assert(
        ordinaryAllocationFields.some(
          (name) =>
            /subscription/i.test(name) &&
            tenantsOnlyAllocationFields.includes(name),
        ),
        "tenants-only must preserve ordinary relationship naming",
      );
    }
    const beforePath = `${ROOT}/${dir}/schema.unsimplified.graphql`;
    const afterPath = `${ROOT}/${dir}/schema.simplified.graphql`;
    const diffPath = `${ROOT}/${dir}/schema.graphql.diff`;
    await snapshot(printSchema(lexicographicSortSchema(before)), beforePath);
    await snapshot(printSchema(lexicographicSortSchema(after)), afterPath);

    const diff = await new Promise((resolve, reject) => {
      const child = child_process.spawn(
        "diff",
        [
          "-u",
          "--label",
          "unsimplified",
          beforePath,
          "--label",
          "simplified",
          afterPath,
        ],
        {
          stdio: "pipe",
        },
      );
      const buffers = [];

      child.stdout.on("data", (data) => {
        buffers.push(data);
      });

      child.stderr.on("data", (data) => {
        console.error(`stderr: ${data}`);
      });

      child.on("close", (code) => {
        const data = Buffer.concat(buffers).toString("utf8");
        if (data.length) {
          resolve(data);
        } else {
          reject(new Error(`child process exited with code ${code}`));
        }
      });
    });

    await snapshot(diff, diffPath);
    if (dir === "gym_franchise") {
      await runTenantNamingRegressionTests(client, settings);
      await runTenantRuntimeTests(client, settings);
    }
  });
}

async function runTenantNamingRegressionTests(client, settings) {
  await client.query(`
    create table app_public.users (
      gym_id uuid not null references app_public.gyms (id),
      id int not null,
      primary key (gym_id, id)
    );
    create table app_public.posts (
      id int primary key,
      gym_id uuid not null references app_public.gyms (id),
      author_id int not null,
      editor_id int not null,
      approver_id int not null,
      assignee_id int not null,
      foreign key (gym_id, author_id) references app_public.users (gym_id, id),
      foreign key (gym_id, editor_id) references app_public.users (gym_id, id),
      foreign key (gym_id, approver_id) references app_public.users (gym_id, id),
      foreign key (gym_id, assignee_id) references app_public.users (gym_id, id)
    );
    create table app_public.unscoped_records (id int primary key, title text);
  `);
  for (const implicit of [false, true]) {
    const schema = await getSchema(
      client,
      true,
      {
        ...settings,
        disableDefaultMutations: false,
        graphileBuildOptions: {
          ...settings.graphileBuildOptions,
          pgMultiTenancy: [
            { table: "gyms", columns: ["gym_id"], implicit: [implicit] },
          ],
        },
      },
      true,
    );
    const postFields = schema.getType("Post").getFields();
    assert(
      postFields.author && postFields.editor,
      "tenant-first primary keys must preserve author and editor names",
    );
    const userFields = schema.getType("User").getFields();
    assert(
      userFields.authoredPosts &&
        userFields.editedPosts &&
        userFields.postsByApproverId &&
        userFields.postsByAssigneeId,
      `reverse relationships must retain business keys for disambiguation: ${Object.keys(userFields)}`,
    );
    assert(
      !userFields.postsByGymIdAndAuthorId &&
        !userFields.postsByGymIdAndEditorId,
      "reverse relationship fallback must omit tenant context",
    );
    const mutations = schema.getMutationType().getFields();
    for (const mode of ["update", "delete"]) {
      assert(
        mutations[`${mode}UnscopedRecord`] &&
          mutations[`${mode}UnscopedRecordByNodeId`],
        "raw primary key and Node ID mutations must both remain available",
      );
    }
  }
}

async function runTenantRuntimeTests(client, settings) {
  const gymA = "11111111-1111-4111-8111-111111111111";
  const gymB = "22222222-2222-4222-8222-222222222222";
  await client.query(`
    insert into app_public.gyms (id, name) values
      ('${gymA}', 'Runtime A'), ('${gymB}', 'Runtime B');
    create table app_public.tenant_runtime_subscriptions (
      id uuid primary key default gen_random_uuid(),
      gym_id uuid not null default current_setting('app.gym_id')::uuid
        references app_public.gyms (id),
      slug text not null,
      description text not null,
      unique (gym_id, slug)
    );
    alter table app_public.tenant_runtime_subscriptions enable row level security;
    alter table app_public.tenant_runtime_subscriptions force row level security;
    create policy tenant_runtime_isolation
      on app_public.tenant_runtime_subscriptions
      using (gym_id = current_setting('app.gym_id', true)::uuid)
      with check (gym_id = current_setting('app.gym_id', true)::uuid);
    insert into app_public.tenant_runtime_subscriptions
      (id, gym_id, slug, description) values
      ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', '${gymA}', 'shared', 'A'),
      ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', '${gymB}', 'shared', 'B');
    do $$ begin
      create role graphile_tenant_runtime nologin;
    exception when duplicate_object then null;
    end $$;
    grant usage on schema app_public to graphile_tenant_runtime;
    grant select, insert, update, delete on
      app_public.tenant_runtime_subscriptions to graphile_tenant_runtime;
  `);

  const scopedSettings = {
    ...settings,
    disableDefaultMutations: false,
    graphileBuildOptions: {
      ...settings.graphileBuildOptions,
      pgMultiTenancy: [
        { table: "gyms", columns: ["gym_id"], implicit: [true] },
      ],
    },
  };
  const built = await getSchema(client, false, scopedSettings, true, true);
  const { schema, resolvedPreset } = built;
  const runtimeType = schema.getType("TenantRuntimeSubscription");
  assert(runtimeType, "new scope-conforming table must be discovered");
  assert(
    !runtimeType.getFields().gymId,
    "runtime implicit gym_id must be hidden",
  );
  assert(
    !Object.keys(runtimeType.getFields()).some((name) =>
      /^gym(?:By|$)/i.test(name),
    ),
    "runtime direct implicit Gym relation must be hidden",
  );
  await client.query("begin");
  await client.query("select set_config('role', $1, true)", [
    "graphile_tenant_runtime",
  ]);
  await client.query("select set_config('app.gym_id', $1, true)", [gymB]);
  const directB = await client.query(
    `select description from app_public.tenant_runtime_subscriptions where slug = 'shared'`,
  );
  await client.query("rollback");
  assert.deepEqual(directB.rows, [{ description: "B" }]);
  const queryField = Object.entries(schema.getQueryType().getFields()).find(
    ([name, field]) =>
      /tenantRuntimeSubscription/i.test(name) &&
      field.args.some((arg) => arg.name.toLowerCase() === "slug"),
  );
  assert(queryField, "expected a public lookup by scoped slug");
  const [queryName] = queryField;
  const execute = (source, gymId) =>
    grafast({
      schema,
      resolvedPreset,
      source,
      requestContext: {
        pgSettings: {
          role: "graphile_tenant_runtime",
          "app.gym_id": gymId,
        },
      },
    });
  try {
    const aLookup = await execute(
      `query { ${queryName}(slug: "shared") { id slug description } }`,
      gymA,
    );
    assert.deepEqual(aLookup.errors, undefined, JSON.stringify(aLookup.errors));
    assert.equal(aLookup.data[queryName].description, "A");
    const bLookup = await execute(
      `query { ${queryName}(slug: "shared") { id slug description } }`,
      gymB,
    );
    assert.deepEqual(bLookup.errors, undefined);
    assert.equal(bLookup.data[queryName].description, "B");

    const updateEntry = Object.entries(
      schema.getMutationType().getFields(),
    ).find(([name]) => /update.*tenantRuntimeSubscription.*slug/i.test(name));
    const deleteEntry = Object.entries(
      schema.getMutationType().getFields(),
    ).find(([name]) => /delete.*tenantRuntimeSubscription.*slug/i.test(name));
    assert(updateEntry, "expected an update-by-slug mutation");
    assert(deleteEntry, "expected a delete-by-slug mutation");
    const updateField = updateEntry[1];
    const inputType = updateField.args.find((arg) => arg.name === "input").type
      .ofType;
    const updateInputFields = inputType.getFields();
    assert(!updateInputFields.gymId, "update input must omit implicit gym_id");
    const patchFieldName = Object.keys(updateInputFields).find((name) =>
      /patch/i.test(name),
    );
    assert(patchFieldName, "expected the update input patch field");
    const updateName = updateEntry[0];
    const updatePayloadField = Object.keys(updateField.type.getFields()).find(
      (name) => /tenantRuntimeSubscription/i.test(name),
    );
    const updateSelection = updatePayloadField
      ? ` { ${updatePayloadField} { description } }`
      : " { clientMutationId }";
    const updateResult = await execute(
      `mutation { ${updateName}(input: { slug: "shared", ${patchFieldName}: { description: "updated A" } })${updateSelection} }`,
      gymA,
    );
    assert.deepEqual(updateResult.errors, undefined);
    const verifyA = await client.query(
      `select description from app_public.tenant_runtime_subscriptions where gym_id = $1`,
      [gymA],
    );
    const verifyB = await client.query(
      `select description from app_public.tenant_runtime_subscriptions where gym_id = $1`,
      [gymB],
    );
    assert.equal(verifyA.rows[0].description, "updated A");
    assert.equal(verifyB.rows[0].description, "B");

    const deleteName = deleteEntry[0];
    const deleteResult = await execute(
      `mutation { ${deleteName}(input: { slug: "shared" }) { clientMutationId } }`,
      gymA,
    );
    assert.deepEqual(deleteResult.errors, undefined);
    const remaining = await client.query(
      `select gym_id, description from app_public.tenant_runtime_subscriptions order by gym_id`,
    );
    assert.deepEqual(remaining.rows, [{ gym_id: gymB, description: "B" }]);

    const createEntry = Object.entries(
      schema.getMutationType().getFields(),
    ).find(([name]) => /create.*tenantRuntimeSubscription/i.test(name));
    assert(createEntry, "expected a create subscription mutation");
    const [createName, createField] = createEntry;
    const createInputType = createField.args.find((arg) => arg.name === "input")
      .type.ofType;
    const createInputFields = createInputType.getFields();
    const recordInputName = Object.keys(createInputFields).find((name) => {
      const type =
        createInputFields[name].type.ofType ?? createInputFields[name].type;
      return typeof type.getFields === "function" && type.getFields().slug;
    });
    const createValue = recordInputName
      ? `{ ${recordInputName}: { slug: "created", description: "Created A" } }`
      : `{ slug: "created", description: "Created A" }`;
    const recordOutputName = Object.keys(createField.type.getFields()).find(
      (name) => /tenantRuntimeSubscription/i.test(name),
    );
    assert(
      recordOutputName,
      "expected create payload to expose its created row",
    );
    const createResult = await execute(
      `mutation { ${createName}(input: ${createValue}) { ${recordOutputName} { id slug description } } }`,
      gymA,
    );
    assert.deepEqual(
      createResult.errors,
      undefined,
      JSON.stringify(createResult.errors),
    );
    assert.equal(
      createResult.data[createName][recordOutputName].description,
      "Created A",
    );
    const createdRow = await client.query(
      `select gym_id from app_public.tenant_runtime_subscriptions where slug = 'created'`,
    );
    assert.deepEqual(createdRow.rows, [{ gym_id: gymA }]);

    await client.query("begin");
    await client.query("set local role graphile_tenant_runtime");
    await client.query("select set_config('app.gym_id', $1, true)", [gymA]);
    await assert.rejects(
      client.query(
        `insert into app_public.tenant_runtime_subscriptions (gym_id, slug, description) values ($1, 'cross', 'no')`,
        [gymB],
      ),
      (error) => error.code === "42501",
    );
    await client.query("rollback");
  } finally {
    await built.close();
  }
}

/**
 * If UPDATE_SNAPSHOTS is set then wrotes the given snapshot to the given
 * filePath, otherwise it asserts that the snapshot matches the previous
 * snapshot.
 */
async function snapshot(actual, filePath) {
  let expected = null;
  try {
    expected = await fsp.readFile(filePath, "utf8");
  } catch (e) {
    /* noop */
  }
  if (!process.env.CI && (expected == null || UPDATE_SNAPSHOTS)) {
    if (expected !== actual) {
      console.warn(`Updated snapshot in '${filePath}'`);
      await fsp.writeFile(filePath, actual);
    }
  } else {
    assert.equal(actual, expected);
  }
}

async function main() {
  const allDirs = await fsp.readdir(ROOT);
  const specified = process.argv[2];
  const specifiedDir = specified
    ? allDirs.filter((d) => d === specified)
    : null;
  const dirs = specifiedDir ?? allDirs;
  for (const dir of dirs) {
    const stat = await fsp.stat(`${ROOT}/${dir}`);
    if (stat.isDirectory()) {
      if (/^[a-z0-9_]+$/.test(dir)) {
        console.log(dir);
        await withCleanDb((pool) => runTests(pool, dir));
      } else {
        console.warn(
          `Skipping '${dir}' because it does not adhere to the naming convention`,
        );
      }
    }
  }
}

withPool("postgres", main).catch((e) => {
  console.error(e);
  process.exit(1);
});
