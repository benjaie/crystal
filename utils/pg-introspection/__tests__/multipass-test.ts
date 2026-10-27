import assert from "node:assert/strict";
import { it } from "node:test";

import { introspectRaw } from "../src/multipass.ts";

const metadata = {
  catalog_by_oid: {},
  current_user: "test",
  pg_version: "test",
  introspection_version: 1,
};

it("expands terminal objects reached later, deduplicates requests and terminates cycles", async () => {
  let calls = 0;
  const expanded: string[] = [];
  const result = await introspectRaw(
    {
      onlyEntities: ["namespaces", "types"],
      schemas: ["app_public"],
      multipass: {
        policies: [
          {
            entity: "types",
            select(row) {
              expanded.push(row._id);
              if (row._id === "1")
                return [
                  { entity: "types", match: { _id: 2 } },
                  { entity: "types", match: { _id: "3" }, follow: true },
                  { entity: "classes", match: { _id: "99" }, follow: true },
                ];
              if (row._id === "3")
                return [
                  { entity: "types", match: { _id: "2" }, follow: true },
                  { entity: "types", match: { _id: "4" }, follow: true },
                ];
              if (row._id === "2")
                return [
                  { entity: "types", match: { _id: "1" }, follow: true },
                  { entity: "types", match: { _id: "4" }, follow: true },
                ];
              return [];
            },
          },
        ],
      },
    },
    async (_text, values) => {
      calls++;
      if (calls === 1) {
        assert.deepEqual(values, [["app_public"]]);
        return JSON.stringify({
          ...metadata,
          namespaces: [],
          types: [{ _id: "1" }],
        });
      }
      if (calls === 2) {
        assert.deepEqual(values, [["2", "3"]]);
        return JSON.stringify({ types: [{ _id: "2" }, { _id: "3" }] });
      }
      assert.deepEqual(values, [["4"]]);
      return JSON.stringify({ types: [] });
    },
  );
  assert.equal(calls, 3);
  assert.deepEqual(expanded, ["1", "3", "2"]);
  assert.deepEqual(
    result.types.map((row) => row._id),
    ["1", "2", "3"],
  );
  assert.equal(result.classes, undefined);
});

it("promotes a previously terminal collection without fetching it again", async () => {
  let calls = 0;
  const expanded: string[] = [];
  await introspectRaw(
    {
      onlyEntities: ["namespaces", "classes", "attributes"],
      multipass: {
        policies: [
          {
            entity: "classes",
            select(row) {
              if (row._id === "1")
                return [
                  { entity: "attributes", match: { attrelid: "2" } },
                  { entity: "classes", match: { _id: "3" }, follow: true },
                ];
              if (row._id === "3")
                return [
                  {
                    entity: "attributes",
                    match: { attrelid: "2" },
                    follow: true,
                  },
                ];
              return [];
            },
          },
          {
            entity: "attributes",
            select(row) {
              expanded.push(row.attname);
              return [];
            },
          },
        ],
      },
    },
    async () => {
      calls++;
      if (calls === 1)
        return JSON.stringify({
          ...metadata,
          namespaces: [],
          classes: [{ _id: "1" }],
          attributes: [],
        });
      return JSON.stringify({
        classes: [{ _id: "3" }],
        attributes: [{ attrelid: "2", attnum: 1, attname: "name" }],
      });
    },
  );
  assert.equal(calls, 2);
  assert.deepEqual(expanded, ["name"]);
});

it("preserves the single-pass query when multipass is disabled", async () => {
  let calls = 0;
  await introspectRaw(
    { onlyEntities: ["namespaces"], schemas: ["ignored"] },
    async (text, values) => {
      calls++;
      assert.deepEqual(values, []);
      assert.doesNotMatch(text, /any\(\$1/);
      return JSON.stringify({ ...metadata, namespaces: [] });
    },
  );
  assert.equal(calls, 1);
});

it("passes role and schemas as parameters and applies access checks to entrypoints", async () => {
  await introspectRaw(
    {
      onlyEntities: ["classes"],
      schemas: ["a' schema"],
      multipass: {
        onlyAccessible: { role: "a' role", includeNoInherit: true },
      },
    },
    async (text, values) => {
      assert.deepEqual(values, [["a' schema"], "a' role"]);
      assert.doesNotMatch(text, /a' (?:schema|role)/);
      assert.match(text, /pg_has_role/);
      assert.match(text, /pg_catalog\.quote_ident/);
      assert.match(text, /has_any_column_privilege/);
      return JSON.stringify({ ...metadata, classes: [] });
    },
  );
});

it("preserves attribute filters in follow-up queries and prunes to a fixed point", async () => {
  let calls = 0;
  let prunes = 0;
  const result = await introspectRaw(
    {
      onlyEntities: ["namespaces", "classes", "attributes"],
      multipass: {
        policies: [
          {
            entity: "classes",
            select(row) {
              return [
                {
                  entity: "attributes",
                  match: { attrelid: "2" },
                  follow: true,
                },
              ];
            },
          },
        ],
        prune(data) {
          prunes++;
          if (data.attributes?.length) data.attributes.pop();
        },
      },
    },
    async (text) => {
      calls++;
      if (calls === 1)
        return JSON.stringify({
          ...metadata,
          namespaces: [],
          classes: [{ _id: "1" }],
          attributes: [],
        });
      assert.match(text, /attnum > 0/);
      return JSON.stringify({
        attributes: [
          { attrelid: "2", attnum: 1 },
          { attrelid: "2", attnum: 2 },
        ],
      });
    },
  );
  assert.equal(calls, 2);
  assert.equal(prunes, 3);
  assert.deepEqual(result.attributes, []);
});

it("does not refetch a complete collection from the initial query", async () => {
  let calls = 0;
  const expanded: string[] = [];
  const result = await introspectRaw(
    {
      onlyEntities: ["namespaces", "classes", "attributes"],
      multipass: {
        policies: [
          {
            entity: "classes",
            select(row) {
              return [
                {
                  entity: "attributes",
                  match: { attrelid: row._id },
                  follow: true,
                },
              ];
            },
          },
          {
            entity: "attributes",
            select(row) {
              expanded.push(row.attname);
              return [];
            },
          },
        ],
      },
    },
    async () => {
      calls++;
      return JSON.stringify({
        ...metadata,
        namespaces: [],
        classes: [{ _id: "1" }],
        attributes: [{ attrelid: "1", attnum: 1, attname: "id" }],
      });
    },
  );
  assert.equal(calls, 1);
  assert.deepEqual(expanded, ["id"]);
  assert.equal(result.attributes.length, 1);
});

it("does not require a schema parameter when selected collections do not use it", async () => {
  await introspectRaw(
    { onlyEntities: ["descriptions"], schemas: ["app_public"], multipass: {} },
    async (text, values) => {
      assert.deepEqual(values, []);
      assert.doesNotMatch(text, /\$1/);
      return JSON.stringify({ ...metadata, descriptions: [] });
    },
  );
});

it("applies policies to singular collections", async () => {
  let calls = 0;
  const result = await introspectRaw(
    {
      onlyEntities: ["database", "roles"],
      multipass: {
        policies: [
          {
            entity: "database",
            select(row) {
              return [
                { entity: "roles", match: { _id: row.datdba }, follow: true },
              ];
            },
          },
        ],
      },
    },
    async () => {
      calls++;
      if (calls === 1)
        return JSON.stringify({
          ...metadata,
          database: { _id: "1", datdba: "2" },
          roles: [],
        });
      return JSON.stringify({ roles: [{ _id: "2", rolname: "owner" }] });
    },
  );
  assert.equal(calls, 2);
  assert.equal(result.database.datdba, "2");
  assert.equal(result.roles[0].rolname, "owner");
});
