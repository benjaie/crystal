import { augmentIntrospectionData } from "./augmentIntrospection.ts";
import type {
  DefaultEntities,
  Introspection,
  IntrospectionDataForEntities,
  IntrospectionPolicy,
  IntrospectionRequest,
  IntrospectOptions,
} from "./interfaces.ts";
import { ENTITY_TYPES, type EntityType } from "./introspection.ts";
import {
  buildIntrospectionQuery,
  defaultEntities,
  makeIntrospectionQuery,
} from "./query.ts";

type RunQuery = (text: string, values: any[]) => Promise<string>;
type Row = Record<string, any>;

/** Stable identities for catalogs which do not have an OID. */
const keys: Partial<Record<EntityType, string[]>> = {
  attributes: ["attrelid", "attnum"],
  indexes: ["indexrelid"],
  inherits: ["inhrelid", "inhparent", "inhseqno"],
  ranges: ["rngtypid"],
  auth_members: ["roleid", "member", "grantor"],
  depends: [
    "classid",
    "objid",
    "objsubid",
    "refclassid",
    "refobjid",
    "refobjsubid",
    "deptype",
  ],
  descriptions: ["classoid", "objoid", "objsubid"],
  policies: ["polrelid", "polname"],
  foreign_tables: ["ftrelid"],
  aggregates: ["aggfnoid"],
  stat_user_tables: ["relid"],
  settings: ["name"],
  db_role_settings: ["setdatabase", "setrole"],
};

function identity(entity: EntityType, row: Row): string {
  const fields =
    keys[entity] ?? (ENTITY_TYPES[entity].oids ? ["_id"] : undefined);
  return JSON.stringify(fields ? fields.map((field) => row[field]) : row);
}

function selectorKey(request: IntrospectionRequest): string {
  return JSON.stringify([
    request.entity,
    Object.entries(request.match)
      .map(([column, value]) => [column, String(value)])
      .sort(([a], [b]) => a.localeCompare(b)),
  ]);
}

function entrypointQuery<TEntities extends EntityType>(
  options: IntrospectOptions<TEntities>,
) {
  const { schemas, multipass } = options;
  if (!multipass) throw new Error("Multipass options required");
  const values: any[] = [];
  const overrides: Record<string, string> = {};
  const output = new Set<EntityType>(options.onlyEntities ?? defaultEntities);
  const ctes = new Set(output);
  // Some generated predicates reference a CTE without listing it in requires.
  const implicitRequirements: Partial<Record<EntityType, EntityType[]>> = {
    attribute_defaults: ["attributes"],
    stat_user_tables: ["classes"],
    ...(multipass.onlyAccessible ? { constraints: ["classes"] } : {}),
  };
  const addRequirements = (entity: EntityType) => {
    for (const required of [
      ...(ENTITY_TYPES[entity].requires ?? []),
      ...(implicitRequirements[entity] ?? []),
    ]) {
      if (!ctes.has(required)) {
        ctes.add(required);
        addRequirements(required);
      }
    }
  };
  for (const entity of output) addRequirements(entity);
  if (
    ![
      "namespaces",
      "classes",
      "constraints",
      "procs",
      "types",
      "enums",
      "extensions",
      "foreign_data_wrappers",
      "foreign_servers",
      "rewrites",
      "triggers",
    ].some((entity) => output.has(entity as EntityType))
  ) {
    overrides.depends = "false";
    overrides.descriptions = "false";
  }
  if (schemas && ctes.has("namespaces")) {
    values.push(schemas);
    overrides.namespaces = `nspname = any($${values.length}::text[])`;
  }
  const namespace = (column: string) =>
    schemas
      ? `${column} in (select _id from namespaces)`
      : `${column} in (select _id from namespaces where nspname <> 'information_schema' and nspname not like 'pg\\_%')`;
  overrides.classes = namespace("relnamespace");
  overrides.procs = `${namespace("pronamespace")} and prorettype operator(pg_catalog.<>) 2279`;
  overrides.types = namespace("typnamespace");
  if (ctes.has("classes")) {
    overrides.constraints = "conrelid in (select _id from classes)";
  }
  let prefix = "";
  if (multipass.onlyAccessible) {
    values.push(multipass.onlyAccessible.role ?? null);
    const roleParam = `$${values.length}`;
    const membership = multipass.onlyAccessible.includeNoInherit
      ? ` or pg_catalog.pg_has_role(selected.role_oid, r.oid, 'MEMBER')`
      : "";
    prefix = `selected_role as (select pg_catalog.quote_ident(coalesce(${roleParam}::text, current_user))::regrole::oid as role_oid),\n  accessible_roles as (select r.oid from pg_catalog.pg_roles r, selected_role selected where r.oid = selected.role_oid${membership})`;
    const accessible = (schema: string, privilege: string) =>
      `exists (select 1 from accessible_roles ar where pg_catalog.has_schema_privilege(ar.oid, ${schema}, 'USAGE') and (${privilege}))`;
    const tablePrivilege = `case when relkind in ('r','p','v','m','f') then (pg_catalog.has_table_privilege(ar.oid, pg_class.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') or pg_catalog.has_any_column_privilege(ar.oid, pg_class.oid, 'SELECT,INSERT,UPDATE,REFERENCES')) when relkind = 'S' then pg_catalog.has_sequence_privilege(ar.oid, pg_class.oid, 'USAGE,SELECT,UPDATE') else false end`;
    overrides.classes += ` and ${accessible("relnamespace", tablePrivilege)}`;
    overrides.procs += ` and ${accessible("pronamespace", "pg_catalog.has_function_privilege(ar.oid, pg_proc.oid, 'EXECUTE')")}`;
    overrides.types += ` and ${accessible("typnamespace", "pg_catalog.has_type_privilege(ar.oid, pg_type.oid, 'USAGE')")}`;
  }
  return {
    text: buildIntrospectionQuery(
      { onlyEntities: [...ctes] },
      overrides,
      multipass.filters,
      prefix,
      true,
      true,
      output,
    ),
    values,
  };
}

function makeFetchQuery(
  requests: readonly IntrospectionRequest[],
  filters: Partial<Record<EntityType, string>> | undefined,
) {
  const groups = new Map<EntityType, IntrospectionRequest[]>();
  for (const request of requests) {
    const group = groups.get(request.entity) ?? [];
    group.push(request);
    groups.set(request.entity, group);
  }
  const values: any[] = [];
  const overrides: Record<string, string> = {};
  // These catalog predicates are independent of the scope-defining CTEs in
  // ENTITY_TYPES and must also apply when an object is fetched by identity.
  const invariantFilters: Partial<Record<EntityType, string>> = {
    database: "datname = current_database()",
    attributes: "attnum > 0",
    procs: "prorettype operator(pg_catalog.<>) 2279",
  };
  const effectiveFilters = { ...filters };
  for (const entity of groups.keys()) {
    const invariant = invariantFilters[entity];
    if (invariant) {
      effectiveFilters[entity] = filters?.[entity]
        ? `(${invariant}) and (${filters[entity]})`
        : invariant;
    }
  }
  for (const [entity, group] of groups) {
    const shapes = new Map<
      string,
      { columns: string[]; requests: IntrospectionRequest[] }
    >();
    for (const request of group) {
      const columns = Object.keys(request.match).sort();
      if (!columns.length) throw new Error(`Empty selector for '${entity}'`);
      const shapeKey = JSON.stringify(columns);
      const shape = shapes.get(shapeKey) ?? { columns, requests: [] };
      shape.requests.push(request);
      shapes.set(shapeKey, shape);
    }
    const expressions = [...shapes.values()].map(({ columns, requests }) => {
      const params = columns.map((column) => {
        if (!/^[a-z_][a-z0-9_]*$/.test(column))
          throw new Error(`Invalid catalog column '${column}'`);
        values.push(requests.map((request) => String(request.match[column])));
        return `$${values.length}::text[]`;
      });
      if (columns.length === 1) {
        const column =
          columns[0] === "_id"
            ? `${ENTITY_TYPES[entity].tableName}.oid`
            : columns[0];
        return `${column}::text = any(${params[0]})`;
      }
      const names = columns.map((_, index) => `v${index}`);
      const comparisons = columns.map((column, index) => {
        const sqlColumn =
          column === "_id" ? `${ENTITY_TYPES[entity].tableName}.oid` : column;
        return `${sqlColumn}::text = selector.${names[index]}`;
      });
      return `exists (select 1 from unnest(${params.join(", ")}) as selector(${names.join(", ")}) where ${comparisons.join(" and ")})`;
    });
    overrides[entity] = expressions
      .map((expression) => `(${expression})`)
      .join(" or ");
  }
  return {
    text: buildIntrospectionQuery(
      { onlyEntities: [...groups.keys()] },
      overrides,
      effectiveFilters,
      "",
      false,
      false,
    ),
    values,
  };
}

export async function introspectRaw<
  TEntities extends EntityType = DefaultEntities,
>(
  options: IntrospectOptions<TEntities>,
  runQuery: RunQuery,
): Promise<IntrospectionDataForEntities<TEntities>> {
  if (!options.multipass) {
    return JSON.parse(await runQuery(makeIntrospectionQuery(options), []));
  }
  const { policies = [], filters, prune } = options.multipass;
  const selected = new Set<EntityType>(options.onlyEntities ?? defaultEntities);
  const query = entrypointQuery(options);
  const result: IntrospectionDataForEntities<TEntities> = JSON.parse(
    await runQuery(query.text, query.values),
  );
  const data = result as Record<string, any>;
  const rows = new Map<EntityType, Map<string, Row>>();
  const expanded = new Set<string>();
  const requested = new Set<string>();
  const matches = new Map<string, Row[]>();
  const frontier: Array<[EntityType, Row]> = [];
  const rules = new Map<EntityType, IntrospectionPolicy[]>();
  for (const policy of policies) {
    if (!selected.has(policy.entity)) continue;
    const list = rules.get(policy.entity) ?? [];
    list.push(policy);
    rules.set(policy.entity, list);
  }
  for (const entity of selected) {
    const collection = data[entity];
    if (ENTITY_TYPES[entity].singular && collection) {
      rows.set(entity, new Map([[identity(entity, collection), collection]]));
      frontier.push([entity, collection]);
      continue;
    }
    if (!Array.isArray(collection)) continue;
    const index = new Map<string, Row>();
    rows.set(entity, index);
    for (const row of collection) {
      index.set(identity(entity, row), row);
      frontier.push([entity, row]);
    }
  }
  // The initial CTEs already fetched these complete collections for each root
  // class. Do not query them again merely because a policy follows that edge.
  const initialClassCollections: Array<[EntityType, string]> = [
    ["attributes", "attrelid"],
    ["constraints", "conrelid"],
    ["indexes", "indrelid"],
    ["inherits", "inhrelid"],
    ["policies", "polrelid"],
  ];
  if (selected.has("classes")) {
    for (const row of data.classes ?? []) {
      for (const [entity, column] of initialClassCollections) {
        if (selected.has(entity)) {
          requested.add(selectorKey({ entity, match: { [column]: row._id } }));
        }
      }
    }
  }
  if (selected.has("descriptions")) {
    const catalogOids = new Map(
      Object.entries(result.catalog_by_oid).map(([oid, name]) => [name, oid]),
    );
    const describedEntities: EntityType[] = [
      "namespaces",
      "classes",
      "constraints",
      "procs",
      "types",
      "enums",
      "extensions",
      "foreign_data_wrappers",
      "foreign_servers",
      "rewrites",
      "triggers",
    ];
    for (const entity of describedEntities) {
      if (!selected.has(entity)) continue;
      const catalogId = catalogOids.get(ENTITY_TYPES[entity].tableName);
      if (!catalogId) continue;
      for (const row of data[entity] ?? []) {
        if (row._id != null) {
          requested.add(
            selectorKey({
              entity: "descriptions",
              match: { classoid: catalogId, objoid: row._id },
            }),
          );
        }
      }
    }
  }
  while (frontier.length) {
    const pending = new Map<string, IntrospectionRequest>();
    for (const [entity, row] of frontier.splice(0)) {
      const key = JSON.stringify([entity, identity(entity, row)]);
      if (expanded.has(key)) continue;
      expanded.add(key);
      for (const policy of rules.get(entity) ?? []) {
        for (const request of policy.select(row, result) ?? []) {
          if (!selected.has(request.entity)) continue;
          const entries = Object.entries(request.match);
          if (!entries.length || entries.some(([, value]) => value == null))
            continue;
          const key = selectorKey(request);
          const objectId =
            entries.length === 1 && entries[0][0] === "_id"
              ? JSON.stringify([String(entries[0][1])])
              : undefined;
          const existing = objectId && rows.get(request.entity)?.get(objectId);
          if (existing) {
            if (request.follow) frontier.push([request.entity, existing]);
            continue;
          }
          if (requested.has(key)) {
            if (request.follow) {
              for (const matched of matches.get(key) ?? []) {
                frontier.push([request.entity, matched]);
              }
              matches.delete(key);
            }
            continue;
          }
          const previous = pending.get(key);
          if (!previous || request.follow) pending.set(key, request);
        }
      }
    }
    if (!pending.size) continue;
    const requests = [...pending.values()];
    for (const request of requests) {
      const key = selectorKey(request);
      requested.add(key);
    }
    const fetch = makeFetchQuery(requests, filters);
    const fetched: Record<string, Row[] | Row | null> = JSON.parse(
      await runQuery(fetch.text, fetch.values),
    );
    for (const entity of Object.keys(fetched) as EntityType[]) {
      let index = rows.get(entity);
      if (!index) rows.set(entity, (index = new Map()));
      const collection: Row[] | undefined = ENTITY_TYPES[entity].singular
        ? undefined
        : (data[entity] ?? (data[entity] = []));
      const shapes = new Map<string, string[]>();
      for (const request of requests) {
        if (request.entity !== entity) continue;
        const fields = Object.keys(request.match).sort();
        shapes.set(JSON.stringify(fields), fields);
      }
      const fetchedRows = fetched[entity];
      for (const row of Array.isArray(fetchedRows)
        ? fetchedRows
        : fetchedRows
          ? [fetchedRows]
          : []) {
        const id = identity(entity, row);
        const previous = index.get(id);
        const existing = previous ?? row;
        if (!previous) {
          index.set(id, existing);
          if (collection) collection.push(row);
          else data[entity] = row;
        }
        for (const fields of shapes.values()) {
          const match: Record<string, string | number> = {};
          for (const field of fields) match[field] = row[field];
          const key = selectorKey({ entity, match });
          if (!requested.has(key)) continue;
          if (pending.get(key)?.follow) {
            frontier.push([entity, existing]);
          } else {
            const matchingRows = matches.get(key) ?? [];
            matchingRows.push(existing);
            matches.set(key, matchingRows);
          }
        }
      }
    }
  }
  if (prune) {
    const countRows = () =>
      [...selected].reduce(
        (count, entity) =>
          count +
          (Array.isArray(data[entity])
            ? data[entity].length
            : ENTITY_TYPES[entity].singular && data[entity]
              ? 1
              : 0),
        0,
      );
    let before: number;
    let after = countRows();
    do {
      before = after;
      prune(result);
      after = countRows();
      if (after > before)
        throw new Error("Multipass pruning must not add entities");
    } while (after < before);
  }
  return result;
}

export async function introspect<
  TEntities extends EntityType = DefaultEntities,
>(
  options: IntrospectOptions<TEntities>,
  runQuery: RunQuery,
): Promise<Introspection<TEntities>> {
  return augmentIntrospectionData(
    options,
    await introspectRaw(options, runQuery),
  );
}
