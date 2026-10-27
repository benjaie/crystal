import type {
  IntrospectionPolicy,
  IntrospectionRequest,
  MultipassOptions,
} from "pg-introspection";

/** PostGraphile's catalog graph. The library itself does not impose these rules. */
const policies: IntrospectionPolicy[] = [];

type Entity = IntrospectionPolicy["entity"];
const request = (
  entity: Entity,
  field: string,
  value: string | number | null | undefined,
  follow = true,
): IntrospectionRequest[] =>
  value == null || value === "0" || value === 0
    ? []
    : [{ entity, match: { [field]: value }, follow }];

function rule(entity: Entity, select: IntrospectionPolicy["select"]) {
  policies.push({ entity, select });
}

rule("classes", (row) => [
  ...request("namespaces", "_id", row.relnamespace),
  ...request("types", "_id", row.reltype),
  ...request("types", "_id", row.reloftype),
  ...(!["i", "I"].includes(row.relkind)
    ? [
        ...request("attributes", "attrelid", row._id),
        ...request("constraints", "conrelid", row._id),
        ...request("indexes", "indrelid", row._id),
        ...request("inherits", "inhrelid", row._id),
        ...request("policies", "polrelid", row._id, false),
      ]
    : []),
]);
rule("attributes", (row) => request("types", "_id", row.atttypid));
rule("procs", (row) => [
  ...request("namespaces", "_id", row.pronamespace),
  ...request("types", "_id", row.prorettype),
  ...(row.proallargtypes ?? row.proargtypes ?? []).flatMap((id: string) =>
    request("types", "_id", id),
  ),
]);
rule("types", (row) => [
  ...request("namespaces", "_id", row.typnamespace),
  ...request("types", "_id", row.typelem),
  ...request("types", "_id", row.typarray),
  ...request("types", "_id", row.typbasetype),
  ...request("classes", "_id", row.typrelid),
  ...(row.typtype === "e" ? request("enums", "enumtypid", row._id, false) : []),
  ...(row.typtype === "r" ? request("ranges", "rngtypid", row._id) : []),
  ...(row.typtype === "d" ? request("constraints", "contypid", row._id) : []),
]);
rule("ranges", (row) => request("types", "_id", row.rngsubtype));
rule("indexes", (row) => request("classes", "_id", row.indexrelid));
rule("inherits", (row) => request("classes", "_id", row.inhparent));
rule("constraints", (row) => request("namespaces", "_id", row.connamespace));

const catalogs: Partial<Record<Entity, string>> = {
  namespaces: "pg_namespace",
  classes: "pg_class",
  procs: "pg_proc",
  types: "pg_type",
  constraints: "pg_constraint",
  enums: "pg_enum",
  extensions: "pg_extension",
};
for (const [entity, catalog] of Object.entries(catalogs) as Array<
  [Entity, string]
>) {
  rule(entity, (row, data) => {
    const catalogId = Object.entries(data.catalog_by_oid ?? {}).find(
      ([, name]) => name === catalog,
    )?.[0];
    if (!catalogId || !row._id) return [];
    const requests: IntrospectionRequest[] = [
      {
        entity: "descriptions",
        match: { classoid: catalogId, objoid: row._id },
        follow: false,
      },
      {
        entity: "depends",
        match: { classid: catalogId, objid: row._id },
        follow: false,
      },
    ];
    return requests;
  });
}

// Exclude extension resources before fetching them. The condition is supplied by
// PostGraphile, rather than baked into the generic introspection engine.
const notExtension = (catalog: string, oid: string) =>
  `not exists (select 1 from pg_catalog.pg_depend d where d.classid = 'pg_catalog.${catalog}'::regclass and d.objid = ${oid} and d.refclassid = 'pg_catalog.pg_extension'::regclass and d.deptype = 'e')`;

export const postgraphileMultipass: MultipassOptions = {
  policies,
  filters: {
    classes: notExtension("pg_class", "pg_class.oid"),
    procs: notExtension("pg_proc", "pg_proc.oid"),
    types: `(typrelid = 0 or ${notExtension("pg_class", "typrelid")})`,
    depends: "deptype in ('a', 'e')",
  },
  prune(data) {
    const classes = new Set(data.classes?.map((row) => row._id));
    const attributes = new Set(
      data.attributes?.map((row) => `${row.attrelid}:${row.attnum}`),
    );
    if (data.constraints) {
      data.constraints = data.constraints.filter((constraint) => {
        if (constraint.contype !== "f") return true;
        return (
          classes.has(constraint.conrelid) &&
          classes.has(constraint.confrelid) &&
          constraint.conkey != null &&
          constraint.confkey != null &&
          constraint.conkey.length > 0 &&
          constraint.conkey.length === constraint.confkey.length &&
          constraint.conkey.every((n) =>
            attributes.has(`${constraint.conrelid}:${n}`),
          ) &&
          constraint.confkey.every((n) =>
            attributes.has(`${constraint.confrelid}:${n}`),
          )
        );
      });
    }
    if (data.indexes) {
      data.indexes = data.indexes.filter(
        (index) =>
          classes.has(index.indrelid) &&
          index.indkey.every(
            (number) =>
              number === 0 || attributes.has(`${index.indrelid}:${number}`),
          ),
      );
    }
  },
};
