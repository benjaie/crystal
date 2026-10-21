import type { PgCodec, PgRegistry } from "@dataplan/pg";

/** A foreign-key scope whose local columns identify a tenant context. */
export interface TenantScope {
  table: string;
  columns: string[];
  /** Whether each local scope column is managed by the database/context. */
  implicit?: boolean[];
}

declare global {
  namespace GraphileConfig {
    interface Plugins {
      PgMultiTenancyConfig: true;
    }
  }
  namespace GraphileBuild {
    interface SchemaOptions {
      pgMultiTenancy?: TenantScope[];
      /** @internal Enabled by the \@graphile/tenants preset. */
      pgEnableTenants?: boolean;
    }
  }
}

function normalizeTableName(name: string): string {
  const parts: string[] = [];
  let part = "";
  let quoted = false;
  for (let i = 0; i < name.length; i++) {
    const char = name[i];
    if (char === '"') {
      if (quoted && name[i + 1] === '"') {
        part += '"';
        i++;
      } else quoted = !quoted;
    } else if (char === "." && !quoted) {
      parts.push(part);
      part = "";
    } else part += char;
  }
  parts.push(part);
  return parts.slice(-2).join(".");
}

function codecName(codec: PgCodec): string {
  const pg = codec.extensions?.pg as
    | { schemaName?: string; name?: string }
    | undefined;
  return pg?.schemaName && pg.name ? `${pg.schemaName}.${pg.name}` : codec.name;
}

export interface PgMultiTenancyRelationMetadata {
  localName: string;
  remoteName: string;
  localAttributes: readonly string[];
  remoteAttributes: readonly string[];
}

export function classifyPgMultiTenancyRelations(
  relations: readonly PgMultiTenancyRelationMetadata[],
  scopes: readonly TenantScope[],
): {
  columnsByName: Map<string, Set<string>>;
  implicitByName: Map<string, Map<string, boolean>>;
} {
  const scopesWithImplicit = scopes.map((scope) => {
    const implicit = scope.implicit ?? scope.columns.map(() => false);
    if (implicit.length !== scope.columns.length) {
      throw new Error(
        `pgMultiTenancy scope for '${scope.table}' has ${scope.columns.length} columns but ${implicit.length} implicit flags`,
      );
    }
    return { ...scope, implicit, table: normalizeTableName(scope.table) };
  });
  const columnsByName = new Map<string, Set<string>>();
  const implicitByName = new Map<string, Map<string, boolean>>();
  let changed = true;
  while (changed) {
    changed = false;
    for (const relation of relations) {
      for (const scope of scopesWithImplicit) {
        if (
          relation.remoteName !== scope.table &&
          !(
            !scope.table.includes(".") &&
            relation.remoteName.endsWith(`.${scope.table}`)
          )
        ) {
          continue;
        }
        const localSet = new Set(relation.localAttributes);
        if (
          localSet.size !== scope.columns.length ||
          !scope.columns.every((column) => localSet.has(column))
        ) {
          continue;
        }
        const columns = columnsByName.get(relation.localName) ?? new Set();
        const classifications =
          implicitByName.get(relation.localName) ?? new Map();
        for (let i = 0; i < scope.columns.length; i++) {
          const column = scope.columns[i];
          const old = classifications.get(column);
          if (old !== undefined && old !== scope.implicit[i]) {
            throw new Error(
              `Conflicting pgMultiTenancy implicit classification for ${relation.localName}.${column}`,
            );
          }
          if (!columns.has(column)) changed = true;
          columns.add(column);
          classifications.set(column, scope.implicit[i]);
        }
        columnsByName.set(relation.localName, columns);
        implicitByName.set(relation.localName, classifications);
      }
      const remoteColumns = columnsByName.get(relation.remoteName);
      if (!remoteColumns) continue;
      const localColumns = columnsByName.get(relation.localName) ?? new Set();
      const localImplicit = implicitByName.get(relation.localName) ?? new Map();
      const remoteImplicit = implicitByName.get(relation.remoteName);
      for (let i = 0; i < relation.localAttributes.length; i++) {
        const remoteColumn = relation.remoteAttributes[i];
        if (!remoteColumns.has(remoteColumn)) continue;
        const localColumn = relation.localAttributes[i];
        const value = remoteImplicit?.get(remoteColumn) ?? false;
        const old = localImplicit.get(localColumn);
        if (old !== undefined && old !== value) {
          throw new Error(
            `Conflicting pgMultiTenancy implicit classification for ${relation.localName}.${localColumn}`,
          );
        }
        if (!localColumns.has(localColumn)) changed = true;
        localColumns.add(localColumn);
        localImplicit.set(localColumn, value);
      }
      columnsByName.set(relation.localName, localColumns);
      implicitByName.set(relation.localName, localImplicit);
    }
  }
  return { columnsByName, implicitByName };
}

/** Classifies the codecs in one registry without retaining any global state. */
export function classifyPgMultiTenancy(
  registry: PgRegistry,
  scopes: readonly TenantScope[] | undefined,
): {
  columnsByCodec: Map<PgCodec, ReadonlySet<string>>;
  implicitColumnsByCodec: Map<PgCodec, ReadonlySet<string>>;
} {
  const columnsByCodec = new Map<PgCodec, ReadonlySet<string>>();
  const implicitColumnsByCodec = new Map<PgCodec, ReadonlySet<string>>();
  if (scopes?.length) {
    const relations = Object.values(registry.pgRelations).flatMap((map) =>
      Object.values(map).filter((relation) => !relation.isReferencee),
    );
    const { columnsByName, implicitByName } = classifyPgMultiTenancyRelations(
      relations.map((relation) => ({
        localName: codecName(relation.localCodec),
        remoteName: codecName(relation.remoteResource.codec),
        localAttributes: relation.localAttributes,
        remoteAttributes: relation.remoteAttributes,
      })),
      scopes,
    );
    for (const relation of relations) {
      const codec = relation.localCodec;
      if (columnsByCodec.has(codec)) continue;
      const name = codecName(codec);
      columnsByCodec.set(codec, columnsByName.get(name) ?? new Set());
      implicitColumnsByCodec.set(
        codec,
        new Set(
          [...(implicitByName.get(name) ?? [])]
            .filter(([, implicit]) => implicit)
            .map(([column]) => column),
        ),
      );
    }
  }
  return { columnsByCodec, implicitColumnsByCodec };
}

/**
 * Stateless lookup for inflectors, which run outside the schema build hooks.
 * Schema plugins should use the cached build.getPgMultiTenancyColumns method.
 */
export function getPgMultiTenancyColumns(
  codec: PgCodec,
  registry: PgRegistry,
  scopes: readonly TenantScope[] | undefined,
): ReadonlySet<string> {
  return (
    classifyPgMultiTenancy(registry, scopes).columnsByCodec.get(codec) ??
    new Set()
  );
}

/** True only for an FK whose local columns exactly match one configured scope. */
export function isPgMultiTenancyRelation(
  relation: {
    localAttributes: readonly string[];
    remoteResource: { codec: PgCodec };
  },
  scopes: readonly TenantScope[] | undefined,
): boolean {
  if (!scopes?.length) return false;
  const remoteName = codecName(relation.remoteResource.codec);
  const localColumns = new Set(relation.localAttributes);
  return scopes.some((scope) => {
    const targetName = normalizeTableName(scope.table);
    const targetMatches =
      remoteName === targetName ||
      (!targetName.includes(".") && remoteName.endsWith(`.${targetName}`));
    return (
      targetMatches &&
      localColumns.size === scope.columns.length &&
      scope.columns.every((column) => localColumns.has(column))
    );
  });
}
