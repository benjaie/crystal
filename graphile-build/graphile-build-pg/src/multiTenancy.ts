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

/**
 * Classifies local FK columns participating in configured scopes. Scope
 * matching is exact and order-independent; subsequent relationships propagate
 * those classifications using PostgreSQL's FK attribute mappings.
 */
export function getPgMultiTenancyColumns(
  codec: PgCodec,
  registry: PgRegistry | undefined,
  scopes: readonly TenantScope[] | undefined,
): ReadonlySet<string> {
  if (!scopes?.length) return EMPTY;
  const scopeKey = JSON.stringify(scopes);
  if (!registry) {
    return (
      latestByName.get(`${scopeKey}:${codecName(codec)}`) ??
      latestByName.get(`${scopeKey}:${codec.name}`) ??
      EMPTY
    );
  }
  const caches = cache.get(registry as object) ?? new Map();
  let byCodec = caches.get(scopeKey);
  if (!byCodec) {
    byCodec = new Map();
    const relations: any[] = [];
    for (const map of Object.values(registry.pgRelations)) {
      for (const relation of Object.values(map)) {
        if (!(relation as any).isReferencee) relations.push(relation);
      }
    }
    const scopesWithImplicit = scopes.map((scope) => {
      const implicit = scope.implicit ?? scope.columns.map(() => false);
      if (implicit.length !== scope.columns.length) {
        throw new Error(
          `pgMultiTenancy scope for '${scope.table}' has ${scope.columns.length} columns but ${implicit.length} implicit flags`,
        );
      }
      return { ...scope, implicit, table: normalizeTableName(scope.table) };
    });
    const byName = new Map<string, Set<string>>();
    const implicitByName = new Map<string, Map<string, boolean>>();
    let changed = true;
    while (changed) {
      changed = false;
      for (const relation of relations) {
        const local = relation.localCodec as PgCodec;
        const remote = relation.remoteResource.codec as PgCodec;
        const localName = codecName(local);
        const remoteName = codecName(remote);
        for (const scope of scopesWithImplicit) {
          if (
            remoteName !== scope.table &&
            !(
              !scope.table.includes(".") &&
              remoteName.endsWith(`.${scope.table}`)
            )
          )
            continue;
          const localSet = new Set<string>();
          const remoteSet = new Set<string>();
          for (let i = 0; i < relation.localAttributes.length; i++) {
            localSet.add(relation.localAttributes[i]);
            remoteSet.add(relation.remoteAttributes[i]);
          }
          if (
            localSet.size !== scope.columns.length ||
            !scope.columns.every((col) => localSet.has(col))
          )
            continue;
          const current = byName.get(localName) ?? new Set<string>();
          const classifications = implicitByName.get(localName) ?? new Map();
          for (let i = 0; i < scope.columns.length; i++) {
            const localColumn = scope.columns[i];
            const remoteColumn =
              relation.remoteAttributes[
                relation.localAttributes.indexOf(localColumn)
              ];
            if (!remoteSet.has(remoteColumn)) continue;
            const old = classifications.get(localColumn);
            if (old !== undefined && old !== scope.implicit[i]) {
              throw new Error(
                `Conflicting pgMultiTenancy implicit classification for ${localName}.${localColumn}`,
              );
            }
            classifications.set(localColumn, scope.implicit[i]);
            current.add(localColumn);
          }
          if (current.size !== (byName.get(localName)?.size ?? 0))
            changed = true;
          byName.set(localName, current);
          implicitByName.set(localName, classifications);
        }
        const remoteColumns = byName.get(remoteName);
        if (!remoteColumns) continue;
        const localColumns = byName.get(localName) ?? new Set<string>();
        const localImplicit = implicitByName.get(localName) ?? new Map();
        const remoteImplicit = implicitByName.get(remoteName);
        for (let i = 0; i < relation.localAttributes.length; i++) {
          const remoteColumn = relation.remoteAttributes[i];
          if (!remoteColumns.has(remoteColumn)) continue;
          const localColumn = relation.localAttributes[i];
          const value = remoteImplicit?.get(remoteColumn) ?? false;
          const old = localImplicit.get(localColumn);
          if (old !== undefined && old !== value) {
            throw new Error(
              `Conflicting pgMultiTenancy implicit classification for ${localName}.${localColumn}`,
            );
          }
          if (!localColumns.has(localColumn)) changed = true;
          localColumns.add(localColumn);
          localImplicit.set(localColumn, value);
        }
        byName.set(localName, localColumns);
        implicitByName.set(localName, localImplicit);
      }
    }
    byCodec = new Map();
    caches.set(scopeKey, byCodec);
    cache.set(registry as object, caches);
    const implicitByCodec = new Map<PgCodec, ReadonlySet<string>>();
    const implicitByScope = implicitCache.get(registry as object) ?? new Map();
    implicitByScope.set(scopeKey, implicitByCodec);
    implicitCache.set(registry as object, implicitByScope);
    for (const relation of relations) {
      const local = relation.localCodec as PgCodec;
      const name = codecName(local);
      if (!byCodec.has(local)) {
        const columns = byName.get(name) ?? EMPTY;
        byCodec.set(local, columns);
        latestByName.set(`${scopeKey}:${name}`, columns);
        latestByName.set(`${scopeKey}:${local.name}`, columns);
        const implicitColumns = new Set(
          [...(implicitByName.get(name) ?? [])]
            .filter(([, value]) => value)
            .map(([column]) => column),
        );
        implicitByCodec.set(local, implicitColumns);
        latestImplicitByName.set(`${scopeKey}:${name}`, implicitColumns);
        latestImplicitByName.set(`${scopeKey}:${local.name}`, implicitColumns);
      }
    }
  }
  return (
    byCodec.get(codec) ??
    latestByName.get(`${JSON.stringify(scopes)}:${codecName(codec)}`) ??
    latestByName.get(`${JSON.stringify(scopes)}:${codec.name}`) ??
    EMPTY
  );
}

/** Returns the classified columns whose values are database-managed. */
export function getPgImplicitMultiTenancyColumns(
  codec: PgCodec,
  registry: PgRegistry | undefined,
  scopes: readonly TenantScope[] | undefined,
): ReadonlySet<string> {
  if (!scopes?.length) return EMPTY;
  const scopeKey = JSON.stringify(scopes);
  if (!registry) {
    return (
      latestImplicitByName.get(`${scopeKey}:${codecName(codec)}`) ??
      latestImplicitByName.get(`${scopeKey}:${codec.name}`) ??
      EMPTY
    );
  }
  getPgMultiTenancyColumns(codec, registry, scopes);
  return (
    implicitCache
      .get(registry as object)
      ?.get(scopeKey)
      ?.get(codec) ??
    latestImplicitByName.get(`${scopeKey}:${codecName(codec)}`) ??
    latestImplicitByName.get(`${scopeKey}:${codec.name}`) ??
    EMPTY
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

const EMPTY: ReadonlySet<string> = new Set();
const cache = new WeakMap<
  object,
  Map<string, Map<PgCodec, ReadonlySet<string>>>
>();
const implicitCache = new WeakMap<
  object,
  Map<string, Map<PgCodec, ReadonlySet<string>>>
>();
const latestByName = new Map<string, ReadonlySet<string>>();
const latestImplicitByName = new Map<string, ReadonlySet<string>>();
