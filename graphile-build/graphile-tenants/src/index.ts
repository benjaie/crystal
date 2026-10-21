import type { PgCodec, PgRegistry } from "@dataplan/pg";
import type { GraphileConfig } from "graphile-config";
import { getPgMultiTenancyColumns } from "graphile-build-pg";
import type { TenantScope } from "graphile-build-pg";

declare global {
  namespace GraphileConfig {
    interface Plugins {
      PgTenantsPlugin: true;
    }
  }
}

/** Hides database-managed tenant values from the generated GraphQL API. */
export const PgTenantsPlugin: GraphileConfig.Plugin = {
  name: "PgTenantsPlugin",
  version: "0.0.0",
  schema: {
    entityBehavior: {
      pgCodecAttribute: {
        inferred: {
          after: ["inferred"],
          provides: ["pgTenantImplicit"],
          callback(behavior, [codec, attributeName], build) {
            const scopes = build.options.pgMultiTenancy;
            const implicit = getImplicitColumns(
              codec,
              (build as any).input.pgRegistry,
              scopes,
            );
            return implicit.has(attributeName)
              ? [
                  behavior,
                  "-select",
                  "-base",
                  "-insert",
                  "-update",
                  "-filterBy",
                  "-condition:attribute:filterBy",
                  "-orderBy",
                ]
              : behavior;
          },
        },
      },
      pgCodecRelation: {
        inferred: {
          after: ["inferred"],
          provides: ["pgTenantImplicit"],
          callback(behavior, relation, build) {
            const scopes = build.options.pgMultiTenancy;
            if (!scopes?.length) return behavior;
            const remoteName = getTableName(relation.remoteResource.codec);
            for (const scope of scopes) {
              if (!matchesTable(remoteName, scope.table)) continue;
              const implicit = scope.implicit ?? scope.columns.map(() => false);
              if (
                implicit.length === scope.columns.length &&
                implicit.every(Boolean) &&
                sameSet(relation.localAttributes, scope.columns)
              ) {
                return [behavior, "-*"];
              }
            }
            return behavior;
          },
        },
      },
    },
  },
};

function getImplicitColumns(
  codec: PgCodec,
  registry: PgRegistry,
  scopes: readonly TenantScope[] | undefined,
): ReadonlySet<string> {
  const classified = getPgMultiTenancyColumns(codec, registry, scopes);
  if (!scopes?.length || !codec.attributes) return EMPTY;
  const implicit = new Set<string>();
  for (const map of Object.values(registry.pgRelations)) {
    for (const relation of Object.values(map)) {
      if (relation.localCodec !== codec) continue;
      const remoteName = getTableName(relation.remoteResource.codec);
      for (const scope of scopes) {
        if (!matchesTable(remoteName, scope.table)) continue;
        const flags = scope.implicit ?? scope.columns.map(() => false);
        for (let i = 0; i < relation.localAttributes.length; i++) {
          const column = relation.localAttributes[i];
          const index = scope.columns.indexOf(column);
          if (index !== -1 && flags[index] && classified.has(column)) {
            implicit.add(column);
          }
        }
      }
    }
  }
  return implicit;
}

function getTableName(codec: PgCodec): string {
  const pg = codec.extensions?.pg as
    | { schemaName?: string; name?: string }
    | undefined;
  return pg?.schemaName && pg.name ? `${pg.schemaName}.${pg.name}` : codec.name;
}

function matchesTable(actual: string, expected: string): boolean {
  const normalize = (name: string) => name.replaceAll('"', "");
  const normalized = normalize(expected);
  return (
    normalize(actual) === normalized ||
    (!normalized.includes(".") && normalize(actual).endsWith(`.${normalized}`))
  );
}

function sameSet(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && b.every((item) => a.includes(item));
}

const EMPTY: ReadonlySet<string> = new Set();

export const TenantPreset: GraphileConfig.Preset = {
  plugins: [PgTenantsPlugin],
};

export default TenantPreset;
