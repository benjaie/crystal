import type { PgCodec, PgRegistry } from "@dataplan/pg";
import type { TenantScope } from "graphile-build-pg";
import {
  getPgImplicitMultiTenancyColumns,
  isPgMultiTenancyRelation,
} from "graphile-build-pg";
import type { GraphileConfig } from "graphile-config";

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
            const registry = (build as any).input.pgRegistry;
            if (
              isPgMultiTenancyRelation(relation, scopes) &&
              relation.localAttributes.every((column: string) =>
                getPgImplicitMultiTenancyColumns(
                  relation.localCodec,
                  registry,
                  scopes,
                ).has(column),
              )
            ) {
              return [behavior, "-*"];
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
  return getPgImplicitMultiTenancyColumns(codec, registry, scopes);
}

export const TenantPreset: GraphileConfig.Preset = {
  plugins: [PgTenantsPlugin],
  schema: { pgEnableTenants: true },
};

export default TenantPreset;
