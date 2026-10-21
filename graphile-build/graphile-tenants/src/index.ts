import { gatherConfig } from "graphile-build";
import {
  classifyPgMultiTenancyRelations,
  isPgMultiTenancyRelation,
} from "graphile-build-pg";
import type { GraphileConfig } from "graphile-config";

declare global {
  namespace GraphileConfig {
    interface Plugins {
      PgTenantsPlugin: true;
    }
    interface GatherHelpers {
      pgTenants: Record<string, never>;
    }
  }
}

/** Hides database-managed tenant values from the generated GraphQL API. */
export const PgTenantsPlugin: GraphileConfig.Plugin = {
  name: "PgTenantsPlugin",
  version: "0.0.0",
  after: ["PgTablesPlugin"],
  gather: gatherConfig({
    namespace: "pgTenants",
    initialCache: () => Object.freeze({}),
    initialState: () => ({
      implicitColumnsByClass: new Map<any, Set<string>>(),
    }),
    helpers: {},
    hooks: {
      pgIntrospection_introspection(info, { introspection }) {
        const scopes = info.resolvedPreset.schema?.pgMultiTenancy;
        if (!scopes?.length) return;
        const classes: any[] = introspection.classes;
        const classNames = (pgClass: any) => {
          const schema = pgClass.getNamespace()?.nspname;
          return schema ? `${schema}.${pgClass.relname}` : pgClass.relname;
        };
        const relations: any[] = [];
        const attributesByClass = new Map<any, Map<number, string>>();
        for (const pgClass of classes) {
          attributesByClass.set(
            pgClass,
            new Map(
              pgClass
                .getAttributes()
                .map((attribute: any) => [attribute.attnum, attribute.attname]),
            ),
          );
          for (const constraint of pgClass.getConstraints() ?? []) {
            if (constraint.contype !== "f") continue;
            const foreignClass = constraint.getForeignClass();
            if (!foreignClass) continue;
            const localNames = constraint.conkey!.map(
              (number: number) =>
                attributesByClass.get(pgClass)?.get(number) ??
                pgClass.getAttributes().find((a: any) => a.attnum === number)
                  ?.attname,
            );
            const remoteNames = constraint.confkey!.map(
              (number: number) =>
                foreignClass
                  .getAttributes()
                  .find((a: any) => a.attnum === number)?.attname,
            );
            relations.push({ pgClass, foreignClass, localNames, remoteNames });
          }
        }
        const classification = classifyPgMultiTenancyRelations(
          relations.map((relation) => ({
            localName: classNames(relation.pgClass),
            remoteName: classNames(relation.foreignClass),
            localAttributes: relation.localNames,
            remoteAttributes: relation.remoteNames,
          })),
          scopes,
        );
        for (const pgClass of classes) {
          const implicitByName = classification.implicitByName.get(
            classNames(pgClass),
          );
          info.state.implicitColumnsByClass.set(
            pgClass,
            new Set(
              [...(implicitByName ?? [])]
                .filter(([, implicit]) => implicit)
                .map(([column]) => column),
            ),
          );
        }
      },
      pgTables_PgResourceOptions(info, { pgClass, resourceOptions }) {
        const implicit = info.state.implicitColumnsByClass.get(pgClass);
        if (!implicit?.size || !resourceOptions.uniques?.length) return;
        const originals = resourceOptions.uniques as any[];
        const primary = originals.filter((unique) => unique.isPrimary);
        const canonical = new Set<string>();
        for (const unique of primary) {
          if (
            !unique.attributes.some((attribute: string) =>
              implicit.has(attribute),
            )
          ) {
            canonical.add([...unique.attributes].sort().join("\0"));
          }
        }
        const byIdentity = new Map<
          string,
          { unique: any; shortened: boolean }
        >();
        for (const unique of originals) {
          if (unique.isPrimary) continue;
          const attributes = unique.attributes.filter(
            (attribute: string) => !implicit.has(attribute),
          );
          if (!attributes.length) continue;
          const shortened = attributes.length !== unique.attributes.length;
          const key = [...attributes].sort().join("\0");
          if (shortened && canonical.has(key)) continue;
          const previous = byIdentity.get(key);
          if (!previous || (previous.shortened && !shortened)) {
            if (shortened) (unique as any).attributes = attributes;
            byIdentity.set(key, { unique, shortened });
          }
        }
        const shortenedPrimary = primary.flatMap((unique) => {
          const attributes = unique.attributes.filter(
            (attribute: string) => !implicit.has(attribute),
          );
          if (!attributes.length) return [];
          const key = [...attributes].sort().join("\0");
          if (canonical.has(key) || byIdentity.has(key)) return [];
          canonical.add(key);
          return [{ ...unique, attributes, isPrimary: false }];
        });
        (resourceOptions as any).uniques = [
          ...primary,
          ...shortenedPrimary,
          ...byIdentity.values(),
        ].map((entry: any) => entry.unique ?? entry);
      },
    },
  }),
  schema: {
    entityBehavior: {
      pgCodecAttribute: {
        inferred: {
          after: ["inferred"],
          provides: ["pgTenantImplicit"],
          callback(behavior, [codec, attributeName], build) {
            const implicit = build.getPgImplicitMultiTenancyColumns(codec);
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
            if (
              isPgMultiTenancyRelation(relation, scopes) &&
              relation.localAttributes.every((column: string) =>
                build
                  .getPgImplicitMultiTenancyColumns(relation.localCodec)
                  .has(column),
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

export const TenantPreset: GraphileConfig.Preset = {
  plugins: [PgTenantsPlugin],
  schema: { pgEnableTenants: true },
};

export default TenantPreset;
