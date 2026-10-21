import type { PgCodec, PgResourceUnique } from "@dataplan/pg";
import type {} from "graphile-build";
import type {} from "graphile-build-pg";
import type { GraphileConfig } from "graphile-config";

declare global {
  namespace GraphileConfig {
    interface Plugins {
      PgSimplifyInflectionPlugin: true;
    }
  }
}

/**
 * Returns true if array1 and array2 have the same length, and every pair of
 * values within them pass the `comparator` check (which defaults to `===`).
 */
export function arraysMatch<T>(
  array1: ReadonlyArray<T>,
  array2: ReadonlyArray<T>,
  comparator: (val1: T, val2: T) => boolean = (v1, v2) => v1 === v2,
): boolean {
  if (array1 === array2) return true;
  const l = array1.length;
  if (l !== array2.length) {
    return false;
  }
  for (let i = 0; i < l; i++) {
    if (!comparator(array1[i], array2[i])) {
      return false;
    }
  }
  return true;
}

declare global {
  namespace GraphileBuild {
    interface Inflection {
      distinctPluralize(this: GraphileBuild.Inflection, str: string): string;
      // NOTE: methods that may return non-string should be prefixed with `_` to
      // indicate they should only be used from other inflectors
      _getBaseName(
        this: GraphileBuild.Inflection,
        attributeName: string,
      ): string | null;
      _baseNameMatches(
        this: GraphileBuild.Inflection,
        baseName: string,
        otherName: string,
      ): boolean;
      /* This is a good method to override. */
      _getOppositeBaseName(
        this: GraphileBuild.Inflection,
        baseName: string,
      ): string | null;
      _getBaseNameFromKeys(
        this: GraphileBuild.Inflection,
        detailedKeys: Array<{
          codec: PgCodec<any, any, any, any, any, any, any>;
          attributeName: string;
        }>,
      ): string | null;
      _pgMultiTenancyColumns(
        this: GraphileBuild.Inflection,
        codec: PgCodec<any, any, any, any, any, any, any>,
        registry: any,
        schemaOptions: GraphileConfig.ResolvedPreset["schema"],
      ): ReadonlySet<string>;
    }
    interface SchemaOptions {
      pgOmitListSuffix?: boolean;
      pgSimplifyPatch?: boolean;
      pgSimplifyAllRows?: boolean;
      pgShortPk?: boolean;
      pgSimplifyMultikeyRelations?: boolean;
      pgMultiTenancy?: Array<{ table: string; columns: string[] }>;
    }
  }
}

function fixCapitalisedPlural(
  this: GraphileBuild.Inflection,
  previous: ((str: string) => string) | undefined,
  _preset: GraphileConfig.ResolvedPreset,
  str: string,
) {
  const original = previous!(str);
  return original.replace(/[0-9]S(?=[A-Z]|$)/g, (match) => match.toLowerCase());
}

function fixChangePlural(
  this: GraphileBuild.Inflection,
  previous: ((str: string) => string) | undefined,
  _options: GraphileConfig.ResolvedPreset,
  str: string,
): string {
  const matches = str.match(/([A-Z]|_[a-z0-9])[a-z0-9]*_*$/);
  const index = matches ? matches.index! + matches[1].length - 1 : 0;
  const suffixMatches = str.match(/_*$/);
  const suffixIndex = suffixMatches!.index!;
  const prefix = str.substring(0, index);
  const word = str.substring(index, suffixIndex);
  const suffix = str.substring(suffixIndex);
  return `${prefix}${previous!(word)}${suffix}`;
}

function normalizePgTableName(name: string): string {
  const parts: string[] = [];
  let part = "";
  let quoted = false;
  for (let i = 0; i < name.length; i++) {
    const char = name[i];
    if (char === '"') {
      if (quoted && name[i + 1] === '"') {
        part += '"';
        i++;
      } else {
        quoted = !quoted;
      }
    } else if (char === "." && !quoted) {
      parts.push(part);
      part = "";
    } else {
      part += char;
    }
  }
  parts.push(part);
  return parts.slice(-2).join(".");
}

interface PgMultiTenancyMatch {
  columns: Set<string>;
  finalColumns: Set<string>;
  scopeIndex: number;
}

const multiTenancyColumnsByRegistry = new WeakMap<
  object,
  Map<string, PgMultiTenancyMatch>
>();
const latestMultiTenancyColumnsByCodec = new WeakMap<object, Set<string>>();

// Users can add 'listSuffix === "omit"`/`"include"` smart tags, this handles
// that.
let globalPgOmitListSuffix: boolean | null = null;

function overrideListSuffix(
  listSuffix: string | true | Array<string | true> | null | undefined,
  cb: () => string,
): string {
  if (listSuffix == null) {
    return cb();
  }

  if (listSuffix !== "include" && listSuffix !== "omit") {
    throw new Error(
      `Unrecognized @listSuffix value "${listSuffix}". If @listSuffix is set, it must be "omit" or "include".`,
    );
  }
  const oldOverridePgOmitListSuffix = globalPgOmitListSuffix;
  try {
    globalPgOmitListSuffix = listSuffix === "omit";
    return cb();
  } finally {
    globalPgOmitListSuffix = oldOverridePgOmitListSuffix;
  }
}

const PgSimplifyInflectionPlugin: GraphileConfig.Plugin = {
  name: "PgSimplifyInflectionPlugin",
  version: "0.0.0",

  inflection: {
    add: {
      distinctPluralize(_preset, str) {
        const singular = this.singularize(str);
        const plural = this.pluralize(singular);
        if (singular !== plural) {
          return plural;
        }
        if (
          plural.endsWith("ch") ||
          plural.endsWith("s") ||
          plural.endsWith("sh") ||
          plural.endsWith("x") ||
          plural.endsWith("z")
        ) {
          return plural + "es";
        } else if (plural.endsWith("y")) {
          return plural.slice(0, -1) + "ies";
        } else {
          return plural + "s";
        }
      },

      _getBaseName(_preset, attributeName) {
        const matches = attributeName.match(
          /^(.+?)(_row_id|_id|_uuid|_fk|_pk|RowId|Id|Uuid|UUID|Fk|Pk)$/,
        );
        if (matches) {
          return matches[1];
        }
        return null;
      },

      _baseNameMatches(_preset, baseName, otherName) {
        const singularizedName = this.singularize(otherName);
        return this.camelCase(baseName) === this.camelCase(singularizedName);
      },

      /* This is a good method to override. */
      _getOppositeBaseName(_preset, baseName) {
        return (
          (
            {
              /*
               * Changes to this list are breaking changes and will require a
               * major version update, so we need to group as many together as
               * possible! Rather than sending a PR, please look for an open
               * issue called something like "Add more opposites" (if there isn't
               * one then please open it) and add your suggestions to the GitHub
               * comments.
               */
              parent: "child",
              child: "parent",
              author: "authored",
              editor: "edited",
              reviewer: "reviewed",
            } as { [key: string]: string }
          )[baseName] || null
        );
      },

      _getBaseNameFromKeys(preset, detailedKeys) {
        if (detailedKeys.length === 1) {
          const key = detailedKeys[0];
          const attributeName = this._attributeName({
            ...key,
            skipRowId: true,
          });
          return this._getBaseName(attributeName);
        }
        if (preset.schema?.pgSimplifyMultikeyRelations) {
          const attributeNames = detailedKeys.map((key) =>
            this._attributeName({ ...key, skipRowId: true }),
          );
          const baseNames = attributeNames.map((attributeName) =>
            this._getBaseName(attributeName),
          );
          // Check none are null
          if (baseNames.every((n) => n)) {
            return baseNames.join("-");
          }
        }
        return null;
      },

      _pgMultiTenancyColumns(_preset, codec, registry, schemaOptions) {
        const scopes = schemaOptions?.pgMultiTenancy;
        if (!scopes?.length) return new Set();
        if (!registry) {
          return latestMultiTenancyColumnsByCodec.get(codec) ?? new Set();
        }
        let columnsByCodec = multiTenancyColumnsByRegistry.get(registry);
        if (!columnsByCodec) {
          columnsByCodec = new Map();
          const relations: any[] = [];
          const getName = (codec: any) => {
            const pg = codec.extensions?.pg;
            return pg ? `${pg.schemaName}.${pg.name}` : codec.name;
          };
          for (const codecRelations of Object.values(
            registry.pgRelations,
          ) as any[]) {
            for (const relation of Object.values(codecRelations) as any[]) {
              if (!relation.isReferencee) relations.push(relation);
            }
          }
          const getPrimaryKeyAttribute = (codec: any) =>
            (codec.uniques as PgResourceUnique[] | undefined)?.find(
              (unique) => unique.isPrimary,
            )?.attributes[0];
          const considerMatch = (
            name: string,
            candidate: PgMultiTenancyMatch,
          ) => {
            const current = columnsByCodec!.get(name);
            if (
              !current ||
              candidate.columns.size > current.columns.size ||
              (candidate.columns.size === current.columns.size &&
                candidate.finalColumns.size > current.finalColumns.size) ||
              (candidate.columns.size === current.columns.size &&
                candidate.finalColumns.size === current.finalColumns.size &&
                candidate.scopeIndex < current.scopeIndex)
            ) {
              columnsByCodec!.set(name, candidate);
              return true;
            }
            return false;
          };
          let changed = true;
          while (changed) {
            changed = false;
            for (const relation of relations) {
              const localName = getName(relation.localCodec);
              const remoteName = getName(relation.remoteResource.codec);
              for (
                let scopeIndex = 0;
                scopeIndex < scopes.length;
                scopeIndex++
              ) {
                const scope = scopes[scopeIndex];
                const scopeTable = normalizePgTableName(scope.table);
                if (
                  remoteName === scopeTable ||
                  (!scopeTable.includes(".") &&
                    remoteName.endsWith(`.${scopeTable}`))
                ) {
                  const targetBaseName = this._singularizedCodecName(
                    relation.remoteResource.codec,
                  );
                  const matchedColumns = new Set<string>();
                  const finalColumns = new Set<string>();
                  for (let i = 0; i < relation.localAttributes.length; i++) {
                    const attribute = relation.localAttributes[i];
                    if (!scope.columns.includes(attribute)) continue;
                    const attributeBaseName = this._getBaseName(attribute);
                    if (
                      attribute !== relation.remoteAttributes[i] &&
                      (!attributeBaseName ||
                        !this._baseNameMatches(
                          attributeBaseName,
                          targetBaseName,
                        ))
                    ) {
                      continue;
                    }
                    matchedColumns.add(attribute);
                    if (
                      relation.remoteAttributes[i] ===
                      getPrimaryKeyAttribute(relation.remoteResource.codec)
                    ) {
                      finalColumns.add(attribute);
                    }
                  }
                  if (matchedColumns.size) {
                    changed =
                      considerMatch(localName, {
                        columns: matchedColumns,
                        finalColumns,
                        scopeIndex,
                      }) || changed;
                  }
                }
              }
              const remoteMatch = columnsByCodec.get(remoteName);
              if (remoteMatch) {
                const matchedColumns = new Set<string>();
                const finalColumns = new Set<string>();
                for (let i = 0; i < relation.localAttributes.length; i++) {
                  const remoteAttribute = relation.remoteAttributes[i];
                  if (remoteMatch.columns.has(remoteAttribute)) {
                    const attribute = relation.localAttributes[i];
                    matchedColumns.add(attribute);
                    if (remoteMatch.finalColumns.has(remoteAttribute)) {
                      finalColumns.add(attribute);
                    }
                  }
                }
                if (matchedColumns.size) {
                  changed =
                    considerMatch(localName, {
                      columns: matchedColumns,
                      finalColumns,
                      scopeIndex: remoteMatch.scopeIndex,
                    }) || changed;
                }
              }
            }
          }
          for (const relation of relations) {
            const match = columnsByCodec.get(getName(relation.localCodec));
            if (match) {
              latestMultiTenancyColumnsByCodec.set(
                relation.localCodec,
                match.columns,
              );
            }
          }
          multiTenancyColumnsByRegistry.set(registry, columnsByCodec);
        }
        const pg = (codec.extensions as any)?.pg;
        const name = pg ? `${pg.schemaName}.${pg.name}` : codec.name;
        return columnsByCodec.get(name)?.columns ?? new Set();
      },
    },
    ignoreReplaceIfNotExists: ["deletedNodeId"],
    replace: {
      /*
       * This solves the issue with `blah-table1s` becoming `blahTable1S`
       * (i.e. the capital S at the end) or `table1-connection becoming `Table1SConnection`
       */
      camelCase: fixCapitalisedPlural,
      upperCamelCase: fixCapitalisedPlural,

      /*
       * Pluralize/singularize only supports single words, so only run
       * on the final segment of a name.
       */
      pluralize: fixChangePlural,
      singularize: fixChangePlural,

      // Fix a naming bug
      deletedNodeId(_prev, preset, { resource }) {
        return this.camelCase(
          `deleted-${this.singularize(
            this.tableType(resource.codec),
          )}-${this.nodeIdFieldName()}`,
        );
      },

      patchField(previous, options, fieldName) {
        return options.schema?.pgSimplifyPatch ? "patch" : previous!(fieldName);
      },

      connectionField(_prev, options, baseName) {
        return (globalPgOmitListSuffix ?? options.schema?.pgOmitListSuffix)
          ? baseName + "Connection"
          : baseName;
      },
      listField(_prev, options, baseName) {
        return (globalPgOmitListSuffix ?? options.schema?.pgOmitListSuffix)
          ? baseName
          : baseName + "List";
      },

      allRowsConnection(previous, options, resource) {
        const listSuffix = resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => {
          if (options.schema?.pgSimplifyAllRows) {
            return this.connectionField(
              this.camelCase(
                `${this.distinctPluralize(
                  this._singularizedResourceName(resource),
                )}`,
              ),
            );
          } else {
            return previous!(resource);
          }
        });
      },
      allRowsList(previous, options, resource) {
        const listSuffix = resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => {
          if (options.schema?.pgSimplifyAllRows) {
            return this.listField(
              this.camelCase(
                this.distinctPluralize(
                  this._singularizedResourceName(resource),
                ),
              ),
            );
          } else {
            return previous!(resource);
          }
        });
      },
      allInterfaceModeUnionRowsConnection(previous, options, codec) {
        const listSuffix = codec.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => {
          if (options.schema?.pgSimplifyAllRows) {
            return this.connectionField(
              this.camelCase(
                `${this.distinctPluralize(this._singularizedCodecName(codec))}`,
              ),
            );
          } else {
            return previous!(codec);
          }
        });
      },
      allInterfaceModeUnionRowsList(previous, options, codec) {
        const listSuffix = codec.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => {
          if (options.schema?.pgSimplifyAllRows) {
            return this.listField(
              this.camelCase(
                this.distinctPluralize(this._singularizedCodecName(codec)),
              ),
            );
          } else {
            return previous!(codec);
          }
        });
      },

      singleRelation(previous, options, details) {
        const { registry, codec, relationName } = details;
        const relation = registry.pgRelations[codec.name]?.[relationName];
        if (typeof relation.extensions?.tags?.fieldName === "string") {
          return relation.extensions.tags.fieldName;
        }
        let attributes = relation.localAttributes as string[];
        const tenancyColumns = this._pgMultiTenancyColumns(
          codec,
          registry,
          options.schema,
        );
        const primaryKey = (
          relation.remoteResource.uniques as PgResourceUnique[]
        ).find((unique) => unique.isPrimary);
        const primaryKeyAttribute = primaryKey?.attributes[0];
        const withoutTenancy = attributes.filter((attribute, index) => {
          return (
            !tenancyColumns.has(attribute) ||
            relation.remoteAttributes[index] === primaryKeyAttribute
          );
        });
        if (withoutTenancy.length) attributes = withoutTenancy;
        const detailedKeys = attributes.map((attributeName) => ({
          codec,
          attributeName,
        }));
        const baseName = this._getBaseNameFromKeys(detailedKeys);
        if (baseName) {
          return this.camelCase(baseName);
        }
        const foreignPk = (
          relation.remoteResource.uniques as PgResourceUnique[]
        ).find((u) => u.isPrimary);
        if (
          foreignPk &&
          arraysMatch(foreignPk.attributes, relation.localAttributes)
        ) {
          return this.camelCase(
            `${this._singularizedCodecName(relation.remoteResource.codec)}`,
          );
        }
        return previous!(details);
      },

      singleRelationBackwards(previous, options, details) {
        const { registry, codec, relationName } = details;
        const relation = registry.pgRelations[codec.name]?.[relationName];
        if (
          typeof relation.extensions?.tags?.foreignSingleFieldName === "string"
        ) {
          return relation.extensions.tags.foreignSingleFieldName;
        }
        if (typeof relation.extensions?.tags?.foreignFieldName === "string") {
          return relation.extensions.tags.foreignFieldName;
        }
        let attributes = relation.remoteAttributes as string[];
        const tenancyColumns = this._pgMultiTenancyColumns(
          relation.remoteResource.codec,
          registry,
          options.schema,
        );
        const withoutTenancy = attributes.filter(
          (attribute) => !tenancyColumns.has(attribute),
        );
        if (withoutTenancy.length) attributes = withoutTenancy;
        const detailedKeys = attributes.map((attributeName) => ({
          codec: relation.remoteResource.codec,
          attributeName,
        }));
        const baseName = this._getBaseNameFromKeys(detailedKeys);
        if (baseName) {
          const oppositeBaseName = this._getOppositeBaseName(baseName);
          if (oppositeBaseName) {
            return this.camelCase(
              `${oppositeBaseName}-${this._singularizedCodecName(
                relation.remoteResource.codec,
              )}`,
            );
          }
          if (this._baseNameMatches(baseName, codec.name)) {
            return this.camelCase(
              `${this._singularizedCodecName(relation.remoteResource.codec)}`,
            );
          }
        }
        const possibleResources = Object.values(registry.pgResources).filter(
          (resource) => resource.codec === codec && !resource.parameters,
        );
        if (possibleResources.length > 1) {
          throw new Error(
            `singleRelationBackwards inflector check failed: multiple table-like resources for codec '${codec.name}', so we cannot determine the primary key - please override this inflector for this table.`,
          );
        }
        const uniques = (possibleResources[0]?.uniques ??
          []) as PgResourceUnique[];
        const pk = uniques.find((u) => u.isPrimary);
        if (pk && arraysMatch(pk.attributes, relation.remoteAttributes)) {
          return this.camelCase(
            `${this._singularizedCodecName(relation.remoteResource.codec)}`,
          );
        }
        return previous!(details);
      },

      _manyRelation(previous, options, details) {
        const { registry, codec, relationName } = details;
        const relation = registry.pgRelations[codec.name]?.[relationName];
        const baseOverride = relation.extensions?.tags?.foreignFieldName;
        if (typeof baseOverride === "string") {
          return baseOverride;
        }
        let attributes = relation.remoteAttributes as string[];
        const tenancyColumns = this._pgMultiTenancyColumns(
          relation.remoteResource.codec,
          registry,
          options.schema,
        );
        const withoutTenancy = attributes.filter(
          (attribute) => !tenancyColumns.has(attribute),
        );
        const tenancyWasRemoved = withoutTenancy.length < attributes.length;
        if (withoutTenancy.length) attributes = withoutTenancy;
        const detailedKeys = attributes.map((attributeName) => ({
          codec: relation.remoteResource.codec,
          attributeName,
        }));
        const baseName = this._getBaseNameFromKeys(detailedKeys);
        if (baseName) {
          const oppositeBaseName = this._getOppositeBaseName(baseName);
          if (oppositeBaseName) {
            return this.camelCase(
              `${oppositeBaseName}-${this.distinctPluralize(
                this._singularizedCodecName(relation.remoteResource.codec),
              )}`,
            );
          }
          if (this._baseNameMatches(baseName, codec.name)) {
            return this.camelCase(
              `${this.distinctPluralize(
                this._singularizedCodecName(relation.remoteResource.codec),
              )}`,
            );
          }
        }
        if (tenancyWasRemoved) {
          return this.camelCase(
            this.distinctPluralize(
              this._singularizedCodecName(relation.remoteResource.codec),
            ),
          );
        }
        const pk = (relation.remoteResource.uniques as PgResourceUnique[]).find(
          (u) => u.isPrimary,
        );
        if (pk && arraysMatch(pk.attributes, relation.remoteAttributes)) {
          return this.camelCase(
            `${this.distinctPluralize(
              this._singularizedCodecName(relation.remoteResource.codec),
            )}`,
          );
        }
        return previous!(details);
      },

      manyRelationConnection(previous, _options, details) {
        const { registry, codec, relationName } = details;
        const relation = registry.pgRelations[codec.name]?.[relationName];
        const listSuffix =
          relation.extensions?.tags?.listSuffix ??
          relation.remoteResource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },

      manyRelationList(previous, _options, details) {
        const { registry, codec, relationName } = details;
        const relation = registry.pgRelations[codec.name]?.[relationName];
        const listSuffix =
          relation.extensions?.tags?.listSuffix ??
          relation.remoteResource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },
      customQueryConnectionField(previous, _options, details) {
        const listSuffix = details.resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },
      customQueryListField(previous, _options, details) {
        const listSuffix = details.resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },
      computedAttributeConnectionField(previous, _options, details) {
        const listSuffix = details.resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },
      computedAttributeListField(previous, _options, details) {
        const listSuffix = details.resource.extensions?.tags?.listSuffix;
        return overrideListSuffix(listSuffix, () => previous!(details));
      },

      nodeById(previous, options, typeName) {
        if (options.schema?.pgShortPk) {
          return this.camelCase(`${typeName}-by-${this.nodeIdFieldName()}`);
        } else {
          return previous!(typeName);
        }
      },
      rowByUnique(previous, options, details) {
        const { unique, resource } = details;
        if (typeof unique.extensions?.tags?.fieldName === "string") {
          return unique.extensions?.tags?.fieldName;
        }
        if (options.schema?.pgShortPk && unique.isPrimary) {
          // Primary key, shorten!
          return this.camelCase(this._singularizedCodecName(resource.codec));
        }
        if (!options.schema?.pgMultiTenancy?.length) {
          return previous!(details);
        }
        const tenancyColumns = this._pgMultiTenancyColumns(
          resource.codec,
          undefined,
          options.schema,
        );
        const remainingAttributes = unique.attributes.filter(
          (attribute) => !tenancyColumns.has(attribute),
        );
        if (
          remainingAttributes.length &&
          remainingAttributes.length < unique.attributes.length
        ) {
          return this.camelCase(
            `${this.tableType(resource.codec)}-by-${this._joinAttributeNames(
              resource.codec,
              remainingAttributes,
            )}`,
          );
        }
        return previous!(details);
      },

      updateByKeysField(previous, options, details) {
        const { resource, unique } = details;
        if (typeof unique.extensions?.tags?.updateFieldName === "string") {
          return unique.extensions.tags.updateFieldName;
        }
        if (options.schema?.pgShortPk && unique.isPrimary) {
          return this.camelCase(
            `update-${this._singularizedCodecName(resource.codec)}`,
          );
        }
        const tenancyColumns = this._pgMultiTenancyColumns(
          resource.codec,
          undefined,
          options.schema,
        );
        const attributes = unique.attributes.filter(
          (attribute) => !tenancyColumns.has(attribute),
        );
        if (attributes.length && attributes.length < unique.attributes.length) {
          return this.camelCase(
            `update-${this._singularizedResourceName(
              resource,
            )}-by-${this._joinAttributeNames(resource.codec, attributes)}`,
          );
        }
        return previous!(details);
      },
      deleteByKeysField(previous, options, details) {
        const { resource, unique } = details;
        if (typeof unique.extensions?.tags?.deleteFieldName === "string") {
          return unique.extensions.tags.deleteFieldName;
        }
        if (options.schema?.pgShortPk && unique.isPrimary) {
          // Primary key, shorten!
          return this.camelCase(
            `delete-${this._singularizedCodecName(resource.codec)}`,
          );
        }
        const tenancyColumns = this._pgMultiTenancyColumns(
          resource.codec,
          undefined,
          options.schema,
        );
        const attributes = unique.attributes.filter(
          (attribute) => !tenancyColumns.has(attribute),
        );
        if (attributes.length && attributes.length < unique.attributes.length) {
          return this.camelCase(
            `delete-${this._singularizedResourceName(
              resource,
            )}-by-${this._joinAttributeNames(resource.codec, attributes)}`,
          );
        }
        return previous!(details);
      },
      updateByKeysInputType(previous, options, details) {
        const { resource, unique } = details;
        if (unique.extensions?.tags?.updateFieldName) {
          return this.upperCamelCase(
            `${unique.extensions.tags.updateFieldName}-input`,
          );
        }
        if (options.schema?.pgShortPk && unique.isPrimary) {
          // Primary key, shorten!
          return this.upperCamelCase(
            `update-${this._singularizedCodecName(resource.codec)}-input`,
          );
        }
        const tenancyColumns = this._pgMultiTenancyColumns(
          resource.codec,
          undefined,
          options.schema,
        );
        if (
          unique.attributes.some((attribute) =>
            tenancyColumns.has(attribute),
          ) &&
          unique.attributes.some((attribute) => !tenancyColumns.has(attribute))
        ) {
          return this.upperCamelCase(
            `${this.updateByKeysField(details)}-input`,
          );
        }
        return previous!(details);
      },
      deleteByKeysInputType(previous, options, details) {
        const { resource, unique } = details;
        if (unique.extensions?.tags?.deleteFieldName) {
          return this.upperCamelCase(
            `${unique.extensions.tags.deleteFieldName}-input`,
          );
        }
        if (options.schema?.pgShortPk && unique.isPrimary) {
          // Primary key, shorten!
          return this.upperCamelCase(
            `delete-${this._singularizedCodecName(resource.codec)}-input`,
          );
        }
        const tenancyColumns = this._pgMultiTenancyColumns(
          resource.codec,
          undefined,
          options.schema,
        );
        if (
          unique.attributes.some((attribute) =>
            tenancyColumns.has(attribute),
          ) &&
          unique.attributes.some((attribute) => !tenancyColumns.has(attribute))
        ) {
          return this.upperCamelCase(
            `${this.deleteByKeysField(details)}-input`,
          );
        }
        return previous!(details);
      },
      updateNodeField(previous, options, details) {
        if (options.schema?.pgShortPk) {
          return this.camelCase(
            `update-${this._singularizedCodecName(
              details.resource.codec,
            )}-by-${this.nodeIdFieldName()}`,
          );
        } else {
          return previous!(details);
        }
      },
      deleteNodeField(previous, options, details) {
        if (options.schema?.pgShortPk) {
          return this.camelCase(
            `delete-${this._singularizedCodecName(
              details.resource.codec,
            )}-by-${this.nodeIdFieldName()}`,
          );
        } else {
          return previous!(details);
        }
      },
      updateNodeInputType(previous, options, details) {
        if (options.schema?.pgShortPk) {
          return this.upperCamelCase(
            `update-${this._singularizedCodecName(
              details.resource.codec,
            )}-by-${this.nodeIdFieldName()}-input`,
          );
        } else {
          return previous!(details);
        }
      },
      deleteNodeInputType(previous, options, details) {
        if (options.schema?.pgShortPk) {
          return this.upperCamelCase(
            `delete-${this._singularizedCodecName(
              details.resource.codec,
            )}-by-${this.nodeIdFieldName()}-input`,
          );
        } else {
          return previous!(details);
        }
      },
    },
  },
};

export const PgSimplifyInflectionPreset: GraphileConfig.Preset = {
  plugins: [PgSimplifyInflectionPlugin],
  schema: {
    pgSimplifyPatch: true,
    pgSimplifyAllRows: true,
    pgShortPk: true,
    pgSimplifyMultikeyRelations: true,
  },
};

export { PgSimplifyInflectionPlugin };
