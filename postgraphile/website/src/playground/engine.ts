import type {} from "graphile-config";

import { PGlite } from "@electric-sql/pglite";
import { grafast, isAsyncIterable } from "grafast";
import { printSchema, type GraphQLSchema } from "graphql";
import sql from "pg-sql2";
import postgraphile from "postgraphile";
import { PostGraphileAmberPreset } from "postgraphile/presets/amber";

import {
  makePgLiteService,
  setDefaultPgLiteDatabase,
} from "./pgliteAdaptor";
import type { PlaygroundFile } from "./presetEvaluator.js";
import { mapFiles, preparePreset } from "./presetEvaluator";

type PgliteArtifacts = {
  fsBundle: Blob;
  wasmModule: WebAssembly.Module;
};

let cachedArtifacts: Promise<PgliteArtifacts> | null = null;

async function loadPgliteArtifacts(): Promise<PgliteArtifacts> {
  if (!cachedArtifacts) {
    cachedArtifacts = (async () => {
      const dataUrl = new URL(
        "@electric-sql/pglite/dist/pglite.data",
        import.meta.url,
      ).toString();
      const wasmUrl = new URL(
        "@electric-sql/pglite/dist/pglite.wasm",
        import.meta.url,
      ).toString();

      const [fsBundle, wasmModule] = await Promise.all([
        fetch(dataUrl).then(async (res) => {
          if (!res.ok) {
            throw new Error(
              `Failed to fetch pglite.data (${res.status} ${res.statusText})`,
            );
          }
          return res.blob();
        }),
        fetch(wasmUrl).then(async (res) => {
          if (!res.ok) {
            throw new Error(
              `Failed to fetch pglite.wasm (${res.status} ${res.statusText})`,
            );
          }
          const buffer = await res.arrayBuffer();
          return WebAssembly.compile(buffer);
        }),
      ]);

      return { fsBundle, wasmModule };
    })();
  }
  return cachedArtifacts;
}

export interface PlaygroundEngine {
  schema: GraphQLSchema;
  resolvedPreset: GraphileConfig.ResolvedPreset;
  schemaSDL: string;
  execute: (options: {
    query: string;
    variables?: Record<string, any>;
    operationName?: string;
    requestContext?: Record<string, any>;
  }) => Promise<any>;
  shutdown: () => Promise<void>;
}

export interface BuildEngineOptions {
  files: PlaygroundFile[];
  defaultSchemas?: string[];
}

export async function buildPlaygroundEngine({
  files,
  defaultSchemas = ["app_public"],
}: BuildEngineOptions): Promise<PlaygroundEngine> {
  const fileMap = mapFiles(files);
  const schemaSql = fileMap["schema.sql"]?.content ?? "";
  if (!schemaSql.trim()) {
    throw new Error("schema.sql is empty, add a schema and try again.");
  }

  const { fsBundle, wasmModule } = await loadPgliteArtifacts();
  const db = new PGlite({
    dataDir: "memory://postgraphile-playground",
    fsBundle,
    wasmModule,
  });
  await db.waitReady;
  await db.exec(schemaSql);
  if (defaultSchemas.length > 0) {
    const searchPath = [...defaultSchemas, "public"]
      .map((schema) => `"${schema.replace(/"/g, '""')}"`)
      .join(", ");
    await db.exec(`set search_path to ${searchPath};`);
  }

  setDefaultPgLiteDatabase(db);
  const defaultService = makePgLiteService({
    db,
    schemas: defaultSchemas,
  });

  const presetModules = {
    postgraphile: Object.assign(postgraphile, { default: postgraphile }),
    "postgraphile/presets/amber": {
      PostGraphileAmberPreset,
      default: PostGraphileAmberPreset,
    },
    "@dataplan/pg/adaptors/pglite": {
      makePgLiteService,
      default: makePgLiteService,
    },
    "pg-sql2": Object.assign(sql, { default: sql }),
  };

  let preset: GraphileConfig.Preset | null = null;
  try {
    try {
      preset = await preparePreset({
        files,
        presetModules,
        context: {
          db,
          makePgLiteService,
        },
        defaultService,
      });
    } finally {
      setDefaultPgLiteDatabase(null);
    }

    const pgInstance = postgraphile(preset!);
    const schemaResult = await pgInstance.getSchemaResult();
    const { schema, resolvedPreset } = schemaResult;
    const schemaSDL = printSchema(schema);

    const execute = async (options: {
      query: string;
      variables?: Record<string, any>;
      operationName?: string;
      requestContext?: Record<string, any>;
    }) => {
      const result = await grafast({
        schema,
        source: options.query,
        variableValues: options.variables,
        operationName: options.operationName,
        resolvedPreset,
        contextValue: {},
        requestContext: options.requestContext ?? {},
      });

      if (isAsyncIterable(result)) {
        const payloads = [];
        for await (const chunk of result as AsyncIterable<any>) {
          payloads.push(chunk);
        }
        return payloads;
      }
      return result;
    };

    const shutdown = async () => {
      try {
        await pgInstance.release();
      } finally {
        await db.close();
      }
    };

    return {
      schema,
      resolvedPreset,
      schemaSDL,
      execute,
      shutdown,
    };
  } catch (e) {
    await db.close();
    throw e;
  }
}
