import type {} from "graphile-config";

import { transform } from "sucrase";

import type { PgLiteServiceOptions } from "./pgliteAdaptor";

type FileMap = Record<string, { name: string; content: string }>;

export interface EvaluatePresetOptions {
  files: FileMap;
  modules: Record<string, any>;
  context?: Record<string, any>;
}

function normalisePath(specifier: string) {
  const parts = specifier.split("/").filter(Boolean);
  const stack: string[] = [];
  for (const part of parts) {
    if (part === ".") continue;
    if (part === "..") {
      stack.pop();
    } else {
      stack.push(part);
    }
  }
  return stack.join("/");
}

function resolveRelative(
  specifier: string,
  files: FileMap,
): string | Record<string, unknown> {
  const searchOrder = [
    normalisePath(specifier.replace(/^\.\//, "")),
    `${normalisePath(specifier.replace(/^\.\//, ""))}.ts`,
    `${normalisePath(specifier.replace(/^\.\//, ""))}.js`,
  ];
  for (const key of searchOrder) {
    const file = files[key];
    if (file) {
      if (key.endsWith(".json")) {
        return JSON.parse(file.content);
      }
      return file.content;
    }
  }
  throw new Error(`Cannot resolve '${specifier}' from graphile.preset.ts`);
}

export async function evaluatePresetModule(
  code: string,
  options: EvaluatePresetOptions,
): Promise<GraphileConfig.Preset> {
  const { modules, context, files } = options;
  const compiled = transform(code, {
    transforms: ["typescript", "imports"],
    production: true,
  });

  const sandbox: Record<string, any> = {};
  const module = { exports: sandbox };

  const require = (specifier: string) => {
    if (modules[specifier]) {
      return modules[specifier];
    }
    if (specifier.startsWith(".")) {
      return resolveRelative(specifier, files);
    }
    throw new Error(
      `The playground does not recognise '${specifier}'. Add it to the preset modules map.`,
    );
  };

  const fn = new Function(
    "require",
    "module",
    "exports",
    "context",
    compiled.code,
  );

  fn(require, module, sandbox, context ?? {});

  const exported =
    (module.exports as any).default ?? module.exports ?? sandbox ?? {};

  const presetCandidate =
    typeof exported === "function" ? await exported(context ?? {}) : exported;

  if (!presetCandidate || typeof presetCandidate !== "object") {
    throw new Error(
      "Your graphile.preset.ts must export a preset object or a function that returns one.",
    );
  }

  return presetCandidate as GraphileConfig.Preset;
}

export function coercePreset(
  preset: GraphileConfig.Preset,
  defaultService: GraphileConfig.PgServiceConfiguration<"@dataplan/pg/adaptors/pglite">,
): GraphileConfig.Preset {
  const pgServices = preset.pgServices?.length
    ? preset.pgServices
    : [defaultService];
  return {
    ...preset,
    pgServices,
  };
}

export type PlaygroundFile = {
  name: string;
  language: "ts" | "sql" | "graphql" | "json" | "md";
  content: string;
};

export function mapFiles(files: PlaygroundFile[]): FileMap {
  const fileMap: FileMap = {};
  for (const file of files) {
    fileMap[file.name] = { name: file.name, content: file.content };
  }
  return fileMap;
}

export interface EngineBuildInput {
  files: PlaygroundFile[];
  presetModules: EvaluatePresetOptions["modules"];
  context?: Record<string, any>;
  defaultService: GraphileConfig.PgServiceConfiguration<"@dataplan/pg/adaptors/pglite">;
}

export async function preparePreset({
  files,
  presetModules,
  context,
  defaultService,
}: EngineBuildInput): Promise<GraphileConfig.Preset> {
  const fileMap = mapFiles(files);
  const presetSource = fileMap["graphile.preset.ts"]?.content ?? "";
  const evaluatedPreset = await evaluatePresetModule(presetSource, {
    files: fileMap,
    modules: presetModules,
    context,
  });

  return coercePreset(evaluatedPreset, defaultService);
}

export type PgLiteServiceFactory = (
  options: PgLiteServiceOptions,
) => GraphileConfig.PgServiceConfiguration<"@dataplan/pg/adaptors/pglite">;
