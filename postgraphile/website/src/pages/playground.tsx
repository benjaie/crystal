import BrowserOnly from "@docusaurus/BrowserOnly";
import Layout from "@theme/Layout";
import clsx from "clsx";
import React, { useEffect, useMemo, useState } from "react";

import {
  buildPlaygroundEngine,
  type PlaygroundEngine,
} from "../playground/engine";
import type { PlaygroundFile } from "../playground/presetEvaluator";
import styles from "./playground.module.css";

const starterSchema = `-- schema.sql
create schema if not exists app_public;

create table if not exists app_public.users (
  id serial primary key,
  username text not null unique,
  full_name text not null,
  created_at timestamptz not null default now()
);

insert into app_public.users (username, full_name)
values
  ('ada', 'Ada Lovelace'),
  ('grace', 'Grace Hopper'),
  ('margaret', 'Margaret Hamilton'),
  ('anil', 'Anil Dash')
on conflict do nothing;`;

const starterPreset = `// graphile.preset.ts
import { PostGraphileAmberPreset } from "postgraphile/presets/amber";
import { makePgLiteService } from "@dataplan/pg/adaptors/pglite";

export default {
  extends: [PostGraphileAmberPreset],
  pgServices: [
    makePgLiteService({
      schemas: ["app_public"],
      // Read per-request details from requestContext (set in the runner form)
      pgSettings: (ctx) => ({
        role: ctx.role ?? "app_server",
      }),
    }),
  ],
};`;

const starterQuery = `query ExampleUsers {
  allUsersList {
    id
    username
    fullName
    createdAt
  }
}`;

const starterRequestContext = `{
  "role": "app_server"
}`;

const starterFiles: PlaygroundFile[] = [
  { name: "graphile.preset.ts", content: starterPreset, language: "ts" },
  { name: "schema.sql", content: starterSchema, language: "sql" },
];

function PlaygroundApp() {
  const [files, setFiles] = useState<PlaygroundFile[]>(starterFiles);
  const [activeFile, setActiveFile] = useState<string>(starterFiles[0].name);
  const [engine, setEngine] = useState<PlaygroundEngine | null>(null);
  const [schemaSDL, setSchemaSDL] = useState<string>("");
  const [query, setQuery] = useState<string>(starterQuery);
  const [variables, setVariables] = useState<string>("{}");
  const [requestContext, setRequestContext] =
    useState<string>(starterRequestContext);
  const [operationName, setOperationName] = useState<string>("");
  const [result, setResult] = useState<string>('Hit "Run query" to see results');
  const [status, setStatus] = useState<string>(
    "Load your schema and preset, then build.",
  );
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<boolean>(false);

  const active = useMemo(
    () => files.find((f) => f.name === activeFile) ?? files[0],
    [files, activeFile],
  );

  useEffect(() => {
    return () => {
      if (engine) {
        engine.shutdown().catch(() => {});
      }
    };
  }, [engine]);

  const updateFile = (name: string, content: string) => {
    setFiles((prev) =>
      prev.map((file) => (file.name === name ? { ...file, content } : file)),
    );
  };

  const addFile = () => {
    const name = window.prompt("New file name (e.g. example.sql)")?.trim();
    if (!name) return;
    if (files.find((f) => f.name === name)) {
      window.alert("A file with that name already exists.");
      return;
    }
    setFiles((prev) => [...prev, { name, content: "", language: "md" }]);
    setActiveFile(name);
  };

  const resetFiles = () => {
    setFiles(starterFiles);
    setActiveFile(starterFiles[0].name);
    setQuery(starterQuery);
    setRequestContext(starterRequestContext);
    setVariables("{}");
    setResult('Hit "Run query" to see results');
    setSchemaSDL("");
    setStatus("Starter kit loaded. Build the API to begin.");
    setError(null);
    if (engine) {
      engine.shutdown().catch(() => {});
      setEngine(null);
    }
  };

  const build = async () => {
    setBusy(true);
    setError(null);
    setStatus("Assembling PGLite and PostGraphile…");
    if (engine) {
      await engine.shutdown();
      setEngine(null);
    }
    try {
      const nextEngine = await buildPlaygroundEngine({
        files,
      });
      setEngine(nextEngine);
      setSchemaSDL(nextEngine.schemaSDL);
      setStatus("API ready. Run a query!");
    } catch (e: any) {
      setError(e?.message ?? String(e));
      setStatus("Build failed");
    } finally {
      setBusy(false);
    }
  };

  const runQuery = async () => {
    if (!engine) {
      setError("Build the API before running a query.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const parsedVars = variables?.trim()
        ? JSON.parse(variables)
        : Object.create(null);
      const parsedCtx = requestContext?.trim()
        ? JSON.parse(requestContext)
        : Object.create(null);
      const payload = await engine.execute({
        query,
        variables: parsedVars,
        operationName: operationName || undefined,
        requestContext: parsedCtx,
      });
      setResult(JSON.stringify(payload, null, 2));
      setStatus("Query complete");
    } catch (e: any) {
      setError(e?.message ?? String(e));
      setStatus("Query failed");
    } finally {
      setBusy(false);
    }
  };

  const hint = (
    <div className={styles.tagList}>
      <span className={styles.tag}>PGLite runs in-memory</span>
      <span className={styles.tag}>No backend server</span>
      <span className={styles.tag}>Hot-edit graphile.preset.ts</span>
    </div>
  );

  return (
    <div className={styles.container}>
      <div className={styles.content}>
        <div className={styles.hero}>
          <div className={clsx(styles.titleBlock, styles.panel)}>
            <h1>PostGraphile Playground</h1>
            <p>
              Build, seed, and run PostGraphile entirely in your browser. PGLite
              powers the database; the preset and schema are just files you can
              edit live.
            </p>
            <div className={styles.badgeRow}>
              <span className={styles.badge}>PGLite</span>
              <span className={styles.badge}>PostGraphile v5</span>
              <span className={styles.badge}>
                Gra<em>fast</em> executor
              </span>
            </div>
          </div>
          <div className={clsx(styles.panel, styles.stack)}>
            <div className={styles.statusRow}>
              <span className={styles.status}>{status}</span>
              {hint}
            </div>
            <div className={styles.buttonRow}>
              <button
                className={styles.button}
                onClick={build}
                disabled={busy}
              >
                Build API
              </button>
              <button
                className={clsx(styles.button, styles.buttonSecondary)}
                onClick={runQuery}
                disabled={busy}
              >
                Run query
              </button>
              <button
                className={clsx(styles.button, styles.buttonSecondary)}
                onClick={resetFiles}
                disabled={busy}
              >
                Reset
              </button>
              <button
                className={clsx(styles.button, styles.buttonSecondary)}
                onClick={addFile}
                disabled={busy}
              >
                Add file
              </button>
            </div>
            {error ? <div className={styles.error}>{error}</div> : null}
          </div>
        </div>

        <div className={styles.grid}>
          <div className={styles.panel}>
            <div className={styles.editorHeader}>
              <div className={styles.fileTabs}>
                {files.map((file) => (
                  <button
                    key={file.name}
                    className={clsx(
                      styles.tab,
                      active?.name === file.name && styles.tabActive,
                    )}
                    onClick={() => setActiveFile(file.name)}
                  >
                    {file.name}
                  </button>
                ))}
              </div>
              <span className={styles.label}>
                Editing {active?.name ?? "files"}
              </span>
            </div>
            <textarea
              key={active?.name}
              className={styles.textarea}
              value={active?.content ?? ""}
              onChange={(e) => updateFile(active?.name ?? "", e.target.value)}
              spellCheck={false}
            />
          </div>

          <div className={styles.panel}>
            <div className={styles.stack}>
              <label className={styles.label} htmlFor="query">
                GraphQL operation
              </label>
              <textarea
                id="query"
                className={styles.textarea}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                spellCheck={false}
              />
              <div className={styles.stack}>
                <label className={styles.label} htmlFor="vars">
                  Variables (JSON)
                </label>
                <textarea
                  id="vars"
                  className={clsx(styles.textarea, styles.smallTextarea)}
                  value={variables}
                  onChange={(e) => setVariables(e.target.value)}
                  spellCheck={false}
                />
              </div>
              <div className={styles.stack}>
                <label className={styles.label} htmlFor="ctx">
                  requestContext (JSON) — passed to pgSettings
                </label>
                <textarea
                  id="ctx"
                  className={clsx(styles.textarea, styles.smallTextarea)}
                  value={requestContext}
                  onChange={(e) => setRequestContext(e.target.value)}
                  spellCheck={false}
                />
              </div>
              <div className={styles.stack}>
                <label className={styles.label} htmlFor="operationName">
                  Operation name (optional)
                </label>
                <input
                  id="operationName"
                  className={styles.textInput}
                  value={operationName}
                  onChange={(e) => setOperationName(e.target.value)}
                  spellCheck={false}
                />
              </div>
            </div>
          </div>
        </div>

        <div className={styles.grid}>
          <div className={styles.panel}>
            <div className={styles.label}>GraphQL response</div>
            <pre className={styles.output}>{result}</pre>
          </div>
          <div className={styles.panel}>
            <div className={styles.label}>Live schema (SDL)</div>
            <pre className={clsx(styles.output, styles.schemaBox)}>
              {schemaSDL || "Build the API to see the schema."}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

export default function PlaygroundPage() {
  return (
    <Layout
      title="Playground"
      description="Run PostGraphile with PGLite in your browser"
    >
      <BrowserOnly fallback={<div className={styles.container}>Loading…</div>}>
        {() => <PlaygroundApp />}
      </BrowserOnly>
    </Layout>
  );
}
