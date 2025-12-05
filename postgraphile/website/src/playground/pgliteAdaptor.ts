import type {
  MakePgServiceOptions,
  PgAdaptor,
  PgClient,
  PgClientQuery,
  PgClientResult,
  WithPgClient,
} from "@dataplan/pg";
import type { PGliteInterface, Transaction } from "@electric-sql/pglite";

type Runner = PGliteInterface | Transaction;

class PglitePgClient implements PgClient {
  constructor(private readonly runner: Runner) {}

  async query<TData>(opts: PgClientQuery): Promise<PgClientResult<TData>> {
    const { text, values, arrayMode } = opts;
    const result = await this.runner.query<TData>(text, values ?? []);
    const baseRows = result.rows ?? [];
    const rows =
      arrayMode && result.fields?.length
        ? baseRows.map((row: any) =>
            result.fields!.map((field) => (row as any)[field.name]),
          )
        : baseRows;
    const rowCount =
      result.affectedRows != null
        ? result.affectedRows
        : Array.isArray(baseRows)
          ? baseRows.length
          : null;
    return {
      rows,
      rowCount,
      notices: [],
    };
  }

  async withTransaction<T>(
    callback: (client: this) => Promise<T>,
  ): Promise<T> {
    if ("rollback" in this.runner) {
      return callback(this);
    }
    return this.runner.transaction(async (tx) => {
      const client = new PglitePgClient(tx) as this;
      return callback(client);
    });
  }
}

async function applyPgSettings(
  runner: Runner,
  pgSettings: Record<string, string | undefined> | null,
) {
  if (!pgSettings) return;
  const entries: Array<[string, string]> = [];
  for (const [key, value] of Object.entries(pgSettings)) {
    if (value == null) continue;
    entries.push([key, String(value)]);
  }
  if (entries.length === 0) return;
  await runner.query(
    "select set_config(el->>0, el->>1, true) from json_array_elements($1::json) el",
    [JSON.stringify(entries)],
  );
}

let defaultDb: PGliteInterface | null = null;

export function setDefaultPgLiteDatabase(db: PGliteInterface | null) {
  defaultDb = db;
}

export interface PgLiteAdaptorSettings {
  db: PGliteInterface;
}

export interface PgLiteServiceOptions
  extends MakePgServiceOptions,
    Partial<PgLiteAdaptorSettings> {
  schemas?: string | string[];
}

declare global {
  namespace GraphileConfig {
    interface PgAdaptors {
      "@dataplan/pg/adaptors/pglite": {
        adaptorSettings: PgLiteAdaptorSettings;
        makePgServiceOptions: PgLiteServiceOptions;
        client: PglitePgClient;
      };
    }
  }
}

export const pgliteAdaptor: PgAdaptor<"@dataplan/pg/adaptors/pglite"> = {
  async createWithPgClient(adaptorSettings) {
    const db = adaptorSettings.db;
    await db.waitReady;
    const runExclusive =
      typeof db.runExclusive === "function"
        ? db.runExclusive.bind(db)
        : async (fn: () => Promise<any>) => fn();

    const withPgClient: WithPgClient<PglitePgClient> = async (
      pgSettings,
      callback,
    ) => {
      return runExclusive(async () => {
        const applySettingsInsideTransaction =
          pgSettings && Object.keys(pgSettings).length > 0;
        if (applySettingsInsideTransaction) {
          return db.transaction(async (tx) => {
            await applyPgSettings(tx, pgSettings);
            return callback(new PglitePgClient(tx));
          });
        }
        return callback(new PglitePgClient(db));
      });
    };

    withPgClient.release = async () => {
      /* no-op */
    };

    return withPgClient;
  },
  makePgService(options) {
    const {
      name = "main",
      schemas,
      withPgClientKey = name === "main" ? "withPgClient" : `${name}_withPgClient`,
      pgSettings,
      pgSettingsKey =
        pgSettings != null
          ? name === "main"
            ? "pgSettings"
            : `${name}_pgSettings`
          : undefined,
      pgSettingsForIntrospection,
    } = options;
    const db = options.db ?? defaultDb;
    if (!db) {
      throw new Error(
        "No PGLite database found. Pass `db` to makePgLiteService or setDefaultPgLiteDatabase.",
      );
    }
    if (pgSettings && !pgSettingsKey) {
      throw new Error(
        `makePgLiteService called with pgSettings but no pgSettingsKey`,
      );
    }
    return {
      name,
      schemas: Array.isArray(schemas) ? schemas : [schemas ?? "public"],
      withPgClientKey: withPgClientKey as any,
      pgSettings,
      pgSettingsKey: pgSettingsKey as any,
      pgSettingsForIntrospection,
      pgSubscriber: null,
      adaptor: pgliteAdaptor,
      adaptorSettings: { db },
      async release() {
        if (!db.closed) {
          await db.close();
        }
      },
    };
  },
};

export function makePgLiteService(
  options: PgLiteServiceOptions,
): GraphileConfig.PgServiceConfiguration<"@dataplan/pg/adaptors/pglite"> {
  return pgliteAdaptor.makePgService(options);
}
import type {} from "graphile-config";
