import sql from "mssql";
import { logToFile, settings } from "./";

export let connection: sql.ConnectionPool | null = null;
let dolphinConnection = false; // keep if you still need the "DOLPHINDATA" idea
let config: any = null;

function withDbOverride(base: any, isDolphinData: boolean) {
  // In Snowflake you had a hard override to DOLPHINDATA.
  // In MSSQL, you can do the same by overriding the database name if you want.
  if (!isDolphinData) return base;
  return { ...base, database: "DOLPHINDATA" };
}

/**
 * Initializes an MSSQL connection pool.
 * Keeps the same signature you had for Snowflake.
 */
export async function initDbConnection(
  isDolphinData: boolean = false
): Promise<sql.ConnectionPool | null> {
  if (connection?.connected && dolphinConnection === isDolphinData) return connection;

  config = await settings.getMsSQLConfig();

  dolphinConnection = isDolphinData;

  if (!config) throw new Error("MSSQL config is missing");

  const poolConfig: sql.config = {
    server: config.server,
    database: config.database,
    user: config.user,
    password: config.password,
    port: config.port ?? 1433,
    options: {
      encrypt: Boolean(config.options?.encrypt),
      trustServerCertificate: Boolean(config.options?.trustServerCertificate),
    },
    pool: {
      max: 10,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };

  const finalConfig = withDbOverride(poolConfig, isDolphinData);

  try {
    connection = await new sql.ConnectionPool(finalConfig).connect();
    logToFile("mssql", `Connected to MSSQL: ${finalConfig.server}/${finalConfig.database}`);
    return connection;
  } catch (err: any) {
    logToFile("mssql", `Failed to connect to MSSQL: ${err?.message || err}`);
    connection = null;
    throw err;
  }
}

/**
 * Executes a SQL query on MSSQL.
 * Supports your existing `?` binds by converting them to @p1, @p2...
 */
export async function query(
  conn: sql.ConnectionPool | null,
  sqlText: string,
  binds: any[] = [],
  retry = true
): Promise<any[]> {
  if (!conn) return [];

  // Convert `?` placeholders to @p1, @p2, ...
  let idx = 0;
  const convertedSql = sqlText.replace(/\?/g, () => `@p${++idx}`);

  try {
    const req = conn.request();
    binds.forEach((val, i) => {
      req.input(`p${i + 1}`, val);
    });

    const result = await req.query(convertedSql);
    const rows = result.recordset || [];

    if (/OUTPUT\s+INSERTED\./i.test(convertedSql) && rows.length === 0) {
      throw new Error("Query used OUTPUT INSERTED but returned no rows (check identity column / OUTPUT clause).");
    }

    logToFile("mssql", `Query executed successfully: ${convertedSql}`);

    return rows;
  } catch (err: any) {
    const msg = err?.message || String(err);
    logToFile("mssql", `Error executing query: ${msg}`);

    // Basic retry on common connection-ish issues
    const looksLikeConnIssue =
      /ConnectionError|closed|socket|ECONNRESET|ETIMEOUT|ELOGIN/i.test(msg);

    if (retry && looksLikeConnIssue) {
      logToFile("mssql", "MSSQL connection issue detected. Reconnecting and retrying...");
      try {
        connection?.close?.();
      } catch { }
      connection = null;

      const newConn = await initDbConnection(dolphinConnection);
      return query(newConn, sqlText, binds, false);
    }

    throw err;
  }
}
