import { Pool, PoolClient } from 'pg';
import { getDbConfig } from './config.js';
import logger from '../utils/logger.js';

let pool: Pool | null = null;

function toLogMetadata(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }

  return {
    error,
  };
}

/**
 * Initialize the PostgreSQL connection pool
 */
export function initializePool(): Pool {
  if (pool) {
    return pool;
  }

  const config = getDbConfig();

  pool = new Pool({
    user: config.user,
    password: config.password,
    host: config.host,
    port: config.port,
    database: config.database,
    max: config.maxConnections,
    idleTimeoutMillis: config.idleTimeoutMillis,
    connectionTimeoutMillis: config.connectionTimeoutMillis,
  });

  pool.on('error', (err) => {
    logger.error('Unexpected pool error', toLogMetadata(err));
  });

  pool.on('connect', () => {
    logger.debug('New database connection established');
  });

  logger.info(`PostgreSQL connection pool initialized (max: ${config.maxConnections} connections)`);

  return pool;
}

/**
 * Get the connection pool instance
 */
export function getPool(): Pool {
  if (!pool) {
    throw new Error('Pool not initialized. Call initializePool() first.');
  }
  return pool;
}

/**
 * Execute a query on the pool
 */
export async function query<T = any>(
  text: string,
  values?: any[]
): Promise<{ rows: T[]; rowCount: number }> {
  const result = await getPool().query(text, values);
  return {
    rows: result.rows as T[],
    rowCount: result.rowCount || 0,
  };
}

/**
 * Get a client from the pool for transactions
 */
export async function getClient(): Promise<PoolClient> {
  return getPool().connect();
}

/**
 * Execute a transaction
 */
export async function transaction<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await getClient();

  try {
    await client.query('BEGIN');
    const result = await callback(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Close the connection pool
 */
export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
    logger.info('PostgreSQL connection pool closed');
  }
}

/**
 * Health check for the database connection
 */
export async function healthCheck(): Promise<boolean> {
  try {
    await getPool().query('SELECT NOW()');
    return true;
  } catch (error) {
    logger.error('Database health check failed', toLogMetadata(error));
    return false;
  }
}
