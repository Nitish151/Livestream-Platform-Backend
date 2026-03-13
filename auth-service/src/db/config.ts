import dotenv from 'dotenv';

dotenv.config();

export interface DbConfig {
  user: string;
  password: string;
  host: string;
  port: number;
  database: string;
  maxConnections: number;
  idleTimeoutMillis: number;
  connectionTimeoutMillis: number;
}

/**
 * Get database configuration from environment variables
 */
export function getDbConfig(): DbConfig {
  const {
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_MAX_CONNECTIONS,
    DB_IDLE_TIMEOUT,
    DB_CONNECTION_TIMEOUT,
  } = process.env;

  // Validate required environment variables
  if (!DB_USER) throw new Error('DB_USER environment variable is not set');
  if (!DB_PASSWORD) throw new Error('DB_PASSWORD environment variable is not set');
  if (!DB_HOST) throw new Error('DB_HOST environment variable is not set');
  if (!DB_PORT) throw new Error('DB_PORT environment variable is not set');
  if (!DB_NAME) throw new Error('DB_NAME environment variable is not set');

  return {
    user: DB_USER,
    password: DB_PASSWORD,
    host: DB_HOST,
    port: parseInt(DB_PORT, 10),
    database: DB_NAME,
    maxConnections: parseInt(DB_MAX_CONNECTIONS || '20', 10),
    idleTimeoutMillis: parseInt(DB_IDLE_TIMEOUT || '30000', 10),
    connectionTimeoutMillis: parseInt(DB_CONNECTION_TIMEOUT || '10000', 10),
  };
}
