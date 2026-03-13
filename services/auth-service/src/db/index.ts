export {
  initializePool,
  getPool,
  query,
  getClient,
  transaction,
  closePool,
  healthCheck,
} from './client.js';

export { getDbConfig } from './config.js';
export { runMigrations, getMigrationStatus } from './migrate.js';

export type { DbConfig } from './config.js';
