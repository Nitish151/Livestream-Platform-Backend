import { initializePool, closePool } from '../src/db/client.js';
import { runMigrations, getMigrationStatus } from '../src/db/migrate.js';

const isStatus = process.argv.includes('--status');

try {
  initializePool();

  if (isStatus) {
    await getMigrationStatus();
  } else {
    await runMigrations();
  }
} catch (error) {
  console.error('Migration runner failed:', error);
  process.exit(1);
} finally {
  await closePool();
}
