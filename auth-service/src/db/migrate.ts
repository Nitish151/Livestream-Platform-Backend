import { readdir } from 'fs/promises';
import { join } from 'path';
import { fileURLToPath } from 'url';
import { getPool, query } from './client.js';
import logger from '../utils/logger.js';

const MIGRATIONS_DIR = join(fileURLToPath(new URL('migrations', import.meta.url)));

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

interface Migration {
  name: string;
  version: string;
  appliedAt?: Date;
}

/**
 * Initialize the migrations table if it doesn't exist
 */
async function initMigrationsTable(): Promise<void> {
  await query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      id SERIAL PRIMARY KEY,
      version VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL,
      applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
}

/**
 * Get already applied migrations
 */
async function getAppliedMigrations(): Promise<Map<string, Migration>> {
  const result = await query<Migration>(
    'SELECT version, name, applied_at as "appliedAt" FROM schema_migrations ORDER BY version'
  );

  const migrations = new Map<string, Migration>();
  result.rows.forEach((row) => {
    migrations.set(row.version, row);
  });

  return migrations;
}

/**
 * Read all migration files from the migrations directory
 */
async function readMigrationFiles(): Promise<string[]> {
  try {
    const files = await readdir(MIGRATIONS_DIR);
    return files
      .filter((file) => file.endsWith('.sql'))
      .sort();
  } catch (error) {
    logger.warn(`Migrations directory not found at ${MIGRATIONS_DIR}`);
    return [];
  }
}

/**
 * Parse migration filename to extract version and name
 * Expected format: 001_users.sql
 */
function parseMigrationFile(filename: string): { version: string; name: string } | null {
  const match = filename.match(/^(\d+)_(.+)\.sql$/);
  if (!match) {
    return null;
  }

  return {
    version: match[1],
    name: match[2].replace(/_/g, ' '),
  };
}

/**
 * Execute a migration file
 */
async function executeMigration(filename: string): Promise<void> {
  const { readFile } = await import('fs/promises');
  const filepath = join(MIGRATIONS_DIR, filename);

  try {
    const sql = await readFile(filepath, 'utf-8');
    await getPool().query(sql);
    logger.info(`✓ Executed migration: ${filename}`);
  } catch (error) {
    logger.error(`✗ Failed to execute migration ${filename}`, toLogMetadata(error));
    throw error;
  }
}

/**
 * Record a migration as applied
 */
async function recordMigration(version: string, name: string): Promise<void> {
  await query(
    'INSERT INTO schema_migrations (version, name) VALUES ($1, $2)',
    [version, name]
  );
}

/**
 * Run all pending migrations
 */
export async function runMigrations(): Promise<void> {
  logger.info('Starting database migrations...');

  try {
    await initMigrationsTable();
    const appliedMigrations = await getAppliedMigrations();
    const migrationFiles = await readMigrationFiles();

    let executedCount = 0;

    for (const filename of migrationFiles) {
      const parsed = parseMigrationFile(filename);
      if (!parsed) {
        logger.warn(`Skipping invalid migration filename: ${filename}`);
        continue;
      }

      if (appliedMigrations.has(parsed.version)) {
        logger.debug(`Migration already applied: ${filename}`);
        continue;
      }

      await executeMigration(filename);
      await recordMigration(parsed.version, parsed.name);
      executedCount++;
    }

    if (executedCount === 0) {
      logger.info('No pending migrations to run');
    } else {
      logger.info(`✓ Successfully executed ${executedCount} migration(s)`);
    }
  } catch (error) {
    logger.error('Migration failed', toLogMetadata(error));
    throw error;
  }
}

/**
 * Get migration status
 */
export async function getMigrationStatus(): Promise<void> {
  try {
    await initMigrationsTable();
    const appliedMigrations = await getAppliedMigrations();
    const migrationFiles = await readMigrationFiles();

    logger.info(`\n=== Database Migration Status ===`);
    logger.info(`Total migration files: ${migrationFiles.length}`);
    logger.info(`Applied migrations: ${appliedMigrations.size}\n`);

    migrationFiles.forEach((filename) => {
      const parsed = parseMigrationFile(filename);
      if (!parsed) return;

      const isApplied = appliedMigrations.has(parsed.version);
      const status = isApplied ? '✓' : '✗';
      const appliedAt = appliedMigrations.get(parsed.version)?.appliedAt;
      const timestamp = appliedAt ? new Date(appliedAt).toISOString() : 'pending';

      logger.info(`${status} ${filename} (${timestamp})`);
    });
  } catch (error) {
    logger.error('Failed to get migration status', toLogMetadata(error));
    throw error;
  }
}
