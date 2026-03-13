import { createClient, RedisClientType } from 'redis';
import logger from '../utils/logger.js';

let client: RedisClientType | null = null;

/**
 * Initialize the Redis client and connect
 */
export async function initializeRedis(): Promise<RedisClientType> {
  if (client) {
    return client;
  }

  const url = process.env.REDIS_URL || 'redis://localhost:6379';

  client = createClient({ url });

  client.on('error', (err) => {
    logger.error('Redis client error', {
      message: err instanceof Error ? err.message : String(err),
    });
  });

  client.on('connect', () => {
    logger.debug('Redis connection established');
  });

  await client.connect();
  logger.info(`Redis client connected (${url})`);

  return client;
}

/**
 * Get the Redis client instance
 */
export function getRedisClient(): RedisClientType {
  if (!client) {
    throw new Error('Redis not initialized. Call initializeRedis() first.');
  }
  return client;
}

/**
 * Close the Redis connection
 */
export async function closeRedis(): Promise<void> {
  if (client) {
    await client.quit();
    client = null;
    logger.info('Redis client disconnected');
  }
}
