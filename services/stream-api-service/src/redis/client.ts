import { Redis } from 'ioredis';
import logger from '../utils/logger.js';

let client: Redis | null = null;

export function initializeRedis(): Redis {
  if (client) return client;

  const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
  client = new Redis(url);

  client.on('error', (err) => {
    logger.error('Redis client error', {
      message: err instanceof Error ? err.message : String(err),
    });
  });

  client.on('connect', () => {
    logger.info('Redis connection established');
  });

  return client;
}

export function getRedisClient(): Redis {
  if (!client) throw new Error('Redis not initialized. Call initializeRedis() first.');
  return client;
}

export async function closeRedis(): Promise<void> {
  if (client) {
    await client.quit();
    client = null;
    logger.info('Redis client disconnected');
  }
}
