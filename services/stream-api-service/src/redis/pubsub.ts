import { Redis } from 'ioredis';
import logger from '../utils/logger.js';

let redisSubClient: Redis | null = null;

export function initializeRedisSubClient(): Redis {
	if (redisSubClient) return redisSubClient;

	const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
	redisSubClient = new Redis(url);

	redisSubClient.on('error', (err) => {
		logger.error('Redis subscriber client error', {
			message: err instanceof Error ? err.message : String(err),
		});
	});

	redisSubClient.on('connect', () => {
		logger.info('Redis subscriber connection established');
	});

	return redisSubClient;
}

export function getRedisSubClient(): Redis {
	if (!redisSubClient) {
		return initializeRedisSubClient();
	}

	return redisSubClient;
}

export async function closeRedisSubClient(): Promise<void> {
	if (redisSubClient) {
		await redisSubClient.quit();
		redisSubClient = null;
		logger.info('Redis subscriber client disconnected');
	}
}
