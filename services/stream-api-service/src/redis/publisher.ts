import { Redis } from 'ioredis';
import logger from '../utils/logger.js';

let redisPublisherClient: Redis | null = null;

export function initializeRedisPublisherClient(): Redis {
	if (redisPublisherClient) return redisPublisherClient;

	const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
	redisPublisherClient = new Redis(url);

	redisPublisherClient.on('error', (err) => {
		logger.error('Redis publisher client error', {
			message: err instanceof Error ? err.message : String(err),
		});
	});

	redisPublisherClient.on('connect', () => {
		logger.info('Redis publisher connection established');
	});

	return redisPublisherClient;
}

export function getRedisPublisherClient(): Redis {
	if (!redisPublisherClient) {
		return initializeRedisPublisherClient();
	}

	return redisPublisherClient;
}

export async function closeRedisPublisherClient(): Promise<void> {
	if (redisPublisherClient) {
		await redisPublisherClient.quit();
		redisPublisherClient = null;
		logger.info('Redis publisher client disconnected');
	}
}
