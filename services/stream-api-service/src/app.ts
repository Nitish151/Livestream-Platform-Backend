import dotenv from 'dotenv';
import Fastify from 'fastify';
import fjwt from '@fastify/jwt';
import websocket from '@fastify/websocket';
import cors from '@fastify/cors';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { initializePool, closePool } from './db/index.js';
import { initializeRedis, closeRedis } from './redis/client.js';
import { initializeRedisSubClient, closeRedisSubClient } from './redis/pubsub.js';
import { initializeRedisPublisherClient, closeRedisPublisherClient } from './redis/publisher.js';
import { initializeProducer, closeProducer } from './kafka/producer.js';
import requestLogger from './plugins/requestLogger.js';
import streamsRoutes from './routes/streams.js';
import chatWebSocketRoutes from './websocket/chatHandler.js';

import logger from './utils/logger.js';

dotenv.config();

async function bootstrap() {
  const app = Fastify({ logger: false });

  await app.register(requestLogger);

  const defaultCorsOrigins = [
    'http://localhost:5000',
    'http://localhost:5500',
    'http://127.0.0.1:5500',
    'http://localhost:5173',
  ];

  const allowedOrigins = (process.env.STREAM_API_CORS_ORIGINS ?? defaultCorsOrigins.join(','))
    .split(',')
    .map((origin) => origin.trim())
    .filter(Boolean);

  await app.register(cors, {
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
        return;
      }

      callback(new Error('CORS origin not allowed'), false);
    },
    methods: ['GET', 'POST', 'PATCH', 'OPTIONS'],
  });

  // ── Register @fastify/jwt with the Auth Service RS256 public key ──
  const publicKeyPath = resolve(process.cwd(), process.env.JWT_PUBLIC_KEY_PATH ?? 'keys/public.pem');
  const publicKey = process.env.JWT_PUBLIC_KEY
    ? process.env.JWT_PUBLIC_KEY.replace(/\\n/g, '\n')
    : existsSync(publicKeyPath)
      ? readFileSync(publicKeyPath, 'utf-8')
      : (() => { throw new Error(`JWT public key not found at ${publicKeyPath}`); })();

  await app.register(fjwt, {
    secret: { public: publicKey },
    verify: { algorithms: ['RS256'] },
  });
  await app.register(websocket);

  logger.info('Initializing stream API infrastructure');
  initializePool();
  initializeRedis();
  initializeRedisSubClient();
  initializeRedisPublisherClient();
  await initializeProducer();

  await app.register(streamsRoutes);
  await app.register(chatWebSocketRoutes);

  const port = parseInt(process.env.HTTP_PORT ?? '3003', 10);
  const host = process.env.HOST ?? '0.0.0.0';

  await app.listen({ port, host });
  logger.info(`Stream API service listening on ${host}:${port}`);

  const shutdown = async (signal: string) => {
    logger.info(`${signal} received. Shutting down stream API service.`);
    await app.close();
    await closeProducer();
    await closeRedisPublisherClient();
    await closeRedisSubClient();
    await closeRedis();
    await closePool();
    process.exit(0);
  };

  process.on('SIGINT', () => {
    void shutdown('SIGINT');
  });
  process.on('SIGTERM', () => {
    void shutdown('SIGTERM');
  });
}

void bootstrap().catch((err) => {
  logger.error('Failed to start stream API service', {
    message: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack : undefined,
  });
  process.exit(1);
});
