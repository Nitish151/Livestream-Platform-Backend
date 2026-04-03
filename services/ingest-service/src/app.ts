import dotenv from 'dotenv';
import Fastify from 'fastify';
import fjwt from '@fastify/jwt';
import cors from '@fastify/cors';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { initializePool, closePool } from './db/index.js';
import { initializeRedis, closeRedis } from './redis/client.js';
import { initializeProducer, closeProducer } from './kafka/producer.js';
import { createRtmpServer } from './rtmp/server.js';
import requestLogger from './plugins/requestLogger.js';
import streamsRoutes from './routes/streams.js';
import healthRoutes from './routes/health.js';
import metricsRoutes from './routes/metrics.js';
import logger from './utils/logger.js';

dotenv.config();

async function bootstrap() {
  const app = Fastify({ logger: false });

  const publicKeyPath = resolve(process.cwd(), process.env.JWT_PUBLIC_KEY_PATH ?? 'keys/public.pem');
  const publicKey = process.env.JWT_PUBLIC_KEY
    ? process.env.JWT_PUBLIC_KEY.replace(/\\n/g, '\n')
    : existsSync(publicKeyPath)
      ? readFileSync(publicKeyPath, 'utf-8')
      : (() => {
          throw new Error(`JWT public key not found at ${publicKeyPath}`);
        })();

  await app.register(fjwt, {
    secret: { public: publicKey },
    verify: { algorithms: ['RS256'] },
  });

  app.decorate('authenticate', async function authenticate(request, reply) {
    try {
      await request.jwtVerify();
      request.jwtUser = request.user as { sub: string; username: string; role: string };
    } catch {
      reply.code(401).send({ message: 'Invalid or expired token' });
    }
  });

  /* ── CORS ── */
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

  await app.register(requestLogger);

  logger.info('Initializing ingest infrastructure');
  initializePool();
  initializeRedis();
  await initializeProducer();

  const rtmpServer = createRtmpServer();
  rtmpServer.run();
  logger.info('RTMP server started');

  await app.register(streamsRoutes);
  await app.register(healthRoutes, { prefix: '/health' });
  await app.register(metricsRoutes);

  const port = parseInt(process.env.HTTP_PORT ?? '3001', 10);
  const mediaHttpPort = parseInt(process.env.NMS_HTTP_PORT ?? String(port + 1), 10);
  const host = process.env.HOST ?? '0.0.0.0';

  if (port === mediaHttpPort) {
    throw new Error('Port conflict: HTTP_PORT and NMS_HTTP_PORT must be different');
  }

  await app.listen({ port, host });
  logger.info(`Ingest service listening on ${host}:${port}`);

  const shutdown = async (signal: string) => {
    logger.info(`${signal} received. Shutting down ingest service.`);
    await app.close();
    const stoppable = rtmpServer as unknown as { stop?: () => void };
    stoppable.stop?.();
    await closeProducer();
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
  logger.error('Failed to start ingest service', {
    message: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack : undefined,
  });
  process.exit(1);
});
