import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import dotenv from 'dotenv';
import Fastify from 'fastify';
import fastifyJwt from '@fastify/jwt';
import { initializePool, closePool } from './db/index.js';
import { initializeRedis, closeRedis } from './redis/index.js';
import requestLogger from './plugins/requestLogger.js';
import authRoutes from './routes/auth.js';
import logger from './utils/logger.js';

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const KEYS_DIR = path.resolve(__dirname, '..', 'keys');

async function bootstrap() {
  /* ── read RS256 key pair ── */
  const privateKey = fs.readFileSync(
    path.join(KEYS_DIR, process.env.JWT_PRIVATE_KEY_PATH?.split('/').pop() ?? 'private.pem'),
  );
  const publicKey = fs.readFileSync(
    path.join(KEYS_DIR, process.env.JWT_PUBLIC_KEY_PATH?.split('/').pop() ?? 'public.pem'),
  );

  /* ── create Fastify instance ── */
  const app = Fastify({ logger: false });

  /* ── register plugins ── */
  await app.register(requestLogger);

  await app.register(fastifyJwt, {
    secret: { private: privateKey, public: publicKey },
    sign: { algorithm: 'RS256', expiresIn: '1h' },
    verify: { algorithms: ['RS256'] },
  });

  /* ── initialise infra ── */
  initializePool();
  await initializeRedis();

  /* ── register routes ── */
  await app.register(authRoutes, { prefix: '/auth' });

  /* ── start server ── */
  const port = parseInt(process.env.PORT || '3001', 10);
  const host = process.env.HOST || '0.0.0.0';

  await app.listen({ port, host });
  logger.info(`Auth service listening on ${host}:${port}`);

  /* ── graceful shutdown ── */
  const shutdown = async (signal: string) => {
    logger.info(`${signal} received — shutting down`);
    await app.close();
    await closeRedis();
    await closePool();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

bootstrap().catch((err) => {
  logger.error('Failed to start auth service', {
    message: err instanceof Error ? err.message : String(err),
    stack: err instanceof Error ? err.stack : undefined,
  });
  process.exit(1);
});
