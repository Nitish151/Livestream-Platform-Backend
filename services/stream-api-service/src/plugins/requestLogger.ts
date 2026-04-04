import type { FastifyInstance, FastifyRequest } from 'fastify';
import logger from '../utils/logger.js';
import { httpRequestDurationMs } from '../metrics/registry.js';

type TimedRequest = FastifyRequest & {
  startTime?: bigint;
};

function logResponse(request: TimedRequest, statusCode: number): void {
  const startTime = request.startTime ?? process.hrtime.bigint();
  const durationMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;
  const level = statusCode >= 400 ? 'warn' : 'info';
  const route = request.routeOptions?.url ?? request.url.split('?')[0] ?? 'unknown';

  httpRequestDurationMs.observe(
    { route, status: String(statusCode) },
    durationMs,
  );

  logger[level](`← ${request.method} ${request.url} ${statusCode} ${durationMs.toFixed(1)}ms`, {
    reqId: request.id,
    statusCode,
    durationMs: Math.round(durationMs),
  });
}

export default async function requestLogger(app: FastifyInstance): Promise<void> {
  app.addHook('onRequest', async (request) => {
    (request as TimedRequest).startTime = process.hrtime.bigint();

    logger.info(`→ ${request.method} ${request.url}`, {
      reqId: request.id,
      ip: request.ip,
    });
  });

  app.addHook('onResponse', async (request, reply) => {
    logResponse(request as TimedRequest, reply.statusCode);
  });

  app.addHook('onError', async (request, _reply, error) => {
    logger.error(`✗ ${request.method} ${request.url} — ${error.message}`, {
      reqId: request.id,
      errorName: error.name,
      stack: error.stack,
    });
  });

  app.setNotFoundHandler((request, reply) => {
    logResponse(request as TimedRequest, 404);

    return reply.status(404).send({
      error: 'Not Found',
      message: `Route ${request.method}:${request.url} not found. Did you use the correct method?`,
      statusCode: 404,
    });
  });
}