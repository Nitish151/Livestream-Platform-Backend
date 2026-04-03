import { FastifyInstance, FastifyPluginCallback } from 'fastify';
import fp from 'fastify-plugin';
import logger from '../utils/logger.js';

/**
 * Fastify plugin that logs every HTTP request/response,
 * including 404s and unmatched routes.
 *
 * Wrapped in fastify-plugin to ensure hooks apply globally.
 */
const requestLoggerPlugin: FastifyPluginCallback = (app: FastifyInstance, _opts, done) => {
  /* ── track request start time for all requests ── */
  app.addHook('onRequest', async (request) => {
    (request as any).startTime = process.hrtime.bigint();

    logger.info(`→ ${request.method} ${request.url}`, {
      reqId: request.id,
      ip: request.ip,
    });
  });

  /* ── log response (fires for matched routes and handled 404s) ── */
  app.addHook('onResponse', async (request, reply) => {
    logResponse(request, reply.statusCode);
  });
  app.addHook('onResponse', async (request: any, reply) => {
    logResponse(request, reply.statusCode);
  });

  /* ── log errors ── */
  app.addHook('onError', async (request, _reply, error) => {
    logger.error(`✗ ${request.method} ${request.url} — ${error.message}`, {
      reqId: request.id,
      errorName: error.name,
      stack: error.stack,
    });
  });

  /* ── catch 404s (unmatched routes) ── */
  app.setNotFoundHandler((request, reply) => {
    const startTime: bigint = (request as any).startTime ?? process.hrtime.bigint();
    const durationMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;

    // logger.info was already called in onRequest, so we just log the response aspect
    logger.warn(`← ${request.method} ${request.url} 404 ${durationMs.toFixed(1)}ms`, {
      reqId: request.id,
      statusCode: 404,
      durationMs: Math.round(durationMs),
    });

    return reply.status(404).send({
      error: 'Not Found',
      message: `Route ${request.method}:${request.url} not found. Did you use the correct method?`,
      statusCode: 404,
    });
  });

  done();
};

/* ── shared helper ── */
function logResponse(request: any, statusCode: number) {
  const startTime: bigint = request.startTime ?? process.hrtime.bigint();
  const durationMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;
  const level = statusCode >= 400 ? 'warn' : 'info';

  logger[level](`← ${request.method} ${request.url} ${statusCode} ${durationMs.toFixed(1)}ms`, {
    reqId: request.id,
    statusCode,
    durationMs: Math.round(durationMs),
  });
}

export default fp(requestLoggerPlugin);
