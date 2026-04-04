import { FastifyPluginAsync } from 'fastify';
import { metricsRegistry } from '../metrics/registry.js';

const metricsRoutes: FastifyPluginAsync = async (app) => {
  app.get('/metrics', async (_request, reply) => {
    reply.header('Content-Type', metricsRegistry.contentType);
    return reply.send(await metricsRegistry.metrics());
  });
};

export default metricsRoutes;
