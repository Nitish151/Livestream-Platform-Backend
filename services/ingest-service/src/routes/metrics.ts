import { FastifyPluginAsync } from 'fastify';

const metricsRoutes: FastifyPluginAsync = async (app) => {
  app.get('/metrics', async (_request, reply) => {
    return reply.status(501).send({
      status: 'not_implemented',
      message: 'Metrics endpoint placeholder. Implementation planned for Phase 6.',
    });
  });
};

export default metricsRoutes;
