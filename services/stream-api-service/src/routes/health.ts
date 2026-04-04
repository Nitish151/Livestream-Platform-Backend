import type { FastifyPluginAsync } from 'fastify';

const healthRoutes: FastifyPluginAsync = async (app) => {
  app.get('/health', async (_request, reply) => {
    const payload: Record<string, unknown> = {
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    };

    if ((process.env.DETAILED_HEALTH ?? '').toLowerCase() === 'true') {
      try {
        // lazy-import to avoid startup ordering issues
        const db = await import('../db/client.js');
        // db.healthCheck returns boolean
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        // @ts-ignore
        payload.db = await db.healthCheck();
      } catch (err) {
        payload.db = false;
      }

      try {
        const redisMod = await import('../redis/client.js');
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        const redis = redisMod.getRedisClient();
        // ping may return 'PONG'
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        // @ts-ignore
        payload.redis = (await redis.ping()) === 'PONG';
      } catch (err) {
        payload.redis = false;
      }
    }

    void reply.code(200).send(payload);
  });
};

export default healthRoutes;
