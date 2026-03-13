import { FastifyPluginAsync } from 'fastify';
import { healthCheck } from '../db/index.js';
import { getRedisClient } from '../redis/client.js';
import { getProducer } from '../kafka/producer.js';

const healthRoutes: FastifyPluginAsync = async (app) => {
  app.get('/live', async () => {
    return {
      status: 'ok',
      service: 'ingest-service',
      check: 'live',
      timestamp: new Date().toISOString(),
    };
  });

  app.get('/ready', async (request, reply) => {
    const dbReady = await healthCheck();

    let redisReady = false;
    try {
      redisReady = (await getRedisClient().ping()) === 'PONG';
    } catch {
      redisReady = false;
    }

    let kafkaReady = false;
    try {
      kafkaReady = Boolean(getProducer());
    } catch {
      kafkaReady = false;
    }

    const ready = dbReady && redisReady && kafkaReady;
    if (!ready) {
      return reply.status(503).send({
        status: 'not_ready',
        checks: {
          db: dbReady,
          redis: redisReady,
          kafka: kafkaReady,
        },
        timestamp: new Date().toISOString(),
      });
    }

    return {
      status: 'ready',
      checks: {
        db: dbReady,
        redis: redisReady,
        kafka: kafkaReady,
      },
      timestamp: new Date().toISOString(),
    };
  });
};

export default healthRoutes;
