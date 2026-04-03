import { FastifyInstance } from 'fastify';
import { query } from '../db/index.js';
import { getRedisClient } from '../redis/client.js';
import { publishStreamEnded } from '../kafka/producer.js';
import { AppError } from '../utils/errors.js';
import logger from '../utils/logger.js';

interface CreateStreamBody {
  title: string;
  description?: string;
  category?: string;
}

export default async function streamsRoutes(app: FastifyInstance): Promise<void> {

  // ─── POST /v1/streams ─────────────────────────────────────────────────────
  // Creates a stream record. Called by frontend when user clicks Go Live.
  // Returns streamId so frontend can use it for chat, viewer count, etc.
  // Does NOT receive or store the stream key — that lives on the user.
  app.post<{ Body: CreateStreamBody }>(
    '/v1/streams',
    { preHandler: [app.authenticate] }, // JWT middleware — sets request.user
    async (request, reply) => {
      const { title, description, category } = request.body ?? {};
      const userId = request.jwtUser.sub; // from JWT, never from body

      if (!title || typeof title !== 'string' || title.trim().length === 0) {
        return reply.code(400).send({ message: 'title is required' });
      }

      try {
        // Verify user exists and has a stream key set up
        const userResult = await query<{ id: string; stream_key_hash: string }>(
          'SELECT id, stream_key_hash FROM users WHERE id = $1',
          [userId],
        );

        if (userResult.rowCount === 0) {
          return reply.code(404).send({ message: 'User not found' });
        }

        if (!userResult.rows[0].stream_key_hash) {
          // User never generated a stream key
          return reply.code(400).send({
            message: 'No stream key found. Generate a stream key first.',
            code: 'NO_STREAM_KEY',
          });
        }

        // Create the stream record — no stream key here
        const result = await query<{ id: string }>(
          `INSERT INTO streams (user_id, title, description, category)
           VALUES ($1, $2, $3, $4)
           RETURNING id`,
          [userId, title.trim(), description ?? null, category ?? null],
        );

        const streamId = result.rows[0]?.id;
        if (!streamId) {
          throw new AppError('Failed to create stream', 500, 'STREAM_CREATION_FAILED');
        }

        // Return streamId and rtmp base URL
        // Frontend appends their stream key to construct the full rtmpUrl
        // e.g. rtmp://localhost:1935/live/{streamKey}
        const rtmpHost = process.env.RTMP_HOST ?? 'localhost';
        const rtmpPort = process.env.RTMP_PORT ?? '1935';
        const rtmpBase = `rtmp://${rtmpHost}:${rtmpPort}/live`;

        return reply.status(201).send({ streamId, rtmpBase });

      } catch (error) {
        logger.error('Failed to create stream', {
          message: error instanceof Error ? error.message : String(error),
        });
        if (error instanceof AppError) {
          return reply.code(error.statusCode).send({
            message: error.message,
            code: error.code,
          });
        }
        return reply.code(500).send({ message: 'Failed to create stream' });
      }
    },
  );

  // ─── POST /v1/streams/:id/end ─────────────────────────────────────────────
  // Manual stop stream — called when user clicks "Stop Streaming" button.
  // Verifies the requesting user owns this stream via JWT.
  // The RTMP donePublish hook also ends streams automatically on disconnect —
  // this endpoint handles the case where user clicks stop before disconnecting.
  app.post<{ Params: { id: string } }>(
    '/v1/streams/:id/end',
    { preHandler: [app.authenticate] },
    async (request, reply) => {
      const streamId = request.params.id;
      const userId = request.jwtUser.sub; // from JWT

      if (!streamId) {
        return reply.code(400).send({ message: 'streamId is required' });
      }

      try {
        // Find stream and verify ownership via userId from JWT
        // No stream key needed — JWT proves identity
        const streamResult = await query<{
          id: string;
          user_id: string;
          is_live: boolean;
        }>(
          `SELECT id, user_id, is_live
           FROM streams
           WHERE id = $1 AND user_id = $2`,
          [streamId, userId],
        );

        if (streamResult.rowCount === 0) {
          // Either stream doesn't exist or user doesn't own it
          return reply.code(403).send({ message: 'Stream not found or access denied' });
        }

        const stream = streamResult.rows[0];

        if (!stream.is_live) {
          return reply.code(400).send({ message: 'Stream is not live' });
        }

        const endedAt = new Date().toISOString();

        // Close the open session
        await query(
          `UPDATE stream_sessions
           SET ended_at = $1
           WHERE stream_id = $2 AND ended_at IS NULL`,
          [endedAt, streamId],
        );

        // Mark stream as ended
        await query(
          `UPDATE streams SET is_live = FALSE, ended_at = $1 WHERE id = $2`,
          [endedAt, streamId],
        );

        // Update Redis state
        const redis = getRedisClient();
        await redis.set(
          `stream:state:${streamId}`,
          JSON.stringify({ status: 'ENDED', endedAt }),
        );

        // Notify downstream services (Transcoding kills FFmpeg, Stream API notifies viewers)
        await publishStreamEnded({
          eventType: 'STREAM_ENDED',
          streamId,
          endedAt,
        });

        logger.info('Stream ended via API', { streamId, userId });
        return reply.send({ streamId, endedAt });

      } catch (error) {
        logger.error('Failed to end stream', {
          message: error instanceof Error ? error.message : String(error),
        });
        if (error instanceof AppError) {
          return reply.code(error.statusCode).send({
            message: error.message,
            code: error.code,
          });
        }
        return reply.code(500).send({ message: 'Failed to end stream' });
      }
    },
  );
}