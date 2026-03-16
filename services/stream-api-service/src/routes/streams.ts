import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { query } from '../db/client.js';
import { requireAuth } from '../middleware/auth.js';
import { AppError, normalizeError } from '../utils/errors.js';
import logger from '../utils/logger.js';
import { getObjectBuffer, getObjectText, SEGMENTS_BUCKET } from '../storage/minio.js';
import { getViewerCount, joinViewer, leaveViewer, pingViewer } from '../viewers/tracker.js';

type StreamParams = {
  streamId: string;
};

type SegmentParams = {
  streamId: string;
  rendition: string;
  filename: string;
};

type StreamIdParams = {
  id: string;
};

type StreamMetadata = {
  id: string;
  userId: string;
  title: string;
  description: string | null;
  category: string | null;
  isLive: boolean;
  startedAt: string | null;
  endedAt: string | null;
};

type PatchStreamBody = {
  title?: string;
  description?: string;
  category?: string;
};

type ViewerJoinBody = {
  sessionId?: string;
  userId?: string;
  rendition?: string;
};

type ViewerPingBody = {
  sessionId?: string;
  userId?: string;
  rendition?: string;
};

type ViewerLeaveBody = {
  sessionId?: string;
  userId?: string;
  rendition?: string;
  watchDurationSeconds?: number;
};

function getApiBaseUrl(request: FastifyRequest): string {
  const configuredBaseUrl = process.env.STREAM_API_BASE_URL;
  if (configuredBaseUrl) {
    return configuredBaseUrl.replace(/\/$/, '');
  }

  const forwardedProto = request.headers['x-forwarded-proto'];
  const protocol = Array.isArray(forwardedProto)
    ? forwardedProto[0]
    : forwardedProto ?? request.protocol;

  return `${protocol}://${request.headers.host}`;
}

function rewriteManifestUris(manifest: string, streamId: string, apiBaseUrl: string): string {
  return manifest
    .split('\n')
    .map((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) {
        return line;
      }

      let objectPath = trimmed;
      try {
        const parsed = new URL(trimmed);
        objectPath = parsed.pathname.replace(/^\//, '');
      } catch {
        // Keep the original line if it is not an absolute URL.
      }

      if (objectPath.startsWith(`${SEGMENTS_BUCKET}/`)) {
        objectPath = objectPath.slice(SEGMENTS_BUCKET.length + 1);
      }

      if (objectPath.startsWith(`${streamId}/`)) {
        objectPath = objectPath.slice(streamId.length + 1);
      }

      return `${apiBaseUrl}/v1/segments/${streamId}/${objectPath}`;
    })
    .join('\n');
}

function getSegmentResponseHeaders(filename: string): { contentType: string; cacheControl: string } {
  if (filename.endsWith('.m3u8')) {
    return {
      contentType: 'application/vnd.apple.mpegurl',
      cacheControl: 'no-cache',
    };
  }

  return {
    contentType: 'video/mp2t',
    cacheControl: 'max-age=31536000',
  };
}

export default async function streamsRoutes(app: FastifyInstance): Promise<void> {
  app.get('/v1/streams', async (_request, reply) => {
    try {
      const result = await query<StreamMetadata>(`
        SELECT
          id,
          user_id        AS "userId",
          title,
          description,
          category,
          is_live        AS "isLive",
          started_at     AS "startedAt",
          ended_at       AS "endedAt"
        FROM streams
        WHERE is_live = TRUE
        ORDER BY started_at DESC NULLS LAST, id ASC
      `);

      reply.send(result.rows);
    } catch (error) {
      logger.error('Failed to list active streams', normalizeError(error));
      reply.code(500).send({ message: 'failed to list streams' });
    }
  });

  app.get<{ Params: StreamIdParams }>(
    '/v1/streams/:id',
    async (request: FastifyRequest<{ Params: StreamIdParams }>, reply: FastifyReply) => {
      try {
        const result = await query<StreamMetadata>(`
          SELECT
            id,
            user_id    AS "userId",
            title,
            description,
            category,
            is_live    AS "isLive",
            started_at AS "startedAt",
            ended_at   AS "endedAt"
          FROM streams
          WHERE id = $1
        `, [request.params.id]);

        if (result.rowCount === 0) {
          reply.code(404).send({ message: 'stream not found' });
          return;
        }

        reply.send(result.rows[0]);
      } catch (error) {
        logger.error('Failed to fetch stream metadata', {
          streamId: request.params.id,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to load stream metadata' });
      }
    }
  );

  app.post<{ Params: StreamIdParams; Body: ViewerJoinBody }>(
    '/v1/streams/:id/viewers/join',
    async (request: FastifyRequest<{ Params: StreamIdParams; Body: ViewerJoinBody }>, reply: FastifyReply) => {
      const { id: streamId } = request.params;
      const { sessionId, userId, rendition } = request.body ?? {};

      if (!sessionId || !userId) {
        reply.code(400).send({ message: 'sessionId and userId are required' });
        return;
      }

      try {
        const result = await joinViewer({
          streamId,
          sessionId,
          userId,
          rendition,
          traceId: request.id,
          ip: request.ip,
          userAgent: request.headers['user-agent'] ?? '',
        });

        reply.send({ streamId, sessionId, userId, joinedAt: result.joinedAt });
      } catch (error) {
        logger.error('Failed to register viewer join', {
          streamId,
          sessionId,
          userId,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to register viewer join' });
      }
    }
  );

  app.post<{ Params: StreamIdParams; Body: ViewerPingBody }>(
    '/v1/streams/:id/viewers/ping',
    async (request: FastifyRequest<{ Params: StreamIdParams; Body: ViewerPingBody }>, reply: FastifyReply) => {
      const { id: streamId } = request.params;
      const { sessionId, userId, rendition } = request.body ?? {};

      if (!sessionId || !userId) {
        reply.code(400).send({ message: 'sessionId and userId are required' });
        return;
      }

      try {
        await pingViewer({
          streamId,
          sessionId,
          userId,
          rendition,
          traceId: request.id,
          ip: request.ip,
          userAgent: request.headers['user-agent'] ?? '',
        });

        reply.send({ streamId, sessionId, userId, ttlSeconds: 90 });
      } catch (error) {
        if (error instanceof AppError) {
          reply.code(error.statusCode).send({ message: error.message });
          return;
        }

        logger.error('Failed to register viewer ping', {
          streamId,
          sessionId,
          userId,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to register viewer ping' });
      }
    }
  );

  app.post<{ Params: StreamIdParams; Body: ViewerLeaveBody }>(
    '/v1/streams/:id/viewers/leave',
    async (request: FastifyRequest<{ Params: StreamIdParams; Body: ViewerLeaveBody }>, reply: FastifyReply) => {
      const { id: streamId } = request.params;
      const { sessionId, userId, rendition, watchDurationSeconds } = request.body ?? {};

      if (!sessionId || !userId) {
        reply.code(400).send({ message: 'sessionId and userId are required' });
        return;
      }

      try {
        const result = await leaveViewer({
          streamId,
          sessionId,
          userId,
          rendition,
          watchDurationSeconds,
          traceId: request.id,
          ip: request.ip,
          userAgent: request.headers['user-agent'] ?? '',
        });

        reply.send({ streamId, sessionId, userId, watchDurationSeconds: result.watchDurationSeconds });
      } catch (error) {
        logger.error('Failed to register viewer leave', {
          streamId,
          sessionId,
          userId,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to register viewer leave' });
      }
    }
  );

  app.get<{ Params: StreamIdParams }>(
    '/v1/streams/:id/viewercount',
    async (request: FastifyRequest<{ Params: StreamIdParams }>, reply: FastifyReply) => {
      const { id: streamId } = request.params;

      try {
        const { viewerCount, liveViewerCount, cacheHit } = await getViewerCount(streamId);
        reply.send({ streamId, viewerCount, liveViewerCount, cached: cacheHit });
      } catch (error) {
        logger.error('Failed to fetch viewer count', {
          streamId,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to fetch viewer count' });
      }
    }
  );

  app.patch<{ Params: StreamIdParams; Body: PatchStreamBody }>(
    '/v1/streams/:id',
    { preHandler: requireAuth },
    async (request: FastifyRequest<{ Params: StreamIdParams; Body: PatchStreamBody }>, reply: FastifyReply) => {
      const { id } = request.params;
      const { title, description, category } = request.body ?? {};

      if (title === undefined && description === undefined && category === undefined) {
        reply.code(400).send({ message: 'Provide at least one of: title, description, category' });
        return;
      }

      // Ensure the stream exists and belongs to the requesting user
      const existing = await query<{ user_id: string }>(
        'SELECT user_id FROM streams WHERE id = $1',
        [id],
      );

      if (existing.rowCount === 0) {
        reply.code(404).send({ message: 'stream not found' });
        return;
      }

      if (existing.rows[0].user_id !== request.jwtUser.sub) {
        reply.code(403).send({ message: 'not authorised to update this stream' });
        return;
      }

      // Build a dynamic SET clause for only the supplied fields
      const updates: string[] = [];
      const values: unknown[] = [];

      if (title !== undefined) {
        values.push(title);
        updates.push(`title = $${values.length}`);
      }
      if (description !== undefined) {
        values.push(description);
        updates.push(`description = $${values.length}`);
      }
      if (category !== undefined) {
        values.push(category);
        updates.push(`category = $${values.length}`);
      }

      values.push(id);
      const sql = `
        UPDATE streams
        SET    ${updates.join(', ')}
        WHERE  id = $${values.length}
        RETURNING
          id,
          user_id    AS "userId",
          title,
          description,
          category,
          is_live    AS "isLive",
          started_at AS "startedAt",
          ended_at   AS "endedAt"
      `;

      try {
        const result = await query<StreamMetadata>(sql, values);
        reply.send(result.rows[0]);
      } catch (error) {
        logger.error('Failed to update stream', {
          streamId: id,
          ...normalizeError(error),
        });
        reply.code(500).send({ message: 'failed to update stream' });
      }
    },
  );

  app.get<{ Params: StreamParams }>(
    '/v1/streams/:streamId/master.m3u8',
    async (request: FastifyRequest<{ Params: StreamParams }>, reply: FastifyReply) => {
      const { streamId } = request.params;
      const key = `${streamId}/master.m3u8`;

      try {
        const manifest = await getObjectText(SEGMENTS_BUCKET, key);
        const apiBaseUrl = getApiBaseUrl(request);
        const rewrittenManifest = rewriteManifestUris(manifest, streamId, apiBaseUrl);

        reply
          .header('Content-Type', 'application/vnd.apple.mpegurl')
          .header('Cache-Control', 'no-cache')
          .send(rewrittenManifest);
      } catch (error) {
        const awsError = error as { code?: string };
        if (awsError.code === 'NoSuchKey') {
          reply.code(404).send({ message: 'master manifest not found' });
          return;
        }

        logger.error('Failed to fetch or rewrite master manifest', {
          streamId,
          key,
          ...normalizeError(error),
        });

        reply.code(500).send({ message: 'failed to load master manifest' });
      }
    }
  );

  app.get<{ Params: SegmentParams }>(
    '/v1/segments/:streamId/:rendition/:filename',
    async (request: FastifyRequest<{ Params: SegmentParams }>, reply: FastifyReply) => {
      const { streamId, rendition, filename } = request.params;
      const key = `${streamId}/${rendition}/${filename}`;

      try {
        const segment = await getObjectBuffer(SEGMENTS_BUCKET, key);
        const { contentType, cacheControl } = getSegmentResponseHeaders(filename);

        reply
          .header('Content-Type', contentType)
          .header('Cache-Control', cacheControl)
          .send(segment);
      } catch (error) {
        const awsError = error as { code?: string };
        if (awsError.code === 'NoSuchKey') {
          reply.code(404).send({ message: 'segment not found' });
          return;
        }

        logger.error('Failed to fetch segment', {
          streamId,
          rendition,
          filename,
          key,
          ...normalizeError(error),
        });

        reply.code(500).send({ message: 'failed to load segment' });
      }
    }
  );
}
