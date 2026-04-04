import NodeMediaServer from 'node-media-server';
import crypto from 'node:crypto';
import bcrypt from 'bcrypt';
import { query } from '../db/index.js';
import { getRedisClient } from '../redis/client.js';
import {
  publishStreamStarted,
  publishStreamEnded,
  publishTranscodingJob,
} from '../kafka/producer.js';
import logger from '../utils/logger.js';
import { rtmpConnectionsActive } from '../metrics/registry.js';

interface UserRecord {
  id: string;
}

interface StreamRecord {
  id: string;
  user_id: string;
  title: string;
}

interface NmsSession {
  streamPath: string;
  close(): void;
}

/** Extracts the stream key from an RTMP path of the form /live/<stream-key> */
function extractStreamKey(streamPath: string): string | null {
  const parts = streamPath.split('/');
  const key = parts[parts.length - 1];
  return key || null;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}

async function findUserByStreamKey(rawKey: string): Promise<UserRecord & { stream_key_hash: string } | null> {
  // 1) Preferred format: userId_token (faster key lookup)
  if (rawKey.includes('_')) {
    const userId = rawKey.split('_')[0];
    if (isUuid(userId)) {
      const userResult = await query<UserRecord & { stream_key_hash: string }>(
        'SELECT id, stream_key_hash FROM users WHERE id = $1',
        [userId],
      );

      if (userResult.rowCount > 0) {
        const user = userResult.rows[0];
        const isValid = await bcrypt.compare(rawKey, user.stream_key_hash);
        if (isValid) {
          return user;
        }
      }
    }
  }

  // 2) Legacy format: plain token only (requires scan)
  const userResult = await query<UserRecord & { stream_key_hash: string }>(
    'SELECT id, stream_key_hash FROM users WHERE stream_key_hash IS NOT NULL',
  );

  for (const user of userResult.rows) {
    if (await bcrypt.compare(rawKey, user.stream_key_hash)) {
      return user;
    }
  }

  return null;
}

export function createRtmpServer(): NodeMediaServer {
  const bindHost = process.env.HOST ?? '0.0.0.0';
  const ingestHttpPort = parseInt(process.env.HTTP_PORT ?? '3001', 10);
  const mediaHttpPort = parseInt(
    process.env.NMS_HTTP_PORT ?? String(ingestHttpPort + 1),
    10,
  );

  const config = {
    bind: bindHost,
    rtmp: {
      port: parseInt(process.env.RTMP_PORT ?? '1935', 10),
      chunk_size: 60000,
      gop_cache: true,
      ping: 30,
      ping_timeout: 60,
    },
    http: {
      port: mediaHttpPort,
      mediaroot: './media',
      allow_origin: '*',
    },
  };

  const nms = new NodeMediaServer(config);
  const activeRtmpPublishers = new Set<string>();

  // ─── prePublish ───────────────────────────────────────────────────────────
  // Fires when OBS / any RTMP client connects and wants to start publishing.
  // We validate the stream key, find the user's most recent non-live stream,
  // mark it live, and kick off the transcoding pipeline.
  (nms as any).on('prePublish', async (session: NmsSession) => {
    const { streamPath } = session;
    const streamId = (session as any).args?.streamId;

    // 1. Extract raw key from path e.g. /live/uuid_abc123 → "uuid_abc123"
    const rawKey = extractStreamKey(streamPath);
    if (!rawKey) {
      logger.warn('prePublish: missing stream key', { streamPath });
      session.close();
      return;
    }

    try {
      const user = await findUserByStreamKey(rawKey);
      if (!user) {
        logger.warn('prePublish: user not found / invalid stream key', { rawKey, streamPath });
        session.close();
        return;
      }

      const userId = user.id;

      // 4. Find the user's most recently created stream that isn't live yet.
      //    This is the stream they created via POST /v1/streams before going live.
      let streamResult;
      if (streamId) {
        streamResult = await query<StreamRecord>(
          `SELECT id, user_id, title
           FROM streams
           WHERE id = $1 AND user_id = $2 AND is_live = FALSE`,
          [streamId, userId],
        );
      } else {
        streamResult = await query<StreamRecord>(
          `SELECT id, user_id, title
           FROM streams
           WHERE user_id = $1 AND is_live = FALSE
           ORDER BY created_at DESC
           LIMIT 1`,
          [userId],
        );
      }

      if (streamResult.rowCount === 0) {
        logger.warn('prePublish: no pending stream found for user', { userId });
        // No stream was created via the API first — reject.
        // Streamer must call POST /v1/streams before going live.
        session.close();
        return;
      }

      const stream = streamResult.rows[0];
      const startedAt = new Date().toISOString();
      const redis = getRedisClient();

      // 5. Mark stream as live in Redis (fast state for other services)
      await redis.set(
        `stream:state:${stream.id}`,
        JSON.stringify({
          status: 'LIVE',
          userId: stream.user_id,
          startedAt,
        }),
      );

      // 6. Create a session record and update the stream row in PostgreSQL
      await query(
        'INSERT INTO stream_sessions (stream_id, started_at) VALUES ($1, $2)',
        [stream.id, startedAt],
      );

      await query(
        'UPDATE streams SET is_live = TRUE, started_at = $1 WHERE id = $2',
        [startedAt, stream.id],
      );

      // 7. Build the RTMP relay URL that FFmpeg will read from
      const rtmpPort = process.env.RTMP_PORT ?? '1935';
      const rtmpUrl = `rtmp://localhost:${rtmpPort}/live/${rawKey}`;

      // 8. Publish Kafka events — downstream services react to these
      await publishStreamStarted({
        eventType: 'STREAM_STARTED',
        streamId: stream.id,
        userId: stream.user_id,
        title: stream.title,
        startedAt,
      });

      await publishTranscodingJob({
        eventType: 'TRANSCODING_JOB_REQUESTED',
        streamId: stream.id,
        userId: stream.user_id,
        timestamp: startedAt,
        traceId: crypto.randomUUID(),
        jobId: crypto.randomUUID(),
        rtmpUrl,
        requestedAt: startedAt,
      });

      activeRtmpPublishers.add(stream.id);
      rtmpConnectionsActive.set(activeRtmpPublishers.size);

      logger.info('prePublish: stream accepted', {
        streamId: stream.id,
        userId: stream.user_id,
      });
    } catch (err) {
      logger.error('prePublish: unexpected error', {
        message: err instanceof Error ? err.message : String(err),
      });
      session.close();
    }
  });

  // ─── donePublish ──────────────────────────────────────────────────────────
  // Fires when OBS / RTMP client disconnects or stops streaming.
  // We mark the stream as ended in Redis, DB, and notify downstream via Kafka.
  (nms as any).on('donePublish', async (session: NmsSession) => {
    const { streamPath } = session;

    const rawKey = extractStreamKey(streamPath);
    if (!rawKey) return;

    try {
      const user = await findUserByStreamKey(rawKey);
      if (!user) {
        logger.warn('donePublish: unknown stream key', { rawKey });
        return;
      }

      const userId = user.id;

      const streamResult = await query<StreamRecord>(
        `SELECT id, user_id, title
         FROM streams
         WHERE user_id = $1 AND is_live = TRUE
         LIMIT 1`,
        [userId],
      );

      if (streamResult.rowCount === 0) {
        logger.warn('donePublish: no live stream found for user', { userId });
        return;
      }

      const stream = streamResult.rows[0];
      const endedAt = new Date().toISOString();
      const redis = getRedisClient();

      // 2. Update Redis state
      await redis.set(
        `stream:state:${stream.id}`,
        JSON.stringify({ status: 'ENDED', endedAt }),
      );

      // 3. Close the open session record
      await query(
        `UPDATE stream_sessions
         SET ended_at = $1
         WHERE stream_id = $2 AND ended_at IS NULL`,
        [endedAt, stream.id],
      );

      // 4. Mark stream as no longer live in PostgreSQL
      await query(
        'UPDATE streams SET is_live = FALSE, ended_at = $1 WHERE id = $2',
        [endedAt, stream.id],
      );

      // 5. Notify downstream services via Kafka
      await publishStreamEnded({
        eventType: 'STREAM_ENDED',
        streamId: stream.id,
        endedAt,
      });

      activeRtmpPublishers.delete(stream.id);
      rtmpConnectionsActive.set(activeRtmpPublishers.size);

      logger.info('donePublish: stream ended', { streamId: stream.id });
    } catch (err) {
      logger.error('donePublish: unexpected error', {
        message: err instanceof Error ? err.message : String(err),
      });
    }
  });

  return nms;
}