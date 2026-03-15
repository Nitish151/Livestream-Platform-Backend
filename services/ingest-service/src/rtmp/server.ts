import NodeMediaServer from 'node-media-server';
import crypto from 'node:crypto';
import { query } from '../db/index.js';
import { getRedisClient } from '../redis/client.js';
import {
  publishStreamStarted,
  publishStreamEnded,
  publishTranscodingJob,
} from '../kafka/producer.js';
import logger from '../utils/logger.js';

interface StreamRecord {
  id: string;
  user_id: string;
  title: string;
}

interface NmsSession {
  streamPath: string;
  close(): void;
}

function hashStreamKey(rawKey: string): string {
  return crypto.createHash('sha256').update(rawKey).digest('hex');
}

/** Extracts the stream key from an RTMP path of the form /live/<stream-key> */
function extractStreamKey(streamPath: string): string | null {
  const parts = streamPath.split('/');
  const key = parts[parts.length - 1];
  return key || null;
}

export function createRtmpServer(): NodeMediaServer {
  const bindHost = process.env.HOST ?? '0.0.0.0';
  const ingestHttpPort = parseInt(process.env.HTTP_PORT ?? '3001', 10);
  const mediaHttpPort = parseInt(process.env.NMS_HTTP_PORT ?? String(ingestHttpPort + 1), 10);

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

  (nms as any).on('prePublish', async (session: NmsSession) => {
    const { streamPath } = session;

    const rawKey = extractStreamKey(streamPath);
    if (!rawKey) {
      logger.warn('prePublish: missing stream key', { streamPath });
      session.close();
      return;
    }

    const keyHash = hashStreamKey(rawKey);

    try {
      const result = await query<StreamRecord>(
        'SELECT id, user_id, title FROM streams WHERE stream_key_hash = $1',
        [keyHash],
      );

      if (result.rowCount === 0) {
        logger.warn('prePublish: invalid stream key — connection rejected', { streamPath });
        session.close();
        return;
      }

      const stream = result.rows[0];
      const startedAt = new Date().toISOString();
      const redis = getRedisClient();

      await redis.set(
        `stream:state:${stream.id}`,
        JSON.stringify({ status: 'LIVE', userId: stream.user_id, startedAt }),
      );

      await query(
        'INSERT INTO stream_sessions (stream_id, started_at) VALUES ($1, $2)',
        [stream.id, startedAt],
      );

      await query(
        'UPDATE streams SET is_live = TRUE, started_at = $1 WHERE id = $2',
        [startedAt, stream.id],
      );

      const rtmpPort = process.env.RTMP_PORT ?? '1935';
      const rtmpUrl = `rtmp://localhost:${rtmpPort}/live/${rawKey}`;

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

      logger.info('prePublish: stream accepted', { streamId: stream.id, userId: stream.user_id });
    } catch (err) {
      logger.error('prePublish: unexpected error', {
        message: err instanceof Error ? err.message : String(err),
      });
      session.close();
    }
  });

  (nms as any).on('donePublish', async (session: NmsSession) => {
    const { streamPath } = session;
    const rawKey = extractStreamKey(streamPath);
    if (!rawKey) return;

    const keyHash = hashStreamKey(rawKey);

    try {
      const result = await query<StreamRecord>(
        'SELECT id FROM streams WHERE stream_key_hash = $1',
        [keyHash],
      );

      if (result.rowCount === 0) return;

      const stream = result.rows[0];
      const endedAt = new Date().toISOString();
      const redis = getRedisClient();

      await redis.set(
        `stream:state:${stream.id}`,
        JSON.stringify({ status: 'ENDED', endedAt }),
      );

      await query(
        'UPDATE stream_sessions SET ended_at = $1 WHERE stream_id = $2 AND ended_at IS NULL',
        [endedAt, stream.id],
      );

      await query(
        'UPDATE streams SET is_live = FALSE, ended_at = $1 WHERE id = $2',
        [endedAt, stream.id],
      );

      await publishStreamEnded({
        eventType: 'STREAM_ENDED',
        streamId: stream.id,
        endedAt,
      });

      logger.info('donePublish: stream ended', { streamId: stream.id });
    } catch (err) {
      logger.error('donePublish: unexpected error', {
        message: err instanceof Error ? err.message : String(err),
      });
    }
  });

  return nms;
}
