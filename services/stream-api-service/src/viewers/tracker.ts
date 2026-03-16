import { AppError } from '../utils/errors.js';
import { getRedisClient } from '../redis/client.js';
import { publishViewerEvent } from '../kafka/producer.js';

const VIEWER_SESSION_TTL_SECONDS = 90;
const VIEWER_COUNT_CACHE_TTL_SECONDS = 10;
const DEFAULT_RENDITION = '720p';

type ViewerEventType = 'STREAM_JOIN' | 'STREAM_LEAVE' | 'VIEWER_PING';

interface ViewerSession {
  streamId: string;
  userId: string;
  joinedAt: string;
}

interface BaseTrackerInput {
  streamId: string;
  sessionId: string;
  userId: string;
  traceId: string;
  ip: string;
  userAgent: string;
  rendition?: string;
}

interface LeaveTrackerInput extends BaseTrackerInput {
  watchDurationSeconds?: number;
}

function getViewerCountHllKey(streamId: string): string {
  return `viewercount:${streamId}`;
}

function getViewerCountCacheKey(streamId: string): string {
  return `viewercount:cache:${streamId}`;
}

function getViewerSessionKey(sessionId: string): string {
  return `viewer:session:${sessionId}`;
}

function getLiveViewersKey(streamId: string): string {
  return `live-viewers:${streamId}`;
}

async function publishEvent(eventType: ViewerEventType, input: BaseTrackerInput, watchDurationSeconds: number): Promise<void> {
  await publishViewerEvent({
    eventType,
    streamId: input.streamId,
    userId: input.userId,
    timestamp: new Date().toISOString(),
    traceId: input.traceId,
    sessionId: input.sessionId,
    ip: input.ip,
    userAgent: input.userAgent,
    rendition: input.rendition ?? DEFAULT_RENDITION,
    watchDurationSeconds,
  });
}

export async function joinViewer(input: BaseTrackerInput): Promise<{ joinedAt: string }> {
  const redis = getRedisClient();
  const joinedAt = new Date().toISOString();
  const expiresAt = Math.floor(Date.now() / 1000) + VIEWER_SESSION_TTL_SECONDS;

  await redis.pfadd(getViewerCountHllKey(input.streamId), input.userId);
  await redis.set(
    getViewerSessionKey(input.sessionId),
    JSON.stringify({ streamId: input.streamId, userId: input.userId, joinedAt } satisfies ViewerSession),
    'EX',
    VIEWER_SESSION_TTL_SECONDS,
  );
  await redis.zadd(getLiveViewersKey(input.streamId), expiresAt, input.sessionId);
  await publishEvent('STREAM_JOIN', input, 0);

  return { joinedAt };
}

export async function pingViewer(input: BaseTrackerInput): Promise<void> {
  const redis = getRedisClient();
  const refreshResult = await redis.expire(getViewerSessionKey(input.sessionId), VIEWER_SESSION_TTL_SECONDS);

  if (refreshResult !== 1) {
    throw new AppError('viewer session not found', 404);
  }

  const expiresAt = Math.floor(Date.now() / 1000) + VIEWER_SESSION_TTL_SECONDS;
  await redis.zadd(getLiveViewersKey(input.streamId), expiresAt, input.sessionId);
  await publishEvent('VIEWER_PING', input, 0);
}

export async function leaveViewer(input: LeaveTrackerInput): Promise<{ watchDurationSeconds: number }> {
  const redis = getRedisClient();
  const sessionKey = getViewerSessionKey(input.sessionId);
  const rawSession = await redis.get(sessionKey);

  let watchDurationSeconds = input.watchDurationSeconds;
  if (watchDurationSeconds === undefined && rawSession) {
    try {
      const parsed = JSON.parse(rawSession) as ViewerSession;
      const joinedAtMs = Date.parse(parsed.joinedAt);
      const nowMs = Date.now();
      if (Number.isFinite(joinedAtMs)) {
        watchDurationSeconds = Math.max(0, Math.floor((nowMs - joinedAtMs) / 1000));
      }
    } catch {
      // Fallback to 0 if parsing session fails.
    }
  }

  const resolvedWatchDurationSeconds = watchDurationSeconds ?? 0;
  await publishEvent('STREAM_LEAVE', input, resolvedWatchDurationSeconds);
  await redis.del(sessionKey);
  await redis.zrem(getLiveViewersKey(input.streamId), input.sessionId);

  return { watchDurationSeconds: resolvedWatchDurationSeconds };
}

export async function getViewerCount(streamId: string): Promise<{ viewerCount: number; liveViewerCount: number; cacheHit: boolean }> {
  const redis = getRedisClient();
  const liveKey = getLiveViewersKey(streamId);
  const nowSec = Math.floor(Date.now() / 1000);

  // Single round-trip: prune sessions whose expiry score has passed, then count what remains.
  const pipelineResults = await redis.pipeline()
    .zremrangebyscore(liveKey, 0, nowSec)
    .zcard(liveKey)
    .exec();
  const liveViewerCount = (pipelineResults?.[1]?.[1] as number) ?? 0;

  // Cumulative unique-reach count (HLL) with 10s cache.
  const cacheKey = getViewerCountCacheKey(streamId);
  const cachedCount = await redis.get(cacheKey);
  if (cachedCount !== null) {
    const parsed = Number.parseInt(cachedCount, 10);
    if (!Number.isNaN(parsed)) {
      return { viewerCount: parsed, liveViewerCount, cacheHit: true };
    }
  }

  const viewerCount = await redis.pfcount(getViewerCountHllKey(streamId));
  await redis.set(cacheKey, String(viewerCount), 'EX', VIEWER_COUNT_CACHE_TTL_SECONDS);

  return { viewerCount, liveViewerCount, cacheHit: false };
}
