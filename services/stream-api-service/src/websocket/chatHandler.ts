import type { FastifyInstance, FastifyRequest } from 'fastify';
import type WebSocket from '@fastify/websocket';
import { randomUUID } from 'node:crypto';
import logger from '../utils/logger.js';
import { getRedisClient } from '../redis/client.js';
import { getRedisSubClient } from '../redis/pubsub.js';
import { getRedisPublisherClient } from '../redis/publisher.js';
import { query } from '../db/client.js';
import { publishChatEvent } from '../kafka/producer.js';

type StreamIdParams = {
  streamId: string;
};

interface JwtPayload {
  sub: string;
  username: string;
  role: string;
  iat?: number;
  exp?: number;
}

const PING_INTERVAL_MS = 30_000;
const PONG_TIMEOUT_MS = 10_000;
const CHAT_CHANNEL_PREFIX = 'chat:';
const CHAT_TIMEOUT_PREFIX = 'chat:timeout:';
const CHAT_RATE_LIMIT_PREFIX = 'chat:ratelimit:';
const CHAT_USERNAME_CACHE_PREFIX = 'chat:username:';
const CHAT_MAX_CONTENT_LENGTH = 500;
const CHAT_RATE_LIMIT_WINDOW_MS = 60_000;
const CHAT_RATE_LIMIT_MAX_MESSAGES = 20;
const CHAT_USERNAME_CACHE_TTL_SECONDS = 300;

type ChatSendInboundMessage = {
  type: 'CHAT_SEND';
  content: string;
};

type DeleteMessageInboundMessage = {
  type: 'DELETE_MESSAGE';
  messageId: string;
};

type TimeoutUserInboundMessage = {
  type: 'TIMEOUT_USER';
  userId: string;
  durationSeconds: number;
};

type InboundMessage = ChatSendInboundMessage | DeleteMessageInboundMessage | TimeoutUserInboundMessage;

type UserRow = {
  username: string;
};

// ── Global store: Map<streamId, Set<WebSocket>> ──
const streamConnections = new Map<string, Set<WebSocket.WebSocket>>();
const streamSocketUsers = new Map<string, Map<WebSocket.WebSocket, string>>();
let redisSubListenerInitialized = false;

function getStreamConnections(streamId: string) {
  if (!streamConnections.has(streamId)) {
    streamConnections.set(streamId, new Set());
  }
  return streamConnections.get(streamId)!;
}

function getStreamSocketUsers(streamId: string): Map<WebSocket.WebSocket, string> {
  if (!streamSocketUsers.has(streamId)) {
    streamSocketUsers.set(streamId, new Map());
  }

  return streamSocketUsers.get(streamId)!;
}

function getChatChannel(streamId: string): string {
  return `${CHAT_CHANNEL_PREFIX}${streamId}`;
}

function getChatTimeoutKey(userId: string, streamId: string): string {
  return `${CHAT_TIMEOUT_PREFIX}${userId}:${streamId}`;
}

function getChatRateLimitKey(userId: string, streamId: string): string {
  return `${CHAT_RATE_LIMIT_PREFIX}${userId}:${streamId}`;
}

function getChatUsernameCacheKey(userId: string): string {
  return `${CHAT_USERNAME_CACHE_PREFIX}${userId}`;
}

async function resolveUsername(redis: ReturnType<typeof getRedisClient>, userId: string, fallbackUsername: string): Promise<string> {
  const cacheKey = getChatUsernameCacheKey(userId);

  const cachedUsername = await redis.get(cacheKey);
  if (cachedUsername) {
    return cachedUsername;
  }

  try {
    const userResult = await query<UserRow>(`
      SELECT username
      FROM users
      WHERE id = $1
      LIMIT 1
    `, [userId]);

    const resolvedUsername = userResult.rows[0]?.username ?? fallbackUsername;
    await redis.set(cacheKey, resolvedUsername, 'EX', CHAT_USERNAME_CACHE_TTL_SECONDS);
    return resolvedUsername;
  } catch (error) {
    logger.warn('Failed to resolve username from PostgreSQL, using JWT fallback', {
      userId,
      message: error instanceof Error ? error.message : String(error),
    });
    return fallbackUsername;
  }
}

function parseInboundMessage(data: Buffer): InboundMessage | null {
  try {
    const parsed = JSON.parse(data.toString()) as {
      type?: unknown;
      content?: unknown;
      messageId?: unknown;
      userId?: unknown;
      durationSeconds?: unknown;
    };

    if (parsed.type === 'CHAT_SEND') {
      if (typeof parsed.content !== 'string') {
        return null;
      }

      return {
        type: 'CHAT_SEND',
        content: parsed.content,
      };
    }

    if (parsed.type === 'DELETE_MESSAGE') {
      if (typeof parsed.messageId !== 'string' || !parsed.messageId.trim()) {
        return null;
      }

      return {
        type: 'DELETE_MESSAGE',
        messageId: parsed.messageId,
      };
    }

    if (parsed.type === 'TIMEOUT_USER') {
      if (typeof parsed.userId !== 'string' || !parsed.userId.trim()) {
        return null;
      }

      if (typeof parsed.durationSeconds !== 'number' || !Number.isFinite(parsed.durationSeconds)) {
        return null;
      }

      return {
        type: 'TIMEOUT_USER',
        userId: parsed.userId,
        durationSeconds: Math.floor(parsed.durationSeconds),
      };
    }

    return null;
  } catch {
    return null;
  }
}

function isModeratorRole(role: string): boolean {
  const normalizedRole = role.trim().toLowerCase();
  return normalizedRole === 'moderator' || normalizedRole === 'admin';
}

function isUserTimeoutMessage(data: unknown): data is { type: 'USER_TIMEOUT'; userId: string; durationSeconds: number } {
  if (!data || typeof data !== 'object') {
    return false;
  }

  const maybeTimeout = data as { type?: unknown; userId?: unknown; durationSeconds?: unknown };
  return maybeTimeout.type === 'USER_TIMEOUT'
    && typeof maybeTimeout.userId === 'string'
    && typeof maybeTimeout.durationSeconds === 'number';
}

function parsePubSubMessage(message: string): unknown {
  try {
    return JSON.parse(message);
  } catch {
    return message;
  }
}

function sendWsError(socket: WebSocket.WebSocket, code: string, message: string): void {
  if (socket.readyState !== socket.OPEN) {
    return;
  }

  try {
    socket.send(JSON.stringify({ type: 'ERROR', code, message }));
  } catch {
    // Ignore send failures for error responses.
  }
}

function getStreamIdFromChannel(channel: string): string | null {
  if (!channel.startsWith(CHAT_CHANNEL_PREFIX)) {
    return null;
  }

  return channel.slice(CHAT_CHANNEL_PREFIX.length);
}

function initializeRedisSubListener(): void {
  if (redisSubListenerInitialized) {
    return;
  }

  const redisSub = getRedisSubClient();
  redisSub.on('message', (channel, message) => {
    const streamId = getStreamIdFromChannel(channel);
    if (!streamId) {
      return;
    }

    const connections = streamConnections.get(streamId);
    if (!connections || connections.size === 0) {
      return;
    }

    const parsedMessage = parsePubSubMessage(message);
    if (isUserTimeoutMessage(parsedMessage)) {
      const socketUsers = streamSocketUsers.get(streamId);
      if (socketUsers) {
        for (const [ws, socketUserId] of socketUsers.entries()) {
          if (socketUserId === parsedMessage.userId && ws.readyState === ws.OPEN) {
            ws.close(4003, 'User is timed out from chat');
          }
        }
      }

      return;
    }

    for (const ws of connections) {
      if (ws.readyState !== ws.OPEN) {
        continue;
      }

      try {
        ws.send(message);
      } catch (error) {
        logger.warn('Failed forwarding Redis chat message to websocket client', {
          streamId,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  });

  redisSubListenerInitialized = true;
}

export default async function chatWebSocketRoutes(app: FastifyInstance): Promise<void> {
  initializeRedisSubListener();

  app.get<{ Params: StreamIdParams; Querystring: { token?: string } }>(
    '/ws/chat/:streamId',
    { websocket: true },
    async (socket, request: FastifyRequest<{ Params: StreamIdParams; Querystring: { token?: string } }>) => {
      const { streamId } = request.params;
      const channel = getChatChannel(streamId);
      let userId: string | null = null;
      let usernameFromJwt = 'unknown';
      let roleFromJwt = 'viewer';

      try {
        // ── Extract and validate JWT from Authorization header ──
        const authHeader = request.headers.authorization;
        const tokenFromHeader = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;
        const tokenFromQuery = request.query.token?.trim() || null;
        const token = tokenFromHeader ?? tokenFromQuery;

        if (!token) {
          logger.warn('WebSocket chat connection rejected: missing or invalid Authorization header', {
            streamId,
            remoteAddress: request.ip,
          });
          socket.close(4001, 'Missing or invalid Authorization header');
          return;
        }

        let payload: JwtPayload;

        try {
          payload = await app.jwt.verify(token) as JwtPayload;
          userId = payload.sub;
          usernameFromJwt = payload.username;
          roleFromJwt = payload.role;
        } catch (jwtError) {
          logger.warn('WebSocket chat connection rejected: invalid JWT', {
            streamId,
            remoteAddress: request.ip,
            reason: jwtError instanceof Error ? jwtError.message : String(jwtError),
          });
          socket.close(4001, 'Invalid or expired token');
          return;
        }

        // ── Check Redis for chat timeout ──
        const redis = getRedisClient();
        const timeoutKey = getChatTimeoutKey(userId, streamId);
        const isTimedOut = await redis.exists(timeoutKey);

        if (isTimedOut) {
          logger.warn('WebSocket chat connection rejected: user timed out', {
            streamId,
            userId,
            remoteAddress: request.ip,
          });
          socket.close(4003, 'User is timed out from chat');
          return;
        }

        // ── Store the connection locally ──
        const connections = getStreamConnections(streamId);
        const socketUsers = getStreamSocketUsers(streamId);
        const shouldSubscribe = connections.size === 0;
        connections.add(socket);
        socketUsers.set(socket, userId);

        if (shouldSubscribe) {
          const redisSub = getRedisSubClient();
          await redisSub.subscribe(channel);
          logger.info('Subscribed Redis chat channel for stream', {
            streamId,
            channel,
          });
        }

        let pingInterval: NodeJS.Timeout | null = null;
        let pongTimeout: NodeJS.Timeout | null = null;
        let isClosed = false;

        const clearTimers = () => {
          if (pingInterval) {
            clearInterval(pingInterval);
            pingInterval = null;
          }

          if (pongTimeout) {
            clearTimeout(pongTimeout);
            pongTimeout = null;
          }
        };

        const cleanupConnection = () => {
          if (isClosed) {
            return;
          }
          isClosed = true;

          clearTimers();
          connections.delete(socket);
          socketUsers.delete(socket);

          if (connections.size === 0) {
            streamConnections.delete(streamId);
            streamSocketUsers.delete(streamId);

            const redisSub = getRedisSubClient();
            void redisSub.unsubscribe(channel)
              .then(() => {
                logger.info('Unsubscribed Redis chat channel for stream', {
                  streamId,
                  channel,
                });
              })
              .catch((error) => {
                logger.error('Failed to unsubscribe Redis chat channel for stream', {
                  streamId,
                  channel,
                  message: error instanceof Error ? error.message : String(error),
                });
              });
          }
        };

        const schedulePongTimeout = () => {
          if (pongTimeout) {
            clearTimeout(pongTimeout);
          }

          pongTimeout = setTimeout(() => {
            logger.warn('WebSocket chat connection closing: pong timeout', {
              streamId,
              userId,
              remoteAddress: request.ip,
            });
            socket.close(4000, 'Pong timeout');
          }, PONG_TIMEOUT_MS);
        };

        pingInterval = setInterval(() => {
          if (socket.readyState !== socket.OPEN) {
            return;
          }

          schedulePongTimeout();

          try {
            socket.ping();
          } catch (error) {
            logger.warn('WebSocket chat ping failed', {
              streamId,
              userId,
              remoteAddress: request.ip,
              reason: error instanceof Error ? error.message : String(error),
            });
            socket.close(1011, 'Ping failed');
          }
        }, PING_INTERVAL_MS);

        socket.on('pong', () => {
          if (pongTimeout) {
            clearTimeout(pongTimeout);
            pongTimeout = null;
          }
        });

        socket.on('message', (data: Buffer) => {
          if (!userId) {
            return;
          }

          const inbound = parseInboundMessage(data);
          if (!inbound) {
            sendWsError(socket, 'INVALID_MESSAGE', 'Expected type=CHAT_SEND|DELETE_MESSAGE|TIMEOUT_USER with valid payload');
            return;
          }

          const isModeratorCommand = inbound.type === 'DELETE_MESSAGE' || inbound.type === 'TIMEOUT_USER';
          if (isModeratorCommand && !isModeratorRole(roleFromJwt)) {
            sendWsError(socket, 'FORBIDDEN', 'Only moderators can use moderation commands');
            return;
          }

          if (inbound.type === 'DELETE_MESSAGE') {
            void (async () => {
              try {
                const deletedAt = new Date().toISOString();

                await query(
                  `
                    UPDATE chat_messages
                    SET deleted_at = $1
                    WHERE id = $2 AND stream_id = $3 AND deleted_at IS NULL
                  `,
                  [deletedAt, inbound.messageId, streamId]
                );

                const redisPublisher = getRedisPublisherClient();
                await redisPublisher.publish(channel, JSON.stringify({
                  type: 'CHAT_DELETE',
                  messageId: inbound.messageId,
                }));
              } catch (error) {
                logger.error('Failed to process DELETE_MESSAGE command', {
                  streamId,
                  moderatorUserId: userId,
                  messageId: inbound.messageId,
                  message: error instanceof Error ? error.message : String(error),
                });
                sendWsError(socket, 'INTERNAL_ERROR', 'Failed to delete chat message');
              }
            })();

            return;
          }

          if (inbound.type === 'TIMEOUT_USER') {
            void (async () => {
              try {
                if (inbound.durationSeconds <= 0) {
                  sendWsError(socket, 'INVALID_TIMEOUT', 'durationSeconds must be greater than 0');
                  return;
                }

                const timeoutKeyForTarget = getChatTimeoutKey(inbound.userId, streamId);
                await redis.set(timeoutKeyForTarget, '1', 'EX', inbound.durationSeconds);

                const redisPublisher = getRedisPublisherClient();
                await redisPublisher.publish(channel, JSON.stringify({
                  type: 'USER_TIMEOUT',
                  userId: inbound.userId,
                  durationSeconds: inbound.durationSeconds,
                }));
              } catch (error) {
                logger.error('Failed to process TIMEOUT_USER command', {
                  streamId,
                  moderatorUserId: userId,
                  targetUserId: inbound.userId,
                  durationSeconds: inbound.durationSeconds,
                  message: error instanceof Error ? error.message : String(error),
                });
                sendWsError(socket, 'INTERNAL_ERROR', 'Failed to timeout user');
              }
            })();

            return;
          }

          if (inbound.content.length > CHAT_MAX_CONTENT_LENGTH) {
            sendWsError(socket, 'CONTENT_TOO_LONG', `content exceeds ${CHAT_MAX_CONTENT_LENGTH} characters`);
            return;
          }

          const content = inbound.content.trim();
          if (!content) {
            sendWsError(socket, 'CONTENT_EMPTY', 'content cannot be empty');
            return;
          }

          void (async () => {
            try {
              const timeoutKeyForMessage = getChatTimeoutKey(userId, streamId);
              const timedOutBeforeSend = await redis.exists(timeoutKeyForMessage);
              if (timedOutBeforeSend) {
                sendWsError(socket, 'CHAT_TIMEOUT', 'You are timed out from chat');
                return;
              }

              const now = Date.now();
              const windowStart = now - CHAT_RATE_LIMIT_WINDOW_MS;
              const rateLimitKey = getChatRateLimitKey(userId, streamId);

              const rateLimitResult = await redis
                .multi()
                .zadd(rateLimitKey, now, now.toString())
                .zremrangebyscore(rateLimitKey, 0, windowStart)
                .zcard(rateLimitKey)
                .expire(rateLimitKey, Math.ceil(CHAT_RATE_LIMIT_WINDOW_MS / 1000))
                .exec();

              const countReply = rateLimitResult?.[2]?.[1];
              const messageCount = typeof countReply === 'number'
                ? countReply
                : Number.parseInt(String(countReply ?? '0'), 10);

              if (!Number.isFinite(messageCount)) {
                sendWsError(socket, 'RATE_LIMIT_ERROR', 'Failed to evaluate chat rate limit');
                return;
              }

              if (messageCount > CHAT_RATE_LIMIT_MAX_MESSAGES) {
                sendWsError(socket, 'RATE_LIMITED', 'Too many chat messages, please slow down');
                return;
              }

              const redisPublisher = getRedisPublisherClient();
              const username = await resolveUsername(redis, userId, usernameFromJwt);
              const messageId = randomUUID();
              const timestamp = new Date(now).toISOString();
              const outboundMessage = {
                type: 'CHAT_MESSAGE',
                messageId,
                userId,
                username,
                content,
                timestamp,
              };

              await redisPublisher.publish(channel, JSON.stringify(outboundMessage));

              // Fire and forget: emit chat event to Kafka.
              void publishChatEvent({
                eventType: 'CHAT_SENT',
                messageId,
                streamId,
                userId,
                username,
                message: content,
                timestamp,
              }).catch((error) => {
                logger.error('Failed to publish chat event to Kafka', {
                  streamId,
                  userId,
                  messageId,
                  message: error instanceof Error ? error.message : String(error),
                });
              });

              // Fire and forget: persist chat message for history.
              void query(
                `
                  INSERT INTO chat_messages (id, stream_id, user_id, username, content, created_at)
                  VALUES ($1, $2, $3, $4, $5, $6)
                `,
                [messageId, streamId, userId, username, content, timestamp]
              ).catch((error) => {
                logger.error('Failed to persist chat message', {
                  streamId,
                  userId,
                  messageId,
                  message: error instanceof Error ? error.message : String(error),
                });
              });

              logger.debug('Published chat message to Redis', {
                streamId,
                userId,
                channel,
                messageSize: content.length,
              });
            } catch (error) {
              logger.error('Error publishing chat message', {
                streamId,
                userId,
                message: error instanceof Error ? error.message : String(error),
              });
              sendWsError(socket, 'INTERNAL_ERROR', 'Failed to process chat message');
            }
          })();
        });

        logger.info('WebSocket chat client connected', {
          streamId,
          userId,
          remoteAddress: request.ip,
          totalConnections: connections.size,
        });

        socket.on('close', () => {
          cleanupConnection();
          logger.info('WebSocket chat client disconnected', {
            streamId,
            userId,
            remoteAddress: request.ip,
            remainingConnections: connections.size,
          });
        });

        socket.on('error', (error: Error) => {
          logger.error('WebSocket chat connection error', {
            streamId,
            userId,
            remoteAddress: request.ip,
            message: error.message,
            stack: error.stack,
          });
        });
      } catch (error) {
        const existingConnections = streamConnections.get(streamId);
        if (existingConnections?.has(socket)) {
          existingConnections.delete(socket);
          const existingSocketUsers = streamSocketUsers.get(streamId);
          existingSocketUsers?.delete(socket);
          if (existingConnections.size === 0) {
            streamConnections.delete(streamId);
            streamSocketUsers.delete(streamId);
          }
        }

        logger.error('Unexpected error in WebSocket chat connection handler', {
          streamId,
          remoteAddress: request.ip,
          message: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        });
        socket.close(1011, 'Unexpected server error');
      }
    }
  );
}