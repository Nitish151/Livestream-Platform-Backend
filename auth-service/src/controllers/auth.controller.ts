import crypto from 'node:crypto';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { registerUser, loginUser, storeRefreshToken, getRefreshTokenUserId } from '../services/auth.service.js';
import { query } from '../db/index.js';
import { AppError } from '../utils/errors.js';
import logger from '../utils/logger.js';

interface RegisterBody {
  username: string;
  email: string;
  password: string;
}

interface LoginBody {
  email: string;
  password: string;
}

interface RefreshBody {
  refreshToken: string;
}

/**
 * POST /register
 */
export function handleRegister(app: FastifyInstance) {
  return async (
    request: FastifyRequest<{ Body: RegisterBody }>,
    reply: FastifyReply,
  ) => {
    try {
      const { username, email, password } = request.body ?? {};
      const result = await registerUser(username, email, password);
      return reply.status(201).send(result);
    } catch (error) {
      return sendError(reply, error);
    }
  };
}

/**
 * POST /login
 */
export function handleLogin(app: FastifyInstance) {
  return async (
    request: FastifyRequest<{ Body: LoginBody }>,
    reply: FastifyReply,
  ) => {
    try {
      const { email, password } = request.body ?? {};
      const user = await loginUser(email, password);

      // Sign JWT access token (RS256, 1 hour)
      const accessToken = app.jwt.sign(
        { sub: user.id, username: user.username, role: user.role },
        { expiresIn: '1h' },
      );

      // Generate UUID refresh token and persist in Redis
      const refreshToken = crypto.randomUUID();
      await storeRefreshToken(refreshToken, user.id);

      return reply.status(200).send({ accessToken, refreshToken });
    } catch (error) {
      return sendError(reply, error);
    }
  };
}

/**
 * POST /refresh — validate refresh token, issue new access token
 */
export function handleRefresh(app: FastifyInstance) {
  return async (
    request: FastifyRequest<{ Body: RefreshBody }>,
    reply: FastifyReply,
  ) => {
    try {
      const { refreshToken } = request.body ?? {};
      const userId = await getRefreshTokenUserId(refreshToken);

      // Look up user to get current username + role for the new token
      const result = await query<{ username: string; role: string }>(
        'SELECT username, role FROM users WHERE id = $1',
        [userId],
      );

      const user = result.rows[0];
      if (!user) {
        throw new AppError('User no longer exists', 401, 'USER_NOT_FOUND');
      }

      const accessToken = app.jwt.sign(
        { sub: userId, username: user.username, role: user.role },
        { expiresIn: '1h' },
      );

      return reply.status(200).send({ accessToken });
    } catch (error) {
      return sendError(reply, error);
    }
  };
}

/**
 * GET /validate — verify JWT and return decoded payload (inter-service use)
 */
export function handleValidate(app: FastifyInstance) {
  return async (
    request: FastifyRequest,
    reply: FastifyReply,
  ) => {
    try {
      const authHeader = request.headers.authorization;
      if (!authHeader?.startsWith('Bearer ')) {
        throw new AppError('Missing or malformed Authorization header', 401, 'NO_TOKEN');
      }

      const token = authHeader.slice(7);
      const decoded = app.jwt.verify<{ sub: string; username: string; role: string }>(token);

      return reply.status(200).send({
        userId: decoded.sub,
        username: decoded.username,
        role: decoded.role,
      });
    } catch (error) {
      if (error instanceof AppError) {
        return sendError(reply, error);
      }
      // JWT verification errors (expired, invalid signature, etc.)
      return reply.status(401).send({ error: 'Invalid or expired token', code: 'INVALID_TOKEN' });
    }
  };
}

/* ── helpers ── */

function sendError(reply: FastifyReply, error: unknown) {
  if (error instanceof AppError) {
    return reply
      .status(error.statusCode)
      .send({ error: error.message, code: error.code });
  }

  logger.error('Unhandled controller error', {
    message: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  });

  return reply.status(500).send({ error: 'Internal server error' });
}
