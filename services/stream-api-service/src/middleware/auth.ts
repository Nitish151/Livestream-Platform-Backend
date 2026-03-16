import type { FastifyReply, FastifyRequest } from 'fastify';
import logger from '../utils/logger.js';

// ── JWT payload shape (matches auth-service token: sub / username / role) ──
export interface JwtPayload {
  sub: string;
  username: string;
  role: string;
  iat?: number;
  exp?: number;
}

// ── Extend Fastify request so routes can read the verified user ──
declare module 'fastify' {
  interface FastifyRequest {
    jwtUser: JwtPayload;
  }
}

// ── preHandler: verify Bearer token via @fastify/jwt (registered in app.ts) ──
export async function requireAuth(
  request: FastifyRequest,
  reply: FastifyReply,
): Promise<void> {
  try {
    await request.jwtVerify();
    // @fastify/jwt puts the decoded payload on request.user; mirror it to jwtUser
    // so the rest of the codebase uses the typed interface.
    request.jwtUser = request.user as JwtPayload;
  } catch (error) {
    logger.warn('JWT verification failed', {
      url: request.url,
      reason: error instanceof Error ? error.message : String(error),
    });
    reply.code(401).send({ message: 'Invalid or expired token' });
  }
}
