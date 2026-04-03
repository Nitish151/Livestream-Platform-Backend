import 'fastify';
import type { FastifyReply, FastifyRequest } from 'fastify';

interface JwtPayload {
  sub: string;
  username: string;
  role: string;
  iat?: number;
  exp?: number;
}

declare module 'fastify' {
  interface FastifyInstance {
    authenticate(request: FastifyRequest, reply: FastifyReply): Promise<void>;
  }

  interface FastifyRequest {
    jwtUser: JwtPayload;
  }
}
