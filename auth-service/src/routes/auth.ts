import jwt from '@fastify/jwt';
import type { FastifyInstance } from 'fastify';
import { ACCESS_TOKEN_TTL, getJwtPrivateKey, getJwtPublicKey } from '../config/auth.js';
import { loginController, registerController } from '../controllers/authController.js';
import type { LoginRequestBody, RegisterRequestBody } from '../services/authService.js';

export default async function authRoutes(fastify: FastifyInstance): Promise<void> {
	await fastify.register(jwt, {
		secret: {
			private: getJwtPrivateKey(),
			public: getJwtPublicKey(),
		},
		sign: {
			algorithm: 'RS256',
			expiresIn: ACCESS_TOKEN_TTL,
		},
	});

	fastify.post<{ Body: RegisterRequestBody }>('/register', registerController);
	fastify.post<{ Body: LoginRequestBody }>('/login', loginController);
}
