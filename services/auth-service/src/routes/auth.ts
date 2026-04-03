import type { FastifyInstance } from 'fastify';
import {
	handleLogin,
	handleRegister,
	handleRefresh,
	handleValidate,
	handleGenerateStreamKey,
	handleResetStreamKey,
} from '../controllers/auth.controller.js';

export default async function authRoutes(fastify: FastifyInstance): Promise<void> {
	fastify.post('/register', handleRegister(fastify));
	fastify.post('/login', handleLogin(fastify));
	fastify.post('/refresh', handleRefresh(fastify));
	fastify.get('/validate', handleValidate(fastify));
	fastify.post('/stream-key', handleGenerateStreamKey(fastify));
	fastify.post('/stream-key/reset', handleResetStreamKey(fastify));
}
