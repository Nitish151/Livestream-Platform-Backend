import type { FastifyInstance } from 'fastify';
import { handleLogin, handleRegister } from '../controllers/auth.controller.js';

export default async function authRoutes(fastify: FastifyInstance): Promise<void> {
	fastify.post('/register', handleRegister(fastify));
	fastify.post('/login', handleLogin(fastify));
}
