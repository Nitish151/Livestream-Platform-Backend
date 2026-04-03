import crypto from 'node:crypto';
import bcrypt from 'bcrypt';
import { query } from '../db/index.js';
import { getRedisClient } from '../redis/index.js';
import { AppError } from '../utils/errors.js';
import logger from '../utils/logger.js';

/* ── constants ── */
const BCRYPT_ROUNDS = 12;
const REFRESH_TOKEN_TTL = 604_800; // 7 days in seconds

/* ── validation helpers ── */
const USERNAME_RE = /^[a-zA-Z0-9_]{3,30}$/;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const MIN_PASSWORD_LENGTH = 8;

function validateRegisterInput(username: string, email: string, password: string): void {
  const errors: string[] = [];

  if (!username || !USERNAME_RE.test(username)) {
    errors.push('Username must be 3-30 characters (alphanumeric and underscores only)');
  }
  if (!email || !EMAIL_RE.test(email)) {
    errors.push('A valid email is required');
  }
  if (!password || password.length < MIN_PASSWORD_LENGTH) {
    errors.push(`Password must be at least ${MIN_PASSWORD_LENGTH} characters`);
  }

  if (errors.length > 0) {
    throw new AppError(errors.join('; '), 400, 'VALIDATION_ERROR');
  }
}

/* ── types ── */
interface UserRow {
  id: string;
  username: string;
  email: string;
  role: string;
  is_banned: boolean;
  password_hash: string;
}

/* ── public API ── */

/**
 * Register a new user.
 * Returns the newly created user's id.
 */
export async function registerUser(
  username: string,
  email: string,
  password: string,
): Promise<{ userId: string }> {
  validateRegisterInput(username, email, password);

  const passwordHash = await bcrypt.hash(password, BCRYPT_ROUNDS);

  try {
    const result = await query<{ id: string }>(
      `INSERT INTO users (username, email, password_hash)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [username, email, passwordHash],
    );

    logger.info('User registered', { userId: result.rows[0].id, username });
    return { userId: result.rows[0].id };
  } catch (error: any) {
    // PostgreSQL unique-violation code
    if (error?.code === '23505') {
      const detail: string = error.detail ?? '';
      const field = detail.includes('username') ? 'Username' : 'Email';
      throw new AppError(`${field} already exists`, 409, 'DUPLICATE_ENTRY');
    }
    throw error;
  }
}

/**
 * Authenticate a user by email + password.
 * Returns the user profile (without password_hash).
 */
export async function loginUser(
  email: string,
  password: string,
): Promise<{ id: string; username: string; email: string; role: string }> {
  if (!email || !password) {
    throw new AppError('Email and password are required', 400, 'VALIDATION_ERROR');
  }

  const result = await query<UserRow>(
    `SELECT id, username, email, role, is_banned, password_hash
     FROM users
     WHERE email = $1`,
    [email],
  );

  const user = result.rows[0];

  if (!user) {
    throw new AppError('Invalid email or password', 401, 'INVALID_CREDENTIALS');
  }

  if (user.is_banned) {
    throw new AppError('Account is banned', 401, 'ACCOUNT_BANNED');
  }

  const match = await bcrypt.compare(password, user.password_hash);
  if (!match) {
    throw new AppError('Invalid email or password', 401, 'INVALID_CREDENTIALS');
  }

  return {
    id: user.id,
    username: user.username,
    email: user.email,
    role: user.role,
  };
}

/**
 * Generate a new stream key for the user, store bcrypt hash, and return plaintext key.
 * The value is returned once and not stored in plaintext.
 */
export async function generateStreamKey(userId: string): Promise<string> {
  if (!userId) {
    throw new AppError('User ID is required', 400, 'VALIDATION_ERROR');
  }

  const streamKey = crypto.randomBytes(24).toString('hex');
  const streamKeyHash = await bcrypt.hash(streamKey, BCRYPT_ROUNDS);

  const result = await query(
    `UPDATE users
     SET stream_key_hash = $1
     WHERE id = $2
     RETURNING id`,
    [streamKeyHash, userId],
  );

  if (result.rowCount === 0) {
    throw new AppError('User not found', 404, 'USER_NOT_FOUND');
  }

  logger.info('Stream key generated', { userId });
  return streamKey;
}

/**
 * Reset stream key (same behavior as generateStreamKey for now).
 */
export async function resetStreamKey(userId: string): Promise<string> {
  return generateStreamKey(userId);
}

/**
 * Persist a refresh token in Redis with a 7-day TTL.
 * Key pattern: session:refresh:{token}
 */
export async function storeRefreshToken(token: string, userId: string): Promise<void> {
  const redis = getRedisClient();
  await redis.set(`session:refresh:${token}`, userId, { EX: REFRESH_TOKEN_TTL });
  logger.debug('Refresh token stored', { userId });
}

/**
 * Validate a refresh token from Redis.
 * Returns the userId if valid, throws 401 otherwise.
 */
export async function getRefreshTokenUserId(token: string): Promise<string> {
  if (!token) {
    throw new AppError('Refresh token is required', 400, 'VALIDATION_ERROR');
  }

  const redis = getRedisClient();
  const userId = await redis.get(`session:refresh:${token}`);

  if (!userId) {
    throw new AppError('Invalid or expired refresh token', 401, 'INVALID_REFRESH_TOKEN');
  }

  return userId;
}
