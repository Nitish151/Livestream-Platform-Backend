import { describe, it, expect, vi, beforeEach } from 'vitest';
import { AppError } from '../utils/errors.js';

/* ─── mock dependencies BEFORE importing the module under test ─── */

// Mock pg (query helper)
vi.mock('../db/index', () => ({ query: vi.fn() }));

// Mock redis — shared set mock so the same reference is used everywhere
const mockRedisSet = vi.fn().mockResolvedValue('OK');
const mockRedisGet = vi.fn();
vi.mock('../redis/index', () => ({
  getRedisClient: () => ({ set: mockRedisSet, get: mockRedisGet }),
}));

// Mock logger (silence output in tests)
vi.mock('../utils/logger', () => ({
  default: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

// Mock bcrypt so tests run fast (no real hashing)
vi.mock('bcrypt', () => ({
  default: {
    hash: vi.fn(async (pw: string) => `hashed:${pw}`),
    compare: vi.fn(async (pw: string, hash: string) => hash === `hashed:${pw}`),
  },
}));

// Import after mocks are set up
import { query } from '../db/index.js';
import {
  registerUser,
  loginUser,
  storeRefreshToken,
  getRefreshTokenUserId,
  generateStreamKey,
  resetStreamKey,
} from './auth.service.js';

const mockQuery = vi.mocked(query);

/* ────────────────────────────────────────────── */
/*  registerUser                                  */
/* ────────────────────────────────────────────── */
describe('registerUser', () => {
  beforeEach(() => vi.clearAllMocks());

  it('should register a valid user and return userId', async () => {
    mockQuery.mockResolvedValueOnce({
      rows: [{ id: 'uuid-123' }],
      rowCount: 1,
    });

    const result = await registerUser('testuser', 'test@example.com', 'password123');

    expect(result).toEqual({ userId: 'uuid-123' });
    expect(mockQuery).toHaveBeenCalledOnce();
    expect(mockQuery.mock.calls[0][1]).toContain('hashed:password123');
  });

  it('should throw 400 for short username', async () => {
    await expect(registerUser('ab', 'test@example.com', 'password123'))
      .rejects.toThrow(AppError);
    await expect(registerUser('ab', 'test@example.com', 'password123'))
      .rejects.toHaveProperty('statusCode', 400);
  });

  it('should throw 400 for invalid email', async () => {
    await expect(registerUser('testuser', 'not-an-email', 'password123'))
      .rejects.toHaveProperty('statusCode', 400);
  });

  it('should throw 400 for short password', async () => {
    await expect(registerUser('testuser', 'test@example.com', 'short'))
      .rejects.toHaveProperty('statusCode', 400);
  });

  it('should throw 409 on duplicate username', async () => {
    mockQuery.mockRejectedValueOnce(
      Object.assign(new Error('duplicate'), {
        code: '23505',
        detail: 'Key (username)=(testuser) already exists.',
      }),
    );

    await expect(registerUser('testuser', 'test@example.com', 'password123'))
      .rejects.toHaveProperty('statusCode', 409);
  });

  it('should throw 409 on duplicate email', async () => {
    mockQuery.mockRejectedValueOnce(
      Object.assign(new Error('duplicate'), {
        code: '23505',
        detail: 'Key (email)=(test@example.com) already exists.',
      }),
    );

    await expect(registerUser('testuser', 'test@example.com', 'password123'))
      .rejects.toHaveProperty('statusCode', 409);
  });
});

/* ────────────────────────────────────────────── */
/*  loginUser                                     */
/* ────────────────────────────────────────────── */
describe('loginUser', () => {
  beforeEach(() => vi.clearAllMocks());

  const validUser = {
    id: 'uuid-123',
    username: 'testuser',
    email: 'test@example.com',
    role: 'viewer',
    is_banned: false,
    password_hash: 'hashed:password123',
  };

  it('should return user data on valid credentials', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [validUser], rowCount: 1 });

    const user = await loginUser('test@example.com', 'password123');

    expect(user).toEqual({
      id: 'uuid-123',
      username: 'testuser',
      email: 'test@example.com',
      role: 'viewer',
    });
  });

  it('should throw 401 for non-existent user', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

    await expect(loginUser('ghost@example.com', 'password123'))
      .rejects.toHaveProperty('statusCode', 401);
  });

  it('should throw 401 for wrong password', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [validUser], rowCount: 1 });

    await expect(loginUser('test@example.com', 'wrongpassword'))
      .rejects.toHaveProperty('statusCode', 401);
  });

  it('should throw 401 for banned user', async () => {
    mockQuery.mockResolvedValueOnce({
      rows: [{ ...validUser, is_banned: true }],
      rowCount: 1,
    });

    await expect(loginUser('test@example.com', 'password123'))
      .rejects.toHaveProperty('statusCode', 401);
  });

  it('should throw 400 when email or password is missing', async () => {
    await expect(loginUser('', 'password123'))
      .rejects.toHaveProperty('statusCode', 400);
    await expect(loginUser('test@example.com', ''))
      .rejects.toHaveProperty('statusCode', 400);
  });
});

/* ────────────────────────────────────────────── */
/*  stream key management                        */
/* ────────────────────────────────────────────── */

describe('generateStreamKey / resetStreamKey', () => {
  beforeEach(() => vi.clearAllMocks());

  it('should generate and store stream key for existing user', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [{ id: 'uuid-123' }], rowCount: 1 });

    const streamKey = await generateStreamKey('uuid-123');

    expect(streamKey).toMatch(/^[0-9a-f]+$/);
    expect(streamKey.length).toBe(48);
    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining('UPDATE users'),
      expect.any(Array),
    );
    // we can't inspect hashed value exactly, but the plain text should not match hashed prefix
    expect(streamKey).not.toContain('hashed:');
  });

  it('should throw 400 when userId is empty', async () => {
    // @ts-expect-error test invalid input
    await expect(generateStreamKey('')).rejects.toHaveProperty('statusCode', 400);
  });

  it('should throw 404 when user not found', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

    await expect(generateStreamKey('missing-id')).rejects.toHaveProperty('statusCode', 404);
  });

  it('resetStreamKey should call generateStreamKey', async () => {
    mockQuery.mockResolvedValueOnce({ rows: [{ id: 'uuid-123' }], rowCount: 1 });

    const streamKey = await resetStreamKey('uuid-123');

    expect(streamKey).toMatch(/^[0-9a-f]+$/);
  });
});

/* ────────────────────────────────────────────── */
/*  storeRefreshToken                             */
/* ────────────────────────────────────────────── */
describe('storeRefreshToken', () => {
  beforeEach(() => vi.clearAllMocks());

  it('should store token in Redis with 7-day TTL', async () => {
    await storeRefreshToken('token-abc', 'uuid-123');

    expect(mockRedisSet).toHaveBeenCalledWith(
      'session:refresh:token-abc',
      'uuid-123',
      { EX: 604_800 },
    );
  });
});

/* ────────────────────────────────────────────── */
/*  getRefreshTokenUserId                         */
/* ────────────────────────────────────────────── */
describe('getRefreshTokenUserId', () => {
  beforeEach(() => vi.clearAllMocks());

  it('should return userId for a valid refresh token', async () => {
    mockRedisGet.mockResolvedValueOnce('uuid-123');

    const userId = await getRefreshTokenUserId('valid-token');

    expect(userId).toBe('uuid-123');
    expect(mockRedisGet).toHaveBeenCalledWith('session:refresh:valid-token');
  });

  it('should throw 401 for expired / non-existent token', async () => {
    mockRedisGet.mockResolvedValueOnce(null);

    await expect(getRefreshTokenUserId('bad-token'))
      .rejects.toHaveProperty('statusCode', 401);
  });

  it('should throw 400 when token is empty', async () => {
    await expect(getRefreshTokenUserId(''))
      .rejects.toHaveProperty('statusCode', 400);
  });
});
