import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

export const BCRYPT_COST = 12;
export const REFRESH_TOKEN_TTL_SECONDS = 60 * 60 * 24 * 7;
export const DEFAULT_USER_ROLE = 'viewer';
export const ACCESS_TOKEN_TTL = '1h';

const DEFAULT_REDIS_URL = 'redis://localhost:6379';
const DEFAULT_JWT_PRIVATE_KEY_PATH = 'keys/private.pem';
const DEFAULT_JWT_PUBLIC_KEY_PATH = 'keys/public.pem';

let cachedJwtPrivateKey: string | null = null;
let cachedJwtPublicKey: string | null = null;

export function getRedisUrl(): string {
  return process.env.REDIS_URL ?? DEFAULT_REDIS_URL;
}

function readKeyFromEnvironmentOrFile(
  environmentKeyValue: string | undefined,
  configuredPath: string | undefined,
  defaultPath: string,
  label: string
): string {
  if (environmentKeyValue) {
    return environmentKeyValue.replace(/\\n/g, '\n');
  }

  const keyPath = configuredPath ?? defaultPath;
  const resolvedPath = resolve(process.cwd(), keyPath);

  if (!existsSync(resolvedPath)) {
    throw new Error(`${label} file not found at ${resolvedPath}`);
  }

  return readFileSync(resolvedPath, 'utf-8');
}

function assertPemKeyFormat(key: string, label: string, marker: string): string {
  const normalizedKey = key.trim();

  if (!normalizedKey.includes(marker)) {
    throw new Error(`${label} must be a PEM-encoded key containing ${marker}`);
  }

  return normalizedKey;
}

export function getJwtPrivateKey(): string {
  if (cachedJwtPrivateKey) {
    return cachedJwtPrivateKey;
  }

  cachedJwtPrivateKey = readKeyFromEnvironmentOrFile(
    process.env.JWT_PRIVATE_KEY,
    process.env.JWT_PRIVATE_KEY_PATH,
    DEFAULT_JWT_PRIVATE_KEY_PATH,
    'JWT private key'
  );
  cachedJwtPrivateKey = assertPemKeyFormat(
    cachedJwtPrivateKey,
    'JWT private key',
    'BEGIN PRIVATE KEY'
  );
  return cachedJwtPrivateKey;
}

export function getJwtPublicKey(): string {
  if (cachedJwtPublicKey) {
    return cachedJwtPublicKey;
  }

  cachedJwtPublicKey = readKeyFromEnvironmentOrFile(
    process.env.JWT_PUBLIC_KEY,
    process.env.JWT_PUBLIC_KEY_PATH,
    DEFAULT_JWT_PUBLIC_KEY_PATH,
    'JWT public key'
  );
  cachedJwtPublicKey = assertPemKeyFormat(
    cachedJwtPublicKey,
    'JWT public key',
    'BEGIN PUBLIC KEY'
  );
  return cachedJwtPublicKey;
}
