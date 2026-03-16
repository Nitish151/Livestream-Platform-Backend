import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Readable } from 'node:stream';

const endpoint = process.env.MINIO_ENDPOINT;
const accessKeyId = process.env.MINIO_ACCESS_KEY;
const secretAccessKey = process.env.MINIO_SECRET_KEY;
const region = process.env.MINIO_REGION ?? 'us-east-1';

if (!endpoint || !accessKeyId || !secretAccessKey) {
  throw new Error('Missing MinIO configuration. Expected MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.');
}

export const SEGMENTS_BUCKET = 'lspb-segments';

export const s3 = new S3Client({
  endpoint,
  region,
  credentials: {
    accessKeyId,
    secretAccessKey,
  },
  forcePathStyle: true,
});

async function getObjectBodyBuffer(body: unknown, bucket: string, key: string): Promise<Buffer> {
  if (!body) {
    throw new Error(`Object ${bucket}/${key} has no body.`);
  }

  if (Buffer.isBuffer(body)) {
    return body;
  }

  if (typeof body === 'string') {
    return Buffer.from(body);
  }

  if (body instanceof Uint8Array) {
    return Buffer.from(body);
  }

  if (body instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of body) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }

  if (typeof Blob !== 'undefined' && body instanceof Blob) {
    return Buffer.from(await body.arrayBuffer());
  }

  throw new Error(`Unsupported body type for ${bucket}/${key}.`);
}

export async function getObjectText(bucket: string, key: string): Promise<string> {
  const result = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const bodyBuffer = await getObjectBodyBuffer(result.Body, bucket, key);
  return bodyBuffer.toString('utf-8');
}

export async function getObjectBuffer(bucket: string, key: string): Promise<Buffer> {
  const result = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  return getObjectBodyBuffer(result.Body, bucket, key);
}
