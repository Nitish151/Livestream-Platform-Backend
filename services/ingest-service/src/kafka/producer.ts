import { Kafka, Producer, Partitioners } from 'kafkajs';
import logger from '../utils/logger.js';

export interface StreamStartedEvent {
  eventType: 'STREAM_STARTED';
  streamId: string;
  userId: string;
  title: string;
  startedAt: string;
}

export interface StreamEndedEvent {
  eventType: 'STREAM_ENDED';
  streamId: string;
  endedAt: string;
}

export interface TranscodingJob {
  eventType: 'TRANSCODING_JOB_REQUESTED';
  streamId: string;
  userId: string;
  timestamp: string;
  traceId: string;
  jobId: string;
  rtmpUrl: string;
  requestedAt: string;
}

const STREAM_LIFECYCLE_TOPIC = 'stream-lifecycle';
const TRANSCODING_JOBS_TOPIC = 'transcoding-jobs';
const KAFKA_TOPICS = [STREAM_LIFECYCLE_TOPIC, TRANSCODING_JOBS_TOPIC];

let producer: Producer | null = null;

export async function initializeProducer(): Promise<Producer> {
  if (producer) return producer;

  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);

  const kafka = new Kafka({
    clientId: 'ingest-service',
    brokers,
  });

  const admin = kafka.admin();
  await admin.connect();
  const existingTopics = new Set(await admin.listTopics());
  const missingTopics = KAFKA_TOPICS.filter((topic) => !existingTopics.has(topic));

  if (missingTopics.length > 0) {
    await admin.createTopics({
      topics: missingTopics.map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1,
      })),
      waitForLeaders: true,
    });
  }
  await admin.disconnect();
  logger.info('Kafka topics verified', { topics: KAFKA_TOPICS, created: missingTopics });

  producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    idempotent: true,
    allowAutoTopicCreation: true,
  });

  await producer.connect();
  logger.info('Kafka producer connected', { brokers });

  return producer;
}

export function getProducer(): Producer {
  if (!producer) throw new Error('Kafka producer not initialized. Call initializeProducer() first.');
  return producer;
}

export async function closeProducer(): Promise<void> {
  if (producer) {
    await producer.disconnect();
    producer = null;
    logger.info('Kafka producer disconnected');
  }
}

export async function publishStreamStarted(event: StreamStartedEvent): Promise<void> {
  await getProducer().send({
    topic: STREAM_LIFECYCLE_TOPIC,
    messages: [{ key: event.streamId, value: JSON.stringify(event) }],
    acks: -1, // all
  });
  logger.info('Kafka: published STREAM_STARTED', { streamId: event.streamId });
}

export async function publishStreamEnded(event: StreamEndedEvent): Promise<void> {
  await getProducer().send({
    topic: STREAM_LIFECYCLE_TOPIC,
    messages: [{ key: event.streamId, value: JSON.stringify(event) }],
    acks: -1, // all
  });
  logger.info('Kafka: published STREAM_ENDED', { streamId: event.streamId });
}

export async function publishTranscodingJob(job: TranscodingJob): Promise<void> {
  await getProducer().send({
    topic: TRANSCODING_JOBS_TOPIC,
    messages: [{ key: job.streamId, value: JSON.stringify(job) }],
    acks: -1, // all
  });
  logger.info('Kafka: published TranscodingJob', {
    streamId: job.streamId,
    jobId: job.jobId,
    rtmpUrl: job.rtmpUrl,
  });
}
