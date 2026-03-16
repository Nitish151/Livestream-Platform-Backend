import { Kafka, Partitioners, type Producer } from 'kafkajs';
import logger from '../utils/logger.js';

export type ViewerEventType = 'STREAM_JOIN' | 'STREAM_LEAVE' | 'VIEWER_PING';

export interface ViewerEventMessage {
  eventType: ViewerEventType;
  streamId: string;
  userId: string;
  timestamp: string;
  traceId: string;
  sessionId: string;
  ip: string;
  userAgent: string;
  rendition: string;
  watchDurationSeconds: number;
}

function getViewerEventsTopic(): string {
  return process.env.VIEWER_EVENTS_TOPIC ?? 'viewer-events';
}

let producer: Producer | null = null;

export async function initializeProducer(): Promise<Producer> {
  if (producer) return producer;
  const viewerEventsTopic = getViewerEventsTopic();

  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);

  const kafka = new Kafka({
    clientId: 'stream-api-service',
    brokers,
  });

  const admin = kafka.admin();
  await admin.connect();

  try {
    const existingTopics = new Set(await admin.listTopics());
    if (!existingTopics.has(viewerEventsTopic)) {
      await admin.createTopics({
        topics: [{
          topic: viewerEventsTopic,
          numPartitions: 1,
          replicationFactor: 1,
        }],
        waitForLeaders: true,
      });

      logger.info('Kafka topic created', { topic: viewerEventsTopic });
    }
  } finally {
    await admin.disconnect();
  }

  producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    idempotent: true,
    allowAutoTopicCreation: true,
  });

  await producer.connect();
  logger.info('Kafka producer connected', { brokers, topic: viewerEventsTopic });

  return producer;
}

export function getProducer(): Producer {
  if (!producer) {
    throw new Error('Kafka producer not initialized. Call initializeProducer() first.');
  }

  return producer;
}

export async function closeProducer(): Promise<void> {
  if (!producer) {
    return;
  }

  await producer.disconnect();
  producer = null;
  logger.info('Kafka producer disconnected');
}

export async function publishViewerEvent(event: ViewerEventMessage): Promise<void> {
  const viewerEventsTopic = getViewerEventsTopic();

  await getProducer().send({
    topic: viewerEventsTopic,
    messages: [{ key: event.streamId, value: JSON.stringify(event) }],
    acks: -1,
  });
}
