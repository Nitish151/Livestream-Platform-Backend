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
  countryCode?: string;
  region?: string;
  userAgent: string;
  rendition: string;
  watchDurationSeconds: number;
}

export interface ChatEventMessage {
  eventType: 'CHAT_SENT';
  messageId: string;
  streamId: string;
  userId: string;
  username: string;
  message: string;
  timestamp: string;
}

function getViewerEventsTopic(): string {
  return process.env.VIEWER_EVENTS_TOPIC ?? 'viewer-events';
}

function getChatEventsTopic(): string {
  return process.env.CHAT_EVENTS_TOPIC ?? 'chat-events';
}

let producer: Producer | null = null;

export async function initializeProducer(): Promise<Producer> {
  if (producer) return producer;
  const viewerEventsTopic = getViewerEventsTopic();
  const chatEventsTopic = getChatEventsTopic();

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
    const topicsToCreate = [viewerEventsTopic, chatEventsTopic].filter((topic) => !existingTopics.has(topic));

    if (topicsToCreate.length > 0) {
      await admin.createTopics({
        topics: topicsToCreate.map((topic) => ({
          topic,
          numPartitions: 1,
          replicationFactor: 1,
        })),
        waitForLeaders: true,
      });

      logger.info('Kafka topics created', { topics: topicsToCreate });
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

export async function publishChatEvent(event: ChatEventMessage): Promise<void> {
  const chatEventsTopic = getChatEventsTopic();

  await getProducer().send({
    topic: chatEventsTopic,
    messages: [{ key: event.streamId, value: JSON.stringify(event) }],
    acks: -1,
  });
}
