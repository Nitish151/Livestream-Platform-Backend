import client from 'prom-client';

export const metricsRegistry = new client.Registry();

client.collectDefaultMetrics({
  register: metricsRegistry,
  prefix: 'stream_api_',
});

export const websocketConnectionsActive = new client.Gauge({
  name: 'websocket_connections_active',
  help: 'Number of active websocket chat connections',
  registers: [metricsRegistry],
});

export const chatMessagesPerSecond = new client.Counter({
  name: 'chat_messages_per_second',
  help: 'Total number of chat messages sent',
  labelNames: ['streamId'],
  registers: [metricsRegistry],
});

export const httpRequestDurationMs = new client.Histogram({
  name: 'http_request_duration_ms',
  help: 'HTTP request duration in milliseconds',
  labelNames: ['route', 'status'],
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000],
  registers: [metricsRegistry],
});
