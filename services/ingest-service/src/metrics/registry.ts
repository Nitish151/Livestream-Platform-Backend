import client from 'prom-client';

export const metricsRegistry = new client.Registry();

client.collectDefaultMetrics({
  register: metricsRegistry,
  prefix: 'ingest_service_',
});

export const rtmpConnectionsActive = new client.Gauge({
  name: 'rtmp_connections_active',
  help: 'Current number of active RTMP publishing connections',
  registers: [metricsRegistry],
});

export const httpRequestDurationMs = new client.Histogram({
  name: 'http_request_duration_ms',
  help: 'HTTP request duration in milliseconds',
  labelNames: ['route', 'status'],
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2000],
  registers: [metricsRegistry],
});
