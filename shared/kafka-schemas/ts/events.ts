export type StreamLifecycleEventType =
  | 'STREAM_STARTED'
  | 'STREAM_ENDED'
  | 'STREAM_FAILED';

export type ViewerEventType =
  | 'STREAM_JOIN'
  | 'STREAM_LEAVE'
  | 'VIEWER_PING'
  | 'QUALITY_CHANGE';

export type Rendition = '1080p' | '720p' | '480p' | '360p';

export type ChatEventType = 'CHAT_SENT' | 'CHAT_DELETED' | 'CHAT_MODERATED';

export interface StreamLifecycleMetadata {
  title: string;
  category: string;
  tags: string[];
}

export interface StreamLifecycleEvent {
  eventType: StreamLifecycleEventType;
  streamId: string;
  userId: string;
  timestamp: string; // ISO-8601
  traceId: string;
  metadata: StreamLifecycleMetadata;
}

export interface TranscodingJob {
  eventType: 'TRANSCODING_JOB_REQUESTED' | 'TRANSCODING_JOB_RETRY' | 'TRANSCODING_JOB_CANCELLED';
  streamId: string;
  userId: string;
  timestamp: string; // ISO-8601
  traceId: string;
  jobId: string;
  rtmpUrl: string;
  requestedAt: string; // ISO-8601
}

export interface ViewerEvent {
  eventType: ViewerEventType;
  streamId: string;
  userId: string;
  timestamp: string; // ISO-8601
  traceId: string;
  sessionId: string;
  ip: string;
  countryCode?: string;
  region?: string;
  userAgent: string;
  rendition: Rendition;
  watchDurationSeconds: number;
}

export interface ChatEvent {
  eventType: ChatEventType;
  streamId: string;
  userId: string;
  timestamp: string; // ISO-8601
  traceId: string;
  messageId: string;
  sessionId: string;
  message: string;
}
