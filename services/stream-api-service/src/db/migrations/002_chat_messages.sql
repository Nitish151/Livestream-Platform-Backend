CREATE TABLE IF NOT EXISTS chat_messages (
  message_id UUID PRIMARY KEY,
  stream_id UUID NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  user_id UUID NOT NULL,
  username VARCHAR(255) NOT NULL,
  content TEXT NOT NULL CHECK (char_length(content) <= 500),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_stream_id_created_at
  ON chat_messages (stream_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_chat_messages_user_id_created_at
  ON chat_messages (user_id, created_at DESC);
