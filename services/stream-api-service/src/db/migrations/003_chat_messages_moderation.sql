-- Ensure chat_messages matches moderation requirements.
-- Supports environments where an older schema used message_id instead of id.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'chat_messages' AND column_name = 'message_id'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'chat_messages' AND column_name = 'id'
  ) THEN
    ALTER TABLE chat_messages RENAME COLUMN message_id TO id;
  END IF;
END $$;

ALTER TABLE chat_messages
  ADD COLUMN IF NOT EXISTS id UUID,
  ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_messages_id
  ON chat_messages (id);
