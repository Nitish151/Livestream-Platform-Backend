-- Add optional description and category columns to the shared streams table.
-- Idempotent: ADD COLUMN IF NOT EXISTS is safe to re-run.
ALTER TABLE streams ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE streams ADD COLUMN IF NOT EXISTS category    TEXT;
