-- Migration: add description and category to ingest streams, remove stream_key_hash

ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS description TEXT,
  ADD COLUMN IF NOT EXISTS category TEXT;

ALTER TABLE streams
  DROP COLUMN IF EXISTS stream_key_hash;