-- Index on tickr_history.at so the janitor's PurgeHistory pass scans by
-- time without falling back to a sequential scan as the table grows.
CREATE INDEX tickr_history_at_idx ON tickr_history (at);
