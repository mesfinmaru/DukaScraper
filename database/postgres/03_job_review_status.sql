-- Enables truthful terminal states for generic page validation.
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped', 'needs_review'));
