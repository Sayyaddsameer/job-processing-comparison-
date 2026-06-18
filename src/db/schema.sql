CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(10) NOT NULL CHECK (type IN ('CRON', 'QUEUE')),
    priority INT NOT NULL DEFAULT 10 CHECK (priority BETWEEN 1 AND 10),
    status VARCHAR(10) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ACTIVE', 'DONE', 'FAILED')),
    user_id VARCHAR(255),
    submitted_at BIGINT NOT NULL,
    started_at BIGINT,
    completed_at BIGINT,
    worker_id VARCHAR(255),
    attempts INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobs_type_status ON jobs (type, status);
CREATE INDEX IF NOT EXISTS idx_jobs_type_status_priority ON jobs (type, status, priority);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);

CREATE TABLE IF NOT EXISTS execution_logs (
    id SERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id VARCHAR(255) NOT NULL,
    started_at BIGINT NOT NULL,
    completed_at BIGINT,
    CONSTRAINT uq_execution_log UNIQUE (job_id, worker_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_logs_job_id ON execution_logs (job_id);
