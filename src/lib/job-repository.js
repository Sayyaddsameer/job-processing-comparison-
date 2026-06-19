'use strict';

/**
 * Database CRUD operations for the jobs and execution_logs tables.
 * Every function takes a pool or client as its first argument so callers
 * can choose between connection-pool queries and transaction-bound queries.
 */

async function createJob(pool, { type, priority, userId }) {
  const result = await pool.query(
    `INSERT INTO jobs (id, type, priority, status, user_id, submitted_at)
     VALUES (gen_random_uuid(), $1, $2, 'PENDING', $3, $4)
     RETURNING *`,
    [type, priority, userId, Date.now()]
  );
  return result.rows[0];
}

async function getPendingCronJobs(pool) {
  const result = await pool.query(
    `SELECT * FROM jobs
     WHERE type = 'CRON' AND status = 'PENDING'
     ORDER BY priority ASC, submitted_at ASC`
  );
  return result.rows;
}

async function markActive(clientOrPool, jobId, workerId) {
  const result = await clientOrPool.query(
    `UPDATE jobs
     SET status = 'ACTIVE', started_at = $1, worker_id = $2, attempts = attempts + 1
     WHERE id = $3 AND status = 'PENDING'
     RETURNING *`,
    [Date.now(), workerId, jobId]
  );
  return result.rows[0] || null;
}

async function markDone(clientOrPool, jobId) {
  await clientOrPool.query(
    `UPDATE jobs SET status = 'DONE', completed_at = $1 WHERE id = $2`,
    [Date.now(), jobId]
  );
}

async function markFailed(clientOrPool, jobId) {
  await clientOrPool.query(
    `UPDATE jobs SET status = 'FAILED', completed_at = $1 WHERE id = $2`,
    [Date.now(), jobId]
  );
}

async function updateAttempts(pool, jobId, attempts) {
  await pool.query(
    `UPDATE jobs SET attempts = $1 WHERE id = $2`,
    [attempts, jobId]
  );
}

async function updateStartedAt(pool, jobId, startedAt, workerId) {
  await pool.query(
    `UPDATE jobs SET started_at = $1, worker_id = $2, status = 'ACTIVE' WHERE id = $3`,
    [startedAt, workerId, jobId]
  );
}

async function updateCompletedAt(pool, jobId, completedAt, status) {
  await pool.query(
    `UPDATE jobs SET completed_at = $1, status = $2 WHERE id = $3`,
    [completedAt, status, jobId]
  );
}

async function logExecution(clientOrPool, { jobId, workerId, startedAt, completedAt }) {
  await clientOrPool.query(
    `INSERT INTO execution_logs (job_id, worker_id, started_at, completed_at)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (job_id, worker_id) DO UPDATE SET completed_at = $4`,
    [jobId, workerId, startedAt, completedAt]
  );
}

async function getJobById(pool, jobId) {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE id = $1`,
    [jobId]
  );
  return result.rows[0] || null;
}

async function getJobsByType(pool, type) {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE type = $1 ORDER BY submitted_at ASC`,
    [type]
  );
  return result.rows;
}

async function getCompletedJobsByType(pool, type) {
  const result = await pool.query(
    `SELECT * FROM jobs WHERE type = $1 AND status IN ('DONE', 'FAILED') ORDER BY submitted_at ASC`,
    [type]
  );
  return result.rows;
}

module.exports = {
  createJob,
  getPendingCronJobs,
  markActive,
  markDone,
  markFailed,
  updateAttempts,
  updateStartedAt,
  updateCompletedAt,
  logExecution,
  getJobById,
  getJobsByType,
  getCompletedJobsByType,
};
