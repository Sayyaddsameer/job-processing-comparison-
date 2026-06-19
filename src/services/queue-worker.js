'use strict';

require('dotenv').config();

const { Worker } = require('bullmq');
const { config } = require('../config/index.js');
const { pool } = require('../config/database.js');
const { createRedisConnection } = require('../config/redis.js');
const { runMigrations } = require('../db/migrate.js');
const { simulateWork } = require('../lib/mock-reporter.js');
const {
  updateStartedAt,
  updateCompletedAt,
  updateAttempts,
  logExecution,
} = require('../lib/job-repository.js');

const QUEUE_NAME = 'export-jobs';

let worker = null;

/**
 * BullMQ processor function.
 * Receives a BullMQ Job whose data contains { jobId, type, priority, userId }.
 */
async function processJob(job) {
  const { jobId, priority } = job.data;

  console.log(`[QueueWorker] Processing job ${jobId} (priority: ${priority})`);

  const startedAt = Date.now();
  await updateStartedAt(pool, jobId, startedAt, config.workerId);

  const work = await simulateWork(config.failureRate);

  const completedAt = Date.now();
  await updateCompletedAt(pool, jobId, completedAt, 'DONE');
  await logExecution(pool, {
    jobId,
    workerId: config.workerId,
    startedAt: work.startedAt,
    completedAt: work.completedAt,
  });

  console.log(`[QueueWorker] Job ${jobId} completed in ${work.duration}ms`);

  return {
    jobId,
    startedAt: work.startedAt,
    completedAt: work.completedAt,
    duration: work.duration,
  };
}

/**
 * Graceful shutdown handler.
 */
async function shutdown(signal) {
  console.log(`[QueueWorker] Received ${signal}, shutting down...`);
  try {
    if (worker) {
      await worker.close();
      console.log('[QueueWorker] Worker closed');
    }
    await pool.end();
    console.log('[QueueWorker] Database pool closed');
    process.exit(0);
  } catch (err) {
    console.error('[QueueWorker] Error during shutdown:', err.message);
    process.exit(1);
  }
}

/**
 * Main entry point.
 */
async function main() {
  await runMigrations(pool);

  const connection = createRedisConnection();

  worker = new Worker(QUEUE_NAME, processJob, {
    connection,
    concurrency: config.workerConcurrency || 3,
    stalledInterval: 30000,
    maxStalledCount: 1,
    lockDuration: 60000,
  });

  worker.on('completed', (job) => {
    console.log(`[QueueWorker] Job ${job.data.jobId} marked as completed`);
  });

  worker.on('failed', async (job, err) => {
    console.error(`[QueueWorker] Job ${job.data.jobId} failed: ${err.message}`);
    try {
      if (job.attemptsMade >= job.opts.attempts) {
        console.log(
          `[QueueWorker] Job ${job.data.jobId} exhausted all ${job.opts.attempts} retries`
        );
        await updateCompletedAt(pool, job.data.jobId, Date.now(), 'FAILED');
        await updateAttempts(pool, job.data.jobId, job.attemptsMade);
      } else {
        await updateAttempts(pool, job.data.jobId, job.attemptsMade);
      }
    } catch (updateErr) {
      console.error(
        `[QueueWorker] Error updating job ${job.data.jobId} after failure:`,
        updateErr.message
      );
    }
  });

  worker.on('stalled', (jobId) => {
    console.warn(`[QueueWorker] WARNING: Job ${jobId} has stalled`);
  });

  worker.on('error', (err) => {
    console.error('[QueueWorker] Worker error:', err.message);
  });

  console.log('[QueueWorker] Starting queue worker');
  console.log(`[QueueWorker]   QUEUE            = ${QUEUE_NAME}`);
  console.log(`[QueueWorker]   WORKER_ID        = ${config.workerId}`);
  console.log(`[QueueWorker]   CONCURRENCY      = ${config.workerConcurrency || 3}`);
  console.log(`[QueueWorker]   FAILURE_RATE     = ${config.failureRate}`);

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('[QueueWorker] Fatal startup error:', err.message);
  process.exit(1);
});
