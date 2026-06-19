'use strict';

require('dotenv').config();

const { config } = require('../config/index.js');
const { pool, getClient } = require('../config/database.js');
const { runMigrations } = require('../db/migrate.js');
const { simulateWork } = require('../lib/mock-reporter.js');
const { tryAcquire } = require('../lib/advisory-lock.js');
const {
  getPendingCronJobs,
  markActive,
  markDone,
  markFailed,
  logExecution,
} = require('../lib/job-repository.js');

let intervalHandle = null;

/**
 * Process a single job using a dedicated client within a transaction,
 * guarded by a PostgreSQL advisory lock.
 */
async function processJobWithLock(job) {
  const client = await getClient();
  try {
    await client.query('BEGIN');

    const acquired = await tryAcquire(client, job.id);
    if (!acquired) {
      console.log(`[CronWorker] Could not acquire lock for job ${job.id}, skipping`);
      await client.query('ROLLBACK');
      return;
    }

    const updated = await markActive(client, job.id, config.workerId);
    if (!updated) {
      console.log(`[CronWorker] Job ${job.id} already claimed by another worker, skipping`);
      await client.query('ROLLBACK');
      return;
    }

    console.log(`[CronWorker] Executing job ${job.id} (priority: ${job.priority})`);
    const work = await simulateWork(0);

    await markDone(client, job.id);
    await logExecution(client, {
      jobId: job.id,
      workerId: config.workerId,
      startedAt: work.startedAt,
      completedAt: work.completedAt,
    });

    await client.query('COMMIT');
    console.log(`[CronWorker] Job ${job.id} completed in ${work.duration}ms`);
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(`[CronWorker] Error processing job ${job.id}:`, err.message);
    try {
      await markFailed(pool, job.id);
    } catch (failErr) {
      console.error(`[CronWorker] Failed to mark job ${job.id} as FAILED:`, failErr.message);
    }
  } finally {
    client.release();
  }
}

/**
 * Process a single job without advisory locking.
 * Uses the connection pool directly for all queries.
 */
async function processJobWithoutLock(job) {
  try {
    const updated = await markActive(pool, job.id, config.workerId);
    if (!updated) {
      console.log(`[CronWorker] Job ${job.id} already claimed by another worker, skipping`);
      return;
    }

    console.log(`[CronWorker] Executing job ${job.id} (priority: ${job.priority})`);
    const work = await simulateWork(0);

    await markDone(pool, job.id);
    await logExecution(pool, {
      jobId: job.id,
      workerId: config.workerId,
      startedAt: work.startedAt,
      completedAt: work.completedAt,
    });

    console.log(`[CronWorker] Job ${job.id} completed in ${work.duration}ms`);
  } catch (err) {
    console.error(`[CronWorker] Error processing job ${job.id}:`, err.message);
    try {
      await markFailed(pool, job.id);
    } catch (failErr) {
      console.error(`[CronWorker] Failed to mark job ${job.id} as FAILED:`, failErr.message);
    }
  }
}

/**
 * Poll for pending CRON jobs and process them sequentially.
 */
async function processPendingJobs() {
  console.log('[CronWorker] Polling for pending CRON jobs...');

  const jobs = await getPendingCronJobs(pool);
  if (jobs.length === 0) {
    console.log('[CronWorker] No pending CRON jobs found');
    return;
  }

  console.log(`[CronWorker] Found ${jobs.length} pending CRON job(s)`);

  for (const job of jobs) {
    if (config.advisoryLockEnabled) {
      await processJobWithLock(job);
    } else {
      await processJobWithoutLock(job);
    }
  }
}

/**
 * Graceful shutdown handler.
 */
function shutdown(signal) {
  console.log(`[CronWorker] Received ${signal}, shutting down...`);
  if (intervalHandle) {
    clearInterval(intervalHandle);
    intervalHandle = null;
  }
  pool.end()
    .then(() => {
      console.log('[CronWorker] Database pool closed');
      process.exit(0);
    })
    .catch((err) => {
      console.error('[CronWorker] Error closing pool:', err.message);
      process.exit(1);
    });
}

/**
 * Main entry point.
 */
async function main() {
  await runMigrations(pool);

  console.log('[CronWorker] Starting cron worker');
  console.log(`[CronWorker]   CRON_INTERVAL_MS     = ${config.cronIntervalMs}`);
  console.log(`[CronWorker]   WORKER_ID            = ${config.workerId}`);
  console.log(`[CronWorker]   ADVISORY_LOCK_ENABLED = ${config.advisoryLockEnabled}`);

  intervalHandle = setInterval(() => {
    processPendingJobs().catch((err) => {
      console.error('[CronWorker] Unhandled error during poll cycle:', err.message);
    });
  }, config.cronIntervalMs);

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('[CronWorker] Fatal startup error:', err.message);
  process.exit(1);
});
