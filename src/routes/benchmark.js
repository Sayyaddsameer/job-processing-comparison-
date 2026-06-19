'use strict';

const fs = require('fs');
const path = require('path');
const express = require('express');
const { pool } = require('../config/database');
const { createJob } = require('../lib/job-repository');

const POLL_INTERVAL_MS = 5000;
const MAX_WAIT_MS = 30 * 60 * 1000; // 30 minutes

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/**
 * Wait for every job id in `ids` to reach a terminal status.
 * Resolves when all are DONE or FAILED, or rejects on timeout.
 */
function waitForCompletion(ids) {
  return new Promise((resolve, reject) => {
    const start = Date.now();

    const check = async () => {
      try {
        const { rows } = await pool.query(
          `SELECT COUNT(*) FROM jobs WHERE id = ANY($1) AND status NOT IN ('DONE', 'FAILED')`,
          [ids],
        );

        const pending = parseInt(rows[0].count, 10);

        if (pending === 0) {
          return resolve();
        }

        if (Date.now() - start > MAX_WAIT_MS) {
          return reject(new Error('Benchmark timed out waiting for jobs to complete'));
        }

        setTimeout(check, POLL_INTERVAL_MS);
      } catch (err) {
        reject(err);
      }
    };

    check();
  });
}

/**
 * Compute latency and throughput stats for a set of job rows.
 */
function computeStats(jobs) {
  const latencies = [];

  for (const job of jobs) {
    if (job.started_at != null && job.submitted_at != null) {
      const latency = Number(job.started_at) - Number(job.submitted_at);
      latencies.push(latency);
    }
  }

  if (latencies.length === 0) {
    return {
      avg_latency_ms: 0,
      p95_latency_ms: 0,
      total_throughput_jobs_per_min: 0,
    };
  }

  latencies.sort((a, b) => a - b);

  const sum = latencies.reduce((acc, v) => acc + v, 0);
  const avgLatency = sum / latencies.length;
  const p95Index = Math.ceil(0.95 * latencies.length) - 1;
  const p95Latency = latencies[p95Index];

  // Throughput: completed jobs per minute over the entire window.
  const completedJobs = jobs.filter((j) => j.status === 'DONE');
  let throughput = 0;

  if (completedJobs.length > 0) {
    const timestamps = jobs.map((j) => Number(j.submitted_at));
    const completedTimestamps = completedJobs.map((j) => Number(j.completed_at));

    const minSubmitted = Math.min(...timestamps);
    const maxCompleted = Math.max(...completedTimestamps);
    const totalElapsedMinutes = (maxCompleted - minSubmitted) / 60000;

    if (totalElapsedMinutes > 0) {
      throughput = completedJobs.length / totalElapsedMinutes;
    }
  }

  return {
    avg_latency_ms: Math.round(avgLatency * 100) / 100,
    p95_latency_ms: p95Latency,
    total_throughput_jobs_per_min: Math.round(throughput * 100) / 100,
  };
}

// ---------------------------------------------------------------------------
// route factory
// ---------------------------------------------------------------------------

/**
 * @param {import('bullmq').Queue} exportQueue
 * @returns {express.Router}
 */
function createBenchmarkRouter(exportQueue) {
  const router = express.Router();

  router.post('/benchmark', async (req, res) => {
    try {
      const cronJobCount = req.body.cronJobs || 100;
      const queueJobCount = req.body.queueJobs || 100;
      const allIds = [];

      // ----- submit CRON jobs --------------------------------------------------
      for (let i = 0; i < cronJobCount; i++) {
        const job = await createJob(pool, { type: 'CRON', priority: 10 });
        allIds.push(job.id);
      }

      // ----- submit QUEUE jobs -------------------------------------------------
      for (let i = 0; i < queueJobCount; i++) {
        const job = await createJob(pool, { type: 'QUEUE', priority: 10 });
        allIds.push(job.id);

        await exportQueue.add(
          'export',
          {
            jobId: job.id,
            type: job.type,
            priority: job.priority,
            userId: job.user_id,
          },
          {
            priority: job.priority,
            attempts: 4,
            backoff: { type: 'exponential', delay: 5000 },
          },
        );
      }

      // ----- wait for all jobs to reach terminal status ------------------------
      await waitForCompletion(allIds);

      // ----- fetch final job records -------------------------------------------
      const { rows: allJobs } = await pool.query(
        'SELECT * FROM jobs WHERE id = ANY($1)',
        [allIds],
      );

      const cronJobs = allJobs.filter((j) => j.type === 'CRON');
      const queueJobs = allJobs.filter((j) => j.type === 'QUEUE');

      const results = {
        cron_stats: computeStats(cronJobs),
        queue_stats: computeStats(queueJobs),
      };

      // ----- persist results ---------------------------------------------------
      const outputDir = path.join(process.cwd(), 'output');
      fs.mkdirSync(outputDir, { recursive: true });
      fs.writeFileSync(
        path.join(outputDir, 'benchmarking.json'),
        JSON.stringify(results, null, 2),
      );

      const benchmarkingDir = path.join(process.cwd(), 'benchmarking');
      fs.mkdirSync(benchmarkingDir, { recursive: true });
      fs.writeFileSync(
        path.join(benchmarkingDir, 'results.json'),
        JSON.stringify(results, null, 2),
      );

      return res.status(200).json(results);
    } catch (err) {
      console.error('Benchmark failed:', err);
      return res.status(500).json({ error: err.message });
    }
  });

  return router;
}

module.exports = createBenchmarkRouter;
