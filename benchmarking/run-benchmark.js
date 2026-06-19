'use strict';

require('dotenv').config();

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

const API_URL = process.env.API_URL || 'http://localhost:3000';
const CRON_JOB_COUNT = 100;
const QUEUE_JOB_COUNT = 100;
const POLL_INTERVAL_MS = 5000;
const MAX_WAIT_MS = 30 * 60 * 1000; // 30 minutes

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/**
 * Make an HTTP request and return the parsed JSON body.
 */
function request(method, url, body) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const transport = parsed.protocol === 'https:' ? https : http;

    const options = {
      hostname: parsed.hostname,
      port: parsed.port,
      path: parsed.pathname + parsed.search,
      method,
      headers: {},
    };

    let payload;
    if (body !== undefined) {
      payload = JSON.stringify(body);
      options.headers['Content-Type'] = 'application/json';
      options.headers['Content-Length'] = Buffer.byteLength(payload);
    }

    const req = transport.request(options, (res) => {
      const chunks = [];
      res.on('data', (chunk) => chunks.push(chunk));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString();
        try {
          resolve({ statusCode: res.statusCode, body: JSON.parse(raw) });
        } catch (_err) {
          resolve({ statusCode: res.statusCode, body: raw });
        }
      });
    });

    req.on('error', reject);

    if (payload) {
      req.write(payload);
    }
    req.end();
  });
}

function postJson(url, body) {
  return request('POST', url, body);
}

function getJson(url) {
  return request('GET', url);
}

// ---------------------------------------------------------------------------
// stats computation
// ---------------------------------------------------------------------------

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

  const completedJobs = jobs.filter((j) => j.status === 'DONE');
  let throughput = 0;

  if (completedJobs.length > 0) {
    const timestamps = jobs.map((j) => Number(j.submitted_at));
    const completedTimestamps = completedJobs.map(
      (j) => Number(j.completed_at),
    );

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
// polling helper
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Wait until every job in `jobIds` has a terminal status (DONE or FAILED).
 * Polls the /api/jobs endpoint repeatedly.
 */
async function waitForAllJobs(jobIds) {
  const idSet = new Set(jobIds);
  const start = Date.now();

  while (true) {
    const cronRes = await getJson(`${API_URL}/api/jobs?type=CRON`);
    const queueRes = await getJson(`${API_URL}/api/jobs?type=QUEUE`);

    const allJobs = [
      ...(cronRes.body.jobs || []),
      ...(queueRes.body.jobs || []),
    ];

    const tracked = allJobs.filter((j) => idSet.has(j.id));
    const pending = tracked.filter(
      (j) => j.status !== 'DONE' && j.status !== 'FAILED',
    );

    console.log(
      `  Polling: ${tracked.length - pending.length}/${idSet.size} jobs completed`,
    );

    if (pending.length === 0 && tracked.length === idSet.size) {
      return tracked;
    }

    if (Date.now() - start > MAX_WAIT_MS) {
      throw new Error('Timed out waiting for jobs to complete');
    }

    await sleep(POLL_INTERVAL_MS);
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

async function main() {
  console.log('=== Benchmark Start ===');
  console.log(`API URL: ${API_URL}`);
  console.log(
    `Submitting ${CRON_JOB_COUNT} CRON jobs and ${QUEUE_JOB_COUNT} QUEUE jobs`,
  );
  console.log();

  const jobIds = [];

  // ----- submit CRON jobs ----------------------------------------------------
  console.log(`Submitting ${CRON_JOB_COUNT} CRON jobs...`);
  for (let i = 0; i < CRON_JOB_COUNT; i++) {
    const res = await postJson(`${API_URL}/api/export`, {
      type: 'CRON',
      priority: 10,
    });

    if (res.statusCode !== 201) {
      throw new Error(`Failed to create CRON job: ${JSON.stringify(res.body)}`);
    }
    jobIds.push(res.body.job_id);
  }
  console.log(`  ${CRON_JOB_COUNT} CRON jobs submitted`);

  // ----- submit QUEUE jobs ---------------------------------------------------
  console.log(`Submitting ${QUEUE_JOB_COUNT} QUEUE jobs...`);
  for (let i = 0; i < QUEUE_JOB_COUNT; i++) {
    const res = await postJson(`${API_URL}/api/export`, {
      type: 'QUEUE',
      priority: 10,
    });

    if (res.statusCode !== 201) {
      throw new Error(`Failed to create QUEUE job: ${JSON.stringify(res.body)}`);
    }
    jobIds.push(res.body.job_id);
  }
  console.log(`  ${QUEUE_JOB_COUNT} QUEUE jobs submitted`);
  console.log();

  // ----- poll for completion -------------------------------------------------
  console.log('Waiting for all jobs to complete...');
  const completedJobs = await waitForAllJobs(jobIds);
  console.log();

  // ----- calculate stats -----------------------------------------------------
  const cronJobs = completedJobs.filter((j) => j.type === 'CRON');
  const queueJobs = completedJobs.filter((j) => j.type === 'QUEUE');

  const results = {
    cron_stats: computeStats(cronJobs),
    queue_stats: computeStats(queueJobs),
  };

  // ----- persist results -----------------------------------------------------
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

  // ----- print results -------------------------------------------------------
  console.log('=== Results ===');
  console.log();

  const header = [
    'Metric'.padEnd(30),
    'CRON'.padStart(15),
    'QUEUE'.padStart(15),
  ].join(' | ');

  const separator = '-'.repeat(header.length);

  console.log(header);
  console.log(separator);

  const metrics = [
    ['Avg Latency (ms)', 'avg_latency_ms'],
    ['P95 Latency (ms)', 'p95_latency_ms'],
    ['Throughput (jobs/min)', 'total_throughput_jobs_per_min'],
  ];

  for (const [label, key] of metrics) {
    const cronVal = String(results.cron_stats[key]);
    const queueVal = String(results.queue_stats[key]);
    console.log(
      [label.padEnd(30), cronVal.padStart(15), queueVal.padStart(15)].join(
        ' | ',
      ),
    );
  }

  console.log(separator);
  console.log();

  const cronDone = cronJobs.filter((j) => j.status === 'DONE').length;
  const cronFailed = cronJobs.filter((j) => j.status === 'FAILED').length;
  const queueDone = queueJobs.filter((j) => j.status === 'DONE').length;
  const queueFailed = queueJobs.filter((j) => j.status === 'FAILED').length;

  console.log(`CRON:  ${cronDone} done, ${cronFailed} failed (${cronJobs.length} total)`);
  console.log(`QUEUE: ${queueDone} done, ${queueFailed} failed (${queueJobs.length} total)`);
  console.log();
  console.log(`Results written to:`);
  console.log(`  ${path.join(outputDir, 'benchmarking.json')}`);
  console.log(`  ${path.join(benchmarkingDir, 'results.json')}`);
  console.log();
  console.log('=== Benchmark Complete ===');
}

main().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
