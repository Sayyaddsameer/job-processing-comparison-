# Job Processing Comparison System

A benchmarking system that compares two fundamental approaches to async job processing: traditional Cron-based polling and Redis-backed queue workers (BullMQ). The system implements distributed locking with PostgreSQL advisory locks, task priority handling, worker crash recovery, and produces measurable performance data.

## Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running the Benchmark](#running-the-benchmark)
- [How It Works](#how-it-works)
- [Worker Crash Recovery](#worker-crash-recovery)
- [Performance Analysis](#performance-analysis)
- [API Reference](#api-reference)
- [Project Structure](#project-structure)

## Architecture

```
                    +-------------------+
                    |  Client / Load    |
                    |    Generator      |
                    +--------+----------+
                             |
                         HTTP POST
                             |
                    +--------v----------+
                    |     Web API       |
                    |   (Express.js)    |
                    +---+----------+----+
                        |          |
              Insert Row|          |Push Job
                        |          |
             +----------v--+   +---v-----------+
             |  PostgreSQL  |   |    Redis       |
             |   (jobs DB)  |   | (BullMQ Queue) |
             +------+-------+   +---+-------+---+
                    |               |       |
         Poll every |          Pop Job  Pop Job
           10s      |               |       |
          +---------v---+    +------v--+ +--v-------+
          | Cron Worker  |   | Queue   | | Queue    |
          | (sequential) |   | Worker 1| | Worker 2 |
          +--------------+   +---------+ +----------+
```

The Web API acts as the dispatcher. For Cron jobs, it only writes to the database. For Queue jobs, it writes to both the database and Redis. Workers process jobs from their respective sources and update the database with timing data used for benchmarking.

## Quick Start

### Docker (Recommended)

1. Copy the environment file:
   ```bash
   cp .env.example .env
   ```

2. Start all services:
   ```bash
   docker-compose up -d
   ```

3. Verify everything is healthy:
   ```bash
   docker-compose ps
   ```

4. Check the API:
   ```bash
   curl http://localhost:3000/health
   ```

5. Open the Bull-Board dashboard:
   ```
   http://localhost:3000/admin/queues
   ```

### Local Development

Requires PostgreSQL and Redis running locally.

1. Copy and configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your local Postgres and Redis URLs
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Run database migrations:
   ```bash
   npm run migrate
   ```

4. Start each service in separate terminals:
   ```bash
   npm run start:api
   npm run start:cron
   npm run start:queue-worker
   ```

## Configuration

All configuration is managed through environment variables. See `.env.example` for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://jobuser:jobpassword@localhost:5432/jobcompare` | PostgreSQL connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `PORT` | `3000` | API server port |
| `CRON_INTERVAL_MS` | `10000` | How often the cron worker polls for pending jobs (ms) |
| `WORKER_CONCURRENCY` | `3` | Number of concurrent jobs per queue worker |
| `WORKER_ID` | `worker-<pid>` | Unique identifier for each worker instance |
| `ADVISORY_LOCK_ENABLED` | `true` | Toggle PostgreSQL advisory locks for the cron worker |
| `FAILURE_RATE` | `0.2` | Probability of simulated failure in queue workers (0-1) |

## Running the Benchmark

### Option 1: Standalone Script

After all services are running:

```bash
npm run benchmark
```

Or inside Docker:

```bash
docker-compose exec api node benchmarking/run-benchmark.js
```

This submits 100 CRON and 100 QUEUE jobs, waits for completion, then writes results to `output/benchmarking.json`.

### Option 2: API Endpoint

```bash
curl -X POST http://localhost:3000/api/benchmark \
  -H "Content-Type: application/json" \
  -d '{"cronJobs": 100, "queueJobs": 100}'
```

Results are returned in the response body and also persisted to `output/benchmarking.json` and `benchmarking/results.json`.

## How It Works

### Cron Pathway

The cron worker wakes up every `CRON_INTERVAL_MS` milliseconds and queries the `jobs` table for rows where `type = 'CRON'` and `status = 'PENDING'`. It processes them sequentially, one by one.

When advisory locking is enabled, each job is processed within a database transaction. The worker calls `pg_try_advisory_xact_lock(key1, key2)` using two 32-bit integers derived from the job UUID. If the lock is already held by another cron instance, the job is skipped. This prevents double-processing without the overhead of row-level locking on large tables.

The key limitation is fixed-interval latency. A job submitted one second after the cron fires must wait up to 9 more seconds before being discovered.

### Queue Pathway

When a QUEUE job is submitted, the API inserts a row into the database and simultaneously pushes a message to the `export-jobs` BullMQ queue in Redis. Worker processes monitor this queue and pull jobs within milliseconds of arrival.

Each queue worker handles up to 3 jobs concurrently (configurable). BullMQ provides built-in support for:
- **Priority**: Lower number = higher priority. A priority-1 job is processed before any waiting priority-10 jobs.
- **Retries**: On failure, jobs are retried up to 3 times with exponential backoff (5s, 10s, 20s).
- **Stall detection**: If a worker crashes mid-processing, BullMQ detects the stall after 30s and re-enqueues the job.

### Simulated Work

Both pathways use the same mock reporter that sleeps for a random duration between 2 and 10 seconds to simulate report generation. The queue pathway has an additional 20% random failure rate to exercise the retry logic.

## Worker Crash Recovery

### Queue Workers (Automatic Recovery)

1. Start the system: `docker-compose up -d`
2. Submit some QUEUE jobs
3. While jobs are processing, kill a worker:
   ```bash
   docker-compose stop queue-worker-1
   ```
4. Observe in the Bull-Board dashboard (`/admin/queues`) that active jobs on the killed worker become stalled after ~30s
5. BullMQ automatically moves stalled jobs back to the waiting state
6. The remaining worker (`queue-worker-2`) picks them up
7. Restart the killed worker: `docker-compose start queue-worker-1`

### Cron Workers (Manual Intervention Required)

If a cron worker crashes while processing a job, that job remains in `ACTIVE` status indefinitely. There is no built-in mechanism to detect and recover from this. This is one of the key disadvantages of the cron approach for user-facing workloads.

With advisory locking enabled, at least the crash does not corrupt data -- the lock is released when the database session ends, and no other worker has processed the same job.

## Performance Analysis

### Why Queue Outperforms Cron

**Latency**: The most significant difference is in task discovery latency (`time_to_start`). Cron workers poll at fixed intervals, meaning jobs wait an average of `CRON_INTERVAL_MS / 2` before being discovered. With a 10-second interval, the average discovery latency is ~5 seconds. Queue workers, by contrast, receive jobs via Redis pub/sub within milliseconds. On an idle system, task discovery latency is typically under 100ms.

**Throughput**: Queue workers process jobs concurrently (3 per worker by default, across 2 workers = 6 concurrent jobs). The cron worker processes jobs sequentially. With simulated work of 2-10 seconds per job, the cron worker can complete roughly 6-30 jobs per minute, while the queue cluster can handle 36-180 jobs per minute.

**Reliability**: Queue workers get automatic retry with backoff. Crashed workers have their jobs recovered via stall detection. Cron workers have neither unless you build it yourself.

### When to Use Cron Instead

Cron remains the right choice for:
- Maintenance tasks where latency is irrelevant (daily database backups, log cleanup)
- Environments where adding Redis infrastructure is not justified
- Simple scheduled tasks with no concurrency requirements
- Batch processing that should run at specific calendar times

## API Reference

### POST /api/export

Create a new job.

**Request:**
```json
{
  "type": "CRON | QUEUE",
  "priority": 1-10,
  "user_id": "optional-string"
}
```

**Response (201):**
```json
{
  "job_id": "uuid",
  "status": "PENDING",
  "type": "CRON | QUEUE"
}
```

### GET /api/jobs

List all jobs. Supports query parameters `type` and `status`.

### GET /api/jobs/:id

Get a specific job by UUID.

### POST /api/benchmark

Run the full benchmark. Accepts optional `cronJobs` and `queueJobs` counts (default 100 each).

### GET /health

Health check endpoint.

### /admin/queues

Bull-Board dashboard for real-time queue monitoring.

## Project Structure

```
job-processing-comparison/
  .env.example              # Environment variable reference
  submission.json            # Evaluation metadata
  docker-compose.yml         # Multi-service orchestration
  Dockerfile                 # Shared container image
  package.json               # Dependencies and scripts
  README.md                  # This file
  output/
    benchmarking.json        # Benchmark results (generated)
  benchmarking/
    run-benchmark.js         # Standalone benchmark script
    results.json             # Benchmark results copy (generated)
  src/
    config/
      index.js               # Centralized environment config
      database.js             # PostgreSQL connection pool
      redis.js                # Redis connection factory
    db/
      schema.sql              # DDL for jobs and execution_logs
      migrate.js              # Schema migration runner
    lib/
      mock-reporter.js        # Simulated CSV report work (2-10s)
      advisory-lock.js        # PostgreSQL advisory lock helpers
      job-repository.js       # Database CRUD operations
    routes/
      export.js               # POST /api/export
      jobs.js                 # GET /api/jobs
      benchmark.js            # POST /api/benchmark
    services/
      api.js                  # Express API server entry point
      cron-worker.js          # Cron polling service
      queue-worker.js         # BullMQ worker service
```
