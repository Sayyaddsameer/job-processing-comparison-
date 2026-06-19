require('dotenv').config();

const config = {
  port: parseInt(process.env.PORT, 10) || 3000,
  databaseUrl: process.env.DATABASE_URL || 'postgres://jobuser:jobpassword@localhost:5432/jobcompare',
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  cronIntervalMs: parseInt(process.env.CRON_INTERVAL_MS, 10) || 10000,
  workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY, 10) || 3,
  workerId: process.env.WORKER_ID || `worker-${process.pid}`,
  advisoryLockEnabled: process.env.ADVISORY_LOCK_ENABLED !== 'false',
  failureRate: parseFloat(process.env.FAILURE_RATE) || 0.2,
};

module.exports = { config };
