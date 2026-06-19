'use strict';

require('dotenv').config();

const express = require('express');
const cors = require('cors');

const { config } = require('../config/index');
const { pool } = require('../config/database');
const { createRedisConnection } = require('../config/redis');
const { runMigrations } = require('../db/migrate');

const { Queue } = require('bullmq');
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { ExpressAdapter } = require('@bull-board/express');

const createExportRouter = require('../routes/export');
const jobsRouter = require('../routes/jobs');
const createBenchmarkRouter = require('../routes/benchmark');

// ---------------------------------------------------------------------------
// application setup
// ---------------------------------------------------------------------------

const app = express();

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------------------------
// BullMQ queue
// ---------------------------------------------------------------------------

const exportQueue = new Queue('export-jobs', {
  connection: createRedisConnection(),
});

// ---------------------------------------------------------------------------
// Bull-Board admin UI
// ---------------------------------------------------------------------------

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

createBullBoard({
  queues: [new BullMQAdapter(exportQueue)],
  serverAdapter,
});

app.use('/admin/queues', serverAdapter.getRouter());

// ---------------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------------

app.use('/api', createExportRouter(exportQueue));
app.use('/api', jobsRouter);
app.use('/api', createBenchmarkRouter(exportQueue));

// ---------------------------------------------------------------------------
// health check
// ---------------------------------------------------------------------------

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

// ---------------------------------------------------------------------------
// error handling middleware
// ---------------------------------------------------------------------------

// eslint-disable-next-line no-unused-vars
app.use((err, _req, res, _next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

// ---------------------------------------------------------------------------
// startup
// ---------------------------------------------------------------------------

async function start() {
  try {
    await runMigrations();

    app.listen(config.port, () => {
      console.log(`API server listening on port ${config.port}`);
      console.log(`Bull-Board UI available at http://localhost:${config.port}/admin/queues`);
      console.log(`Health check at http://localhost:${config.port}/health`);
    });
  } catch (err) {
    console.error('Failed to start API server:', err);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// graceful shutdown
// ---------------------------------------------------------------------------

async function shutdown(signal) {
  console.log(`Received ${signal}. Shutting down gracefully...`);

  try {
    await exportQueue.close();
    console.log('BullMQ queue closed');
  } catch (err) {
    console.error('Error closing BullMQ queue:', err);
  }

  try {
    await pool.end();
    console.log('Database pool closed');
  } catch (err) {
    console.error('Error closing database pool:', err);
  }

  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();

module.exports = { app };
