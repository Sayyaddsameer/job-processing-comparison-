'use strict';

const express = require('express');
const { pool } = require('../config/database');
const { createJob } = require('../lib/job-repository');

const VALID_TYPES = ['CRON', 'QUEUE'];

/**
 * Factory that creates the export router.
 * Accepts a BullMQ Queue instance so the route can enqueue QUEUE-type jobs.
 *
 * @param {import('bullmq').Queue} exportQueue
 * @returns {express.Router}
 */
function createExportRouter(exportQueue) {
  const router = express.Router();

  router.post('/export', async (req, res, next) => {
    try {
      const { type, priority, user_id } = req.body;

      // --- validation -----------------------------------------------------------
      if (!type || !VALID_TYPES.includes(type)) {
        return res.status(400).json({
          error: `Invalid type. Must be one of: ${VALID_TYPES.join(', ')}`,
        });
      }

      const resolvedPriority = priority === undefined || priority === null
        ? 10
        : priority;

      if (
        !Number.isInteger(resolvedPriority) ||
        resolvedPriority < 1 ||
        resolvedPriority > 10
      ) {
        return res.status(400).json({
          error: 'Invalid priority. Must be an integer between 1 and 10.',
        });
      }

      // --- create job in database ------------------------------------------------
      const job = await createJob(pool, {
        type,
        priority: resolvedPriority,
        userId: user_id,
      });

      // --- enqueue for worker processing when type is QUEUE ----------------------
      if (type === 'QUEUE') {
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
            attempts: 4,       // 1 initial + 3 retries
            backoff: {
              type: 'exponential',
              delay: 5000,     // 5s, 10s, 20s
            },
          },
        );
      }

      return res.status(201).json({
        job_id: job.id,
        status: job.status,
        type: job.type,
      });
    } catch (err) {
      next(err);
    }
  });

  return router;
}

module.exports = createExportRouter;
