'use strict';

const express = require('express');
const { pool } = require('../config/database');
const { getJobById, getJobsByType } = require('../lib/job-repository');

const router = express.Router();

// ---------------------------------------------------------------------------
// GET /api/jobs  -  list jobs with optional filters
// ---------------------------------------------------------------------------
router.get('/jobs', async (req, res, next) => {
  try {
    const { type, status } = req.query;

    let jobs;

    if (type) {
      jobs = await getJobsByType(pool, type);
    } else {
      // No type filter -- query all jobs directly from the pool.
      const queryParts = ['SELECT * FROM jobs'];
      const values = [];

      if (status) {
        queryParts.push('WHERE status = $1');
        values.push(status);
      }

      queryParts.push('ORDER BY submitted_at DESC');

      const result = await pool.query(queryParts.join(' '), values);
      jobs = result.rows;
    }

    // Apply status filter on top of type-filtered results when both are given.
    if (type && status) {
      jobs = jobs.filter((j) => j.status === status);
    }

    return res.json({ jobs });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/jobs/:id  -  single job lookup
// ---------------------------------------------------------------------------
router.get('/jobs/:id', async (req, res, next) => {
  try {
    const job = await getJobById(pool, req.params.id);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    return res.json({ job });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
