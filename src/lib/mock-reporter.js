'use strict';

/**
 * Mock reporter that simulates CSV report generation work.
 * Sleeps for a random duration and optionally fails based on a configurable rate.
 */

async function simulateWork(failureRate) {
  const duration = Math.floor(Math.random() * (10000 - 2000 + 1)) + 2000;
  console.log(`[MockReporter] Simulating work for ${duration}ms`);

  const startedAt = Date.now();

  await new Promise((resolve) => setTimeout(resolve, duration));

  if (Math.random() < failureRate) {
    throw new Error('Simulated processing failure');
  }

  const completedAt = Date.now();

  return { startedAt, completedAt, duration };
}

module.exports = { simulateWork };
