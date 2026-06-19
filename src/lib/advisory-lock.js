'use strict';

/**
 * PostgreSQL advisory lock helpers.
 * Converts UUIDs to numeric lock keys for the two-argument advisory lock form.
 * Uses pg_try_advisory_xact_lock so locks auto-release at COMMIT/ROLLBACK.
 */

/**
 * Convert a UUID string into two 32-bit integer keys suitable for
 * the two-argument form of pg_try_advisory_xact_lock.
 *
 * @param {string} uuid
 * @returns {{ key1: number, key2: number }}
 */
function uuidToLockKey(uuid) {
  const hex = uuid.replace(/-/g, '');
  const key1 = parseInt(hex.substring(0, 8), 16);
  const key2 = parseInt(hex.substring(8, 16), 16);
  return { key1, key2 };
}

/**
 * Attempt to acquire a transaction-level advisory lock for the given job ID.
 * The caller must already be inside a BEGIN/COMMIT block on the provided client.
 *
 * @param {import('pg').PoolClient} client - A pg client within an active transaction.
 * @param {string} jobId - UUID of the job to lock.
 * @returns {Promise<boolean>} Whether the lock was successfully acquired.
 */
async function tryAcquire(client, jobId) {
  const { key1, key2 } = uuidToLockKey(jobId);
  console.log(`[AdvisoryLock] Attempting lock for job ${jobId} (keys: ${key1}, ${key2})`);

  const result = await client.query(
    'SELECT pg_try_advisory_xact_lock($1, $2) AS locked',
    [key1, key2]
  );

  const acquired = result.rows[0].locked;
  console.log(`[AdvisoryLock] Lock for job ${jobId}: ${acquired ? 'acquired' : 'not acquired'}`);

  return acquired;
}

module.exports = { uuidToLockKey, tryAcquire };
