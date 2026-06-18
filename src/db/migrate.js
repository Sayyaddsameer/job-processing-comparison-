const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { config } = require('../config/index');

async function runMigrations(pool) {
  const ownPool = !pool;
  if (!pool) {
    pool = new Pool({ connectionString: config.databaseUrl });
  }

  try {
    const schemaPath = path.join(__dirname, 'schema.sql');
    const sql = fs.readFileSync(schemaPath, 'utf-8');
    await pool.query(sql);
    console.log('[migrate] Schema applied successfully');
  } catch (err) {
    console.error('[migrate] Migration failed:', err.message);
    throw err;
  } finally {
    if (ownPool) {
      await pool.end();
    }
  }
}

if (require.main === module) {
  const pool = new Pool({ connectionString: config.databaseUrl });
  runMigrations(pool)
    .then(() => {
      console.log('[migrate] Done');
      process.exit(0);
    })
    .catch((err) => {
      console.error('[migrate] Fatal:', err.message);
      process.exit(1);
    });
}

module.exports = { runMigrations };
