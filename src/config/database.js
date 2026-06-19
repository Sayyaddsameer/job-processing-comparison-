const { Pool } = require('pg');
const { config } = require('./index');

const pool = new Pool({ connectionString: config.databaseUrl });

async function getClient() {
  return pool.connect();
}

module.exports = { pool, getClient };
