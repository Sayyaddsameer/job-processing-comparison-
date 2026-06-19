const Redis = require('ioredis');
const { config } = require('./index');

let singleton = null;

function createRedisConnection() {
  return new Redis(config.redisUrl, {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  });
}

function getRedisConnection() {
  if (!singleton) {
    singleton = createRedisConnection();
  }
  return singleton;
}

module.exports = { createRedisConnection, getRedisConnection };
