// db.js — connects your server to PostgreSQL
const { Pool } = require('pg');

const pool = new Pool({
  user:     'postgres',
  host:     'localhost',
  database: 'iotplatform',
  password: 'postgres123',  // ← change to YOUR password
  port:     5432,
});

// Test the connection on startup
pool.connect((err, client, release) => {
  if (err) {
    console.error('❌ Database connection failed:', err.message);
  } else {
    console.log('🗄️  Connected to PostgreSQL');
    release();
  }
});

module.exports = pool;
