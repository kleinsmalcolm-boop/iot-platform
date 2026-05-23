require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const WebSocket = require('ws');
const http = require('http');
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const crypto = require('crypto');
const rateLimit = require('express-rate-limit');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ── Middleware ────────────────────────────────────────
// CORS allows your mobile app and dashboard to talk to this server
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const JWT_SECRET = process.env.JWT_SECRET || 'your-secret-key-change-this';

// ── Rate Limiting ─────────────────────────────────────
// Devices can only send 200 requests per 15 minutes
// This prevents one broken device from crashing your server
const deviceLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 200,
  message: { error: 'Too many requests, slow down your device' }
});

// Login/register can only be tried 20 times per 15 minutes
// This prevents hackers from guessing passwords
const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { error: 'Too many attempts, try again later' }
});

// ── PostgreSQL ────────────────────────────────────────
const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : { host: 'localhost', port: 5432, database: 'iotplatform', user: 'postgres', password: 'Moses@1234' }
);});

pool.connect()
  .then(() => console.log('🗄️  Connected to PostgreSQL'))
  .catch(err => console.error('PostgreSQL error:', err.message));

// ── MQTT ──────────────────────────────────────────────
const mqttClient = mqtt.connect('mqtts://de98cb41160543d4a5a5cc840f429a2c.s1.eu.hivemq.cloud:8883', {
  username: 'Malcolm',
  password: 'Moses@1234',
  rejectUnauthorized: true
});
let deviceData = {};

mqttClient.on('connect', () => {
  console.log('✅ Connected to MQTT broker');
  mqttClient.subscribe('devices/#');
});

mqttClient.on('message', (topic, message) => {
  const payload = message.toString();
  console.log(`📡 [${topic}]`, payload);
  deviceData[topic] = { value: payload, time: new Date() };
  saveReading(topic, payload);
  const msg = JSON.stringify({ topic, value: payload, time: new Date() });
  clients.forEach(ws => { if (ws.readyState === 1) ws.send(msg); });
});

// ── WebSocket ─────────────────────────────────────────
let clients = new Set();
wss.on('connection', ws => {
  clients.add(ws);
  // Send current device data immediately when someone connects
  ws.send(JSON.stringify({ type: 'snapshot', data: deviceData }));
  ws.on('close', () => clients.delete(ws));
});

// ── Helper: broadcast to all dashboard clients ────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => { if (ws.readyState === 1) ws.send(msg); });
}

// ── Helper: save reading to database ─────────────────
async function saveReading(topic, value) {
  try {
    const parts = topic.split('/');
    const deviceKey = parts[1] || 'unknown';
    const pin = parts[2] || 'default';
    const deviceRes = await pool.query(
      'SELECT id FROM devices WHERE device_key = $1', [deviceKey]
    );
    if (deviceRes.rows.length > 0) {
      const deviceId = deviceRes.rows[0].id;
      await pool.query(
        'INSERT INTO readings (device_id, pin, value) VALUES ($1, $2, $3)',
        [deviceId, pin, value]
      );
      await pool.query(
        'UPDATE devices SET online = true, last_seen = NOW() WHERE id = $1',
        [deviceId]
      );
    }
  } catch (err) {
    // Device not registered yet — that is fine
  }
}

// ── Helper: generate a secure API key ────────────────
// Creates a random key like: iot_a3f9b2c1d4e5f6789abcdef123456
function generateApiKey() {
  return 'iot_' + crypto.randomBytes(24).toString('hex');
}

// ── Auth Middleware (for dashboard/app users) ─────────
// Checks the JWT token that users get when they log in
function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token provided' });
  try {
    req.user = jwt.verify(token, JWT_SECRET);
    next();
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}

// ── API Key Middleware (for IoT devices) ─────────────
// Checks the API key that devices send with their data
// Devices send it in the header: X-API-Key: iot_abc123...
async function apiKeyMiddleware(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey) return res.status(401).json({ error: 'No API key. Include X-API-Key header' });
  try {
    const result = await pool.query(
      `SELECT ak.*, d.id as device_id, d.name as device_name, d.user_id
       FROM api_keys ak
       JOIN devices d ON ak.device_id = d.id
       WHERE ak.key_value = $1 AND ak.active = true`,
      [apiKey]
    );
    if (!result.rows.length) {
      return res.status(401).json({ error: 'Invalid API key' });
    }
    // Attach device info to the request so route handlers can use it
    req.device = result.rows[0];
    // Update last_used timestamp so you can see when a device last called the API
    await pool.query(
      'UPDATE api_keys SET last_used = NOW() WHERE key_value = $1', [apiKey]
    );
    next();
  } catch (err) {
    res.status(500).json({ error: 'Server error checking API key' });
  }
}

// ═══════════════════════════════════════════════════════
// REST API ROUTES
// ═══════════════════════════════════════════════════════

// ── Health Check ──────────────────────────────────────
// GET /api/health
// Anyone can call this to check if your server is alive
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: Math.floor(process.uptime()) + ' seconds',
    devices_connected: Object.keys(deviceData).length,
    timestamp: new Date()
  });
});

// ── Auth: Register ────────────────────────────────────
// POST /api/auth/register
// Body: { email, password, name }
app.post('/api/auth/register', authLimiter, async (req, res) => {
  try {
    const { email, password, name } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }
    const hashed = await bcrypt.hash(password, 10);
    const result = await pool.query(
      'INSERT INTO users (email, password, name) VALUES ($1, $2, $3) RETURNING id, email, name',
      [email, hashed, name || email.split('@')[0]]
    );
    const token = jwt.sign({ userId: result.rows[0].id }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ success: true, user: result.rows[0], token });
  } catch (err) {
    if (err.code === '23505') {
      res.status(400).json({ error: 'Email already registered' });
    } else {
      res.status(500).json({ error: err.message });
    }
  }
});

// ── Auth: Login ───────────────────────────────────────
// POST /api/auth/login
// Body: { email, password }
app.post('/api/auth/login', authLimiter, async (req, res) => {
  try {
    const { email, password } = req.body;
    const result = await pool.query('SELECT * FROM users WHERE email = $1', [email]);
    if (!result.rows.length) {
      return res.status(401).json({ error: 'No account with that email' });
    }
    const valid = await bcrypt.compare(password, result.rows[0].password);
    if (!valid) {
      return res.status(401).json({ error: 'Wrong password' });
    }
    const token = jwt.sign({ userId: result.rows[0].id }, JWT_SECRET, { expiresIn: '7d' });
    res.json({
      success: true,
      token,
      user: { id: result.rows[0].id, email, name: result.rows[0].name }
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Devices: List user's devices ──────────────────────
// GET /api/my-devices
// Returns all devices belonging to the logged-in user
app.get('/api/my-devices', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT d.*, 
        (SELECT key_value FROM api_keys WHERE device_id = d.id AND active = true LIMIT 1) as api_key
       FROM devices d WHERE d.user_id = $1 ORDER BY d.created_at DESC`,
      [req.user.userId]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Devices: Register new device ──────────────────────
// POST /api/my-devices
// Body: { name, type }
// Creates device + automatically generates an API key for it
app.post('/api/my-devices', authMiddleware, async (req, res) => {
  try {
    const { name, type } = req.body;
    if (!name) return res.status(400).json({ error: 'Device name required' });

    // Create the device
    const deviceKey = 'dev_' + Math.random().toString(36).substr(2, 12);
    const deviceResult = await pool.query(
      'INSERT INTO devices (user_id, name, device_key, type) VALUES ($1, $2, $3, $4) RETURNING *',
      [req.user.userId, name, deviceKey, type || 'generic']
    );
    const device = deviceResult.rows[0];

    // Automatically generate an API key for it
    const apiKey = generateApiKey();
    await pool.query(
      'INSERT INTO api_keys (user_id, device_id, key_value, name) VALUES ($1, $2, $3, $4)',
      [req.user.userId, device.id, apiKey, `${name} API Key`]
    );

    // Return device + its API key so the developer can copy it
    res.json({ ...device, api_key: apiKey });
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

// ── Devices: Get readings history ─────────────────────
// GET /api/my-devices/:id/readings?limit=50
// Returns the last N readings for a device
app.get('/api/my-devices/:id/readings', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    const result = await pool.query(
      `SELECT * FROM readings 
       WHERE device_id = $1 
       ORDER BY recorded_at DESC 
       LIMIT $2`,
      [req.params.id, limit]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Live device data (from MQTT) ──────────────────────
// GET /api/devices
// Returns the latest value from every device topic
app.get('/api/devices', (req, res) => res.json(deviceData));

// ═══════════════════════════════════════════════════════
// DEVICE API — These endpoints are called BY IoT devices
// Devices authenticate with X-API-Key header
// ═══════════════════════════════════════════════════════

// ── Device: Send sensor data ──────────────────────────
// POST /api/device/data
// Headers: { X-API-Key: iot_abc123... }
// Body: { pin: "temperature", value: "25.6" }
// OR send multiple values: { data: [{ pin: "temp", value: "25" }, ...] }
app.post('/api/device/data', deviceLimiter, apiKeyMiddleware, async (req, res) => {
  try {
    const { pin, value, data } = req.body;
    const deviceId = req.device.device_id;
    const deviceKey = req.device.device_key || `device_${deviceId}`;

    // Handle single reading
    if (pin !== undefined && value !== undefined) {
      await pool.query(
        'INSERT INTO readings (device_id, pin, value) VALUES ($1, $2, $3)',
        [deviceId, pin, String(value)]
      );
      const topic = `devices/${deviceKey}/${pin}`;
      deviceData[topic] = { value: String(value), time: new Date() };
      broadcast({ topic, value: String(value), time: new Date() });
      mqttClient.publish(topic, String(value));
    }

    // Handle multiple readings in one request
    if (Array.isArray(data)) {
      for (const reading of data) {
        await pool.query(
          'INSERT INTO readings (device_id, pin, value) VALUES ($1, $2, $3)',
          [deviceId, reading.pin, String(reading.value)]
        );
        const topic = `devices/${deviceKey}/${reading.pin}`;
        deviceData[topic] = { value: String(reading.value), time: new Date() };
        broadcast({ topic, value: String(reading.value), time: new Date() });
        mqttClient.publish(topic, String(reading.value));
      }
    }

    // Mark device as online
    await pool.query(
      'UPDATE devices SET online = true, last_seen = NOW() WHERE id = $1',
      [deviceId]
    );

    res.json({ success: true, device: req.device.device_name });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Device: Get pending commands ──────────────────────
// GET /api/device/commands
// Headers: { X-API-Key: iot_abc123... }
// Devices poll this to check if you sent them a command
// e.g. "turn on LED", "set threshold to 30"
app.get('/api/device/commands', deviceLimiter, apiKeyMiddleware, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT * FROM commands 
       WHERE device_id = $1 AND executed = false 
       ORDER BY created_at ASC`,
      [req.device.device_id]
    );
    // Mark them as delivered
    if (result.rows.length > 0) {
      await pool.query(
        'UPDATE commands SET executed = true WHERE device_id = $1 AND executed = false',
        [req.device.device_id]
      );
    }
    res.json({ commands: result.rows });
  } catch (err) {
    // Commands table might not exist yet — return empty
    res.json({ commands: [] });
  }
});

// ── Dashboard: Send command to device ─────────────────
// POST /api/my-devices/:id/command
// Headers: { Authorization: Bearer <jwt> }
// Body: { command: "LED_ON", value: "1" }
app.post('/api/my-devices/:id/command', authMiddleware, async (req, res) => {
  try {
    const { command, value } = req.body;
    // Publish via MQTT so device gets it instantly if connected
    const topic = `commands/device_${req.params.id}`;
    mqttClient.publish(topic, JSON.stringify({ command, value, time: new Date() }));
    res.json({ success: true, command, value });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API Keys: List keys for a device ──────────────────
// GET /api/my-devices/:id/keys
app.get('/api/my-devices/:id/keys', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, name, key_value, active, last_used, created_at FROM api_keys WHERE device_id = $1',
      [req.params.id]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── API Keys: Regenerate key ──────────────────────────
// POST /api/my-devices/:id/keys/regenerate
// Deactivates old key and creates a new one
app.post('/api/my-devices/:id/keys/regenerate', authMiddleware, async (req, res) => {
  try {
    // Deactivate old keys
    await pool.query(
      'UPDATE api_keys SET active = false WHERE device_id = $1',
      [req.params.id]
    );
    // Generate new key
    const newKey = generateApiKey();
    await pool.query(
      'INSERT INTO api_keys (user_id, device_id, key_value, name) VALUES ($1, $2, $3, $4)',
      [req.user.userId, req.params.id, newKey, 'Regenerated Key']
    );
    res.json({ success: true, api_key: newKey });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Start server ──────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 IoT server running at http://localhost:${PORT}`);
});