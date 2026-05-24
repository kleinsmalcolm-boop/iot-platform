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

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const JWT_SECRET = process.env.JWT_SECRET || 'iot-secret-2026';

const deviceLimiter = rateLimit({ windowMs: 15*60*1000, max: 500 });
const authLimiter = rateLimit({ windowMs: 15*60*1000, max: 20 });

const pool = new Pool(
  process.env.DATABASE_URL
    ? { connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } }
    : { host: 'localhost', port: 5432, database: 'iotplatform', user: 'postgres', password: 'Moses@1234' }
);

pool.connect()
  .then(() => console.log('Connected to PostgreSQL'))
  .catch(err => console.error('PostgreSQL error:', err.message));

const mqttClient = mqtt.connect('mqtts://de98cb41160543d4a5a5cc840f429a2c.s1.eu.hivemq.cloud:8883', {
  username: 'Malcolm', password: 'Moses@1234', rejectUnauthorized: true
});

let deviceData = {};
let clients = new Set();

mqttClient.on('connect', () => {
  console.log('Connected to MQTT');
  mqttClient.subscribe('devices/#');
  mqttClient.subscribe('commands/#');
});

mqttClient.on('message', (topic, message) => {
  const payload = message.toString();
  deviceData[topic] = { value: payload, time: new Date() };
  saveReading(topic, payload);
  broadcast({ topic, value: payload, time: new Date() });
  checkAutomations(topic, payload);
});

wss.on('connection', ws => {
  clients.add(ws);
  ws.send(JSON.stringify({ type: 'snapshot', data: deviceData }));
  ws.on('close', () => clients.delete(ws));
});

function broadcast(data) {
  const msg = JSON.stringify(data);
  clients.forEach(ws => { if (ws.readyState === 1) ws.send(msg); });
}

async function saveReading(topic, value) {
  try {
    const parts = topic.split('/');
    const deviceKey = parts[1] || 'unknown';
    const pin = parts[2] || 'default';
    const deviceRes = await pool.query('SELECT id FROM devices WHERE device_key = $1', [deviceKey]);
    if (deviceRes.rows.length > 0) {
      const deviceId = deviceRes.rows[0].id;
      await pool.query('INSERT INTO readings (device_id, pin, value) VALUES ($1, $2, $3)', [deviceId, pin, value]);
      await pool.query('UPDATE devices SET online = true, last_seen = NOW() WHERE id = $1', [deviceId]);
    }
  } catch (err) {}
}

async function checkAutomations(topic, value) {
  try {
    const parts = topic.split('/');
    const deviceKey = parts[1];
    const pin = parts[2];
    const numVal = parseFloat(value);
    if (isNaN(numVal)) return;
    const deviceRes = await pool.query('SELECT id FROM devices WHERE device_key = $1', [deviceKey]);
    if (!deviceRes.rows.length) return;
    const deviceId = deviceRes.rows[0].id;
    const autos = await pool.query(
      'SELECT * FROM automations WHERE trigger_device_id = $1 AND trigger_pin = $2 AND active = true',
      [deviceId, pin]
    );
    for (const auto of autos.rows) {
      let triggered = false;
      if (auto.trigger_condition === 'above' && numVal > auto.trigger_value) triggered = true;
      if (auto.trigger_condition === 'below' && numVal < auto.trigger_value) triggered = true;
      if (auto.trigger_condition === 'equals' && numVal == auto.trigger_value) triggered = true;
      if (triggered) {
        await pool.query('UPDATE automations SET last_triggered = NOW() WHERE id = $1', [auto.id]);
        await pool.query('INSERT INTO automation_logs (automation_id, triggered_value) VALUES ($1, $2)', [auto.id, numVal]);
        if (auto.action_type === 'device_command' && auto.action_device_id) {
          const targetDevice = await pool.query('SELECT device_key FROM devices WHERE id = $1', [auto.action_device_id]);
          if (targetDevice.rows.length) {
            const cmdTopic = 'devices/' + targetDevice.rows[0].device_key + '/' + auto.action_pin;
            mqttClient.publish(cmdTopic, String(auto.action_value));
            broadcast({ topic: cmdTopic, value: auto.action_value, time: new Date(), automated: true });
          }
        }
        if (auto.action_type === 'notification' && auto.org_id) {
          const users = await pool.query('SELECT id FROM users WHERE org_id = $1', [auto.org_id]);
          for (const u of users.rows) {
            await pool.query('INSERT INTO notifications (user_id, title, message) VALUES ($1, $2, $3)',
              [u.id, 'Automation: ' + auto.name, 'Triggered at value ' + numVal]);
          }
        }
      }
    }
  } catch(e) {}
}

function generateApiKey() { return 'iot_' + crypto.randomBytes(24).toString('hex'); }

function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'No token' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Invalid token' }); }
}

async function apiKeyMiddleware(req, res, next) {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey) return res.status(401).json({ error: 'No API key' });
  try {
    const result = await pool.query(
      'SELECT ak.*, d.id as device_id, d.name as device_name, d.device_key, d.user_id, d.org_id FROM api_keys ak JOIN devices d ON ak.device_id = d.id WHERE ak.key_value = $1 AND ak.active = true',
      [apiKey]
    );
    if (!result.rows.length) return res.status(401).json({ error: 'Invalid API key' });
    req.device = result.rows[0];
    await pool.query('UPDATE api_keys SET last_used = NOW() WHERE key_value = $1', [apiKey]);
    next();
  } catch (err) { res.status(500).json({ error: 'Server error' }); }
}

// ── Health ────────────────────────────────────────────
app.get('/api/health', (req, res) => res.json({ status: 'ok', uptime: Math.floor(process.uptime()), devices: Object.keys(deviceData).length, timestamp: new Date() }));

// ── Auth ──────────────────────────────────────────────
app.post('/api/auth/register', authLimiter, async (req, res) => {
  try {
    const { email, password, name, orgName } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
    const hashed = await bcrypt.hash(password, 10);
    const org = await pool.query('INSERT INTO organizations (name, slug) VALUES ($1, $2) RETURNING *',
      [orgName || (name || email.split('@')[0]) + "'s Org", email.split('@')[0] + '_' + Date.now()]);
    const user = await pool.query(
      'INSERT INTO users (email, password, name, org_id, role) VALUES ($1, $2, $3, $4, $5) RETURNING id, email, name, org_id, role',
      [email, hashed, name || email.split('@')[0], org.rows[0].id, 'admin']
    );
    const token = jwt.sign({ userId: user.rows[0].id, orgId: org.rows[0].id, role: 'admin' }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ success: true, user: user.rows[0], org: org.rows[0], token });
  } catch (err) {
    if (err.code === '23505') res.status(400).json({ error: 'Email already registered' });
    else res.status(500).json({ error: err.message });
  }
});

app.post('/api/auth/login', authLimiter, async (req, res) => {
  try {
    const { email, password } = req.body;
    const result = await pool.query('SELECT u.*, o.name as org_name FROM users u LEFT JOIN organizations o ON u.org_id = o.id WHERE u.email = $1', [email]);
    if (!result.rows.length) return res.status(401).json({ error: 'No account with that email' });
    const valid = await bcrypt.compare(password, result.rows[0].password);
    if (!valid) return res.status(401).json({ error: 'Wrong password' });
    const token = jwt.sign({ userId: result.rows[0].id, orgId: result.rows[0].org_id, role: result.rows[0].role }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ success: true, token, user: { id: result.rows[0].id, email, name: result.rows[0].name, role: result.rows[0].role, org_id: result.rows[0].org_id, org_name: result.rows[0].org_name } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Organization ──────────────────────────────────────
app.get('/api/org', authMiddleware, async (req, res) => {
  try {
    const org = await pool.query('SELECT * FROM organizations WHERE id = $1', [req.user.orgId]);
    const members = await pool.query('SELECT id, name, email, role, created_at FROM users WHERE org_id = $1', [req.user.orgId]);
    res.json({ org: org.rows[0], members: members.rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/org', authMiddleware, async (req, res) => {
  try {
    const { name } = req.body;
    const org = await pool.query('UPDATE organizations SET name = $1 WHERE id = $2 RETURNING *', [name, req.user.orgId]);
    res.json(org.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/org/invite', authMiddleware, async (req, res) => {
  try {
    const { email, role } = req.body;
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query('INSERT INTO invitations (org_id, email, role, token) VALUES ($1, $2, $3, $4)', [req.user.orgId, email, role || 'viewer', token]);
    res.json({ success: true, invite_link: req.protocol + '://' + req.get('host') + '/invite/' + token });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/org/members', authMiddleware, async (req, res) => {
  try {
    const members = await pool.query('SELECT id, name, email, role, created_at FROM users WHERE org_id = $1', [req.user.orgId]);
    res.json(members.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/org/members/:id', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM users WHERE id = $1 AND org_id = $2', [req.params.id, req.user.orgId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Devices ───────────────────────────────────────────
app.get('/api/my-devices', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT d.*, (SELECT key_value FROM api_keys WHERE device_id = d.id AND active = true LIMIT 1) as api_key FROM devices d WHERE d.org_id = $1 ORDER BY d.created_at DESC',
      [req.user.orgId]
    );
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/my-devices', authMiddleware, async (req, res) => {
  try {
    const { name, type, location, description, lat, lng } = req.body;
    if (!name) return res.status(400).json({ error: 'Device name required' });
    const deviceKey = 'dev_' + Math.random().toString(36).substr(2, 12);
    const device = await pool.query(
      'INSERT INTO devices (user_id, org_id, name, device_key, type, location, description, lat, lng) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *',
      [req.user.userId, req.user.orgId, name, deviceKey, type || 'generic', location, description, lat, lng]
    );
    const apiKey = generateApiKey();
    await pool.query('INSERT INTO api_keys (user_id, device_id, key_value, name) VALUES ($1, $2, $3, $4)',
      [req.user.userId, device.rows[0].id, apiKey, name + ' Key']);
    res.json({ ...device.rows[0], api_key: apiKey });
  } catch(e) { res.status(400).json({ error: e.message }); }
});

app.get('/api/my-devices/:id', authMiddleware, async (req, res) => {
  try {
    const device = await pool.query(
      'SELECT d.*, (SELECT key_value FROM api_keys WHERE device_id = d.id AND active = true LIMIT 1) as api_key FROM devices d WHERE d.id = $1 AND d.org_id = $2',
      [req.params.id, req.user.orgId]
    );
    if (!device.rows.length) return res.status(404).json({ error: 'Device not found' });
    res.json(device.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/my-devices/:id', authMiddleware, async (req, res) => {
  try {
    const { name, type, location, description, lat, lng } = req.body;
    const device = await pool.query(
      'UPDATE devices SET name=$1, type=$2, location=$3, description=$4, lat=$5, lng=$6 WHERE id=$7 AND org_id=$8 RETURNING *',
      [name, type, location, description, lat, lng, req.params.id, req.user.orgId]
    );
    res.json(device.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/my-devices/:id', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM devices WHERE id = $1 AND org_id = $2', [req.params.id, req.user.orgId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/my-devices/:id/readings', authMiddleware, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const pin = req.query.pin;
    let q = 'SELECT * FROM readings WHERE device_id = $1';
    const params = [req.params.id];
    if (pin) { q += ' AND pin = $2 ORDER BY recorded_at DESC LIMIT $3'; params.push(pin, limit); }
    else { q += ' ORDER BY recorded_at DESC LIMIT $2'; params.push(limit); }
    const result = await pool.query(q, params);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/my-devices/:id/command', authMiddleware, async (req, res) => {
  try {
    const { command, value } = req.body;
    const device = await pool.query('SELECT device_key FROM devices WHERE id = $1', [req.params.id]);
    if (!device.rows.length) return res.status(404).json({ error: 'Device not found' });
    const topic = 'devices/' + device.rows[0].device_key + '/' + command;
    mqttClient.publish(topic, String(value));
    await pool.query('INSERT INTO commands (device_id, command, value) VALUES ($1, $2, $3)', [req.params.id, command, String(value)]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Device Widgets ────────────────────────────────────
app.get('/api/my-devices/:id/widgets', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM device_widgets WHERE device_id = $1 ORDER BY position', [req.params.id]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/my-devices/:id/widgets', authMiddleware, async (req, res) => {
  try {
    const { widget_type, label, pin, config, position } = req.body;
    const result = await pool.query(
      'INSERT INTO device_widgets (device_id, widget_type, label, pin, config, position) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *',
      [req.params.id, widget_type, label, pin, JSON.stringify(config || {}), position || 0]
    );
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/my-devices/:id/widgets/:wid', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM device_widgets WHERE id = $1 AND device_id = $2', [req.params.wid, req.params.id]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Live Data ─────────────────────────────────────────
app.get('/api/devices', (req, res) => res.json(deviceData));

// ── Device API (called by IoT devices) ───────────────
app.post('/api/device/data', deviceLimiter, apiKeyMiddleware, async (req, res) => {
  try {
    const { pin, value, data } = req.body;
    const deviceId = req.device.device_id;
    const deviceKey = req.device.device_key;
    const process = async (p, v) => {
      await pool.query('INSERT INTO readings (device_id, pin, value) VALUES ($1, $2, $3)', [deviceId, p, String(v)]);
      const topic = 'devices/' + deviceKey + '/' + p;
      deviceData[topic] = { value: String(v), time: new Date() };
      broadcast({ topic, value: String(v), time: new Date() });
      mqttClient.publish(topic, String(v));
      await checkAutomations(topic, String(v));
    };
    if (pin !== undefined && value !== undefined) await process(pin, value);
    if (Array.isArray(data)) for (const r of data) await process(r.pin, r.value);
    await pool.query('UPDATE devices SET online = true, last_seen = NOW() WHERE id = $1', [deviceId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/device/commands', deviceLimiter, apiKeyMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM commands WHERE device_id = $1 AND executed = false ORDER BY created_at ASC', [req.device.device_id]);
    if (result.rows.length) await pool.query('UPDATE commands SET executed = true WHERE device_id = $1 AND executed = false', [req.device.device_id]);
    res.json({ commands: result.rows });
  } catch(e) { res.json({ commands: [] }); }
});

// ── Automations ───────────────────────────────────────
app.get('/api/automations', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT a.*, d1.name as trigger_device_name, d2.name as action_device_name FROM automations a LEFT JOIN devices d1 ON a.trigger_device_id = d1.id LEFT JOIN devices d2 ON a.action_device_id = d2.id WHERE a.org_id = $1 ORDER BY a.created_at DESC',
      [req.user.orgId]
    );
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/automations', authMiddleware, async (req, res) => {
  try {
    const { name, trigger_device_id, trigger_pin, trigger_condition, trigger_value, action_type, action_device_id, action_pin, action_value } = req.body;
    const result = await pool.query(
      'INSERT INTO automations (org_id, name, trigger_device_id, trigger_pin, trigger_condition, trigger_value, action_type, action_device_id, action_pin, action_value) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *',
      [req.user.orgId, name, trigger_device_id, trigger_pin, trigger_condition, trigger_value, action_type, action_device_id, action_pin, action_value]
    );
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/automations/:id', authMiddleware, async (req, res) => {
  try {
    const { active } = req.body;
    const result = await pool.query('UPDATE automations SET active = $1 WHERE id = $2 AND org_id = $3 RETURNING *', [active, req.params.id, req.user.orgId]);
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/automations/:id', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM automations WHERE id = $1 AND org_id = $2', [req.params.id, req.user.orgId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/automations/:id/logs', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM automation_logs WHERE automation_id = $1 ORDER BY triggered_at DESC LIMIT 50', [req.params.id]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Notifications ─────────────────────────────────────
app.get('/api/notifications', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM notifications WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50', [req.user.userId]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/read', authMiddleware, async (req, res) => {
  try {
    await pool.query('UPDATE notifications SET read = true WHERE user_id = $1', [req.user.userId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Locations ─────────────────────────────────────────
app.get('/api/locations', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT l.*, COUNT(d.id) as device_count FROM locations l LEFT JOIN devices d ON d.location = l.name AND d.org_id = l.org_id WHERE l.org_id = $1 GROUP BY l.id ORDER BY l.created_at DESC', [req.user.orgId]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/locations', authMiddleware, async (req, res) => {
  try {
    const { name, address, lat, lng } = req.body;
    const result = await pool.query('INSERT INTO locations (org_id, name, address, lat, lng) VALUES ($1, $2, $3, $4, $5) RETURNING *', [req.user.orgId, name, address, lat, lng]);
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/locations/:id', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM locations WHERE id = $1 AND org_id = $2', [req.params.id, req.user.orgId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Snapshots ─────────────────────────────────────────
app.get('/api/snapshots', authMiddleware, async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM snapshots WHERE org_id = $1 ORDER BY created_at DESC', [req.user.orgId]);
    res.json(result.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/snapshots', authMiddleware, async (req, res) => {
  try {
    const { name } = req.body;
    const result = await pool.query('INSERT INTO snapshots (org_id, name, data) VALUES ($1, $2, $3) RETURNING *', [req.user.orgId, name || 'Snapshot ' + new Date().toLocaleString(), JSON.stringify(deviceData)]);
    res.json(result.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/snapshots/:id', authMiddleware, async (req, res) => {
  try {
    await pool.query('DELETE FROM snapshots WHERE id = $1 AND org_id = $2', [req.params.id, req.user.orgId]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('IoT server running on port ' + PORT));
