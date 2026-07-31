require('dotenv').config({ path: __dirname + '/.env' });
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname + '/public'));

// Store latest data per device
const devices = new Map();
const deviceLastSeen = new Map();

// Telegram alarm
const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT_ID;
const TELEGRAM_ENABLED = !!(TELEGRAM_TOKEN && TELEGRAM_CHAT);

function sendTelegram(text) {
  if (!TELEGRAM_ENABLED) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
  const body = new URLSearchParams({ chat_id: TELEGRAM_CHAT, text });
  fetch(url, { method: 'POST', body })
    .then(r => r.json())
    .then(j => { if (!j.ok) console.error('Telegram:', j.description); })
    .catch(e => console.error('Telegram:', e.message));
}

function sendBootAlarm(deviceId, data) {
  if (!TELEGRAM_ENABLED) return;
  sendTelegram(`⚠️ ${deviceId} ligou`);
  sendTelegram(`S1 (Pureza): ${data.s1}`);
  sendTelegram(`S2: ${data.s2}`);
  sendTelegram(`S3: ${data.s3}`);
  sendTelegram(`S4: ${data.s4}`);
  console.log(`Boot alarm sent for ${deviceId}`);
}

function mqtt_connect() {
  const opts = {
    host: process.env.MQTT_HOST || 'localhost',
    port: parseInt(process.env.MQTT_PORT || '1883'),
    protocol: process.env.MQTT_PORT === '8883' ? 'mqtts' : 'mqtt',
    username: process.env.MQTT_USERNAME || undefined,
    password: process.env.MQTT_PASSWORD || undefined,
    reconnectPeriod: 5000,
    clientId: 'gas_dashboard_' + Math.random().toString(36).slice(2, 8),
  };

  const client = mqtt.connect(opts);

  client.on('connect', () => {
    console.log(`MQTT connected to ${opts.host}:${opts.port}`);
    client.subscribe(process.env.MQTT_TOPIC || 'gas_monitor/+/status', { qos: 0 });
  });

  client.on('message', (topic, raw) => {
    try {
      // Extract device ID from topic: gas_monitor/<device_id>/status
      const parts = topic.split('/');
      const deviceId = parts.length >= 3 ? parts[1] : 'unknown';

      const data = JSON.parse(raw.toString());
      data._device = deviceId;
      data._time = Date.now();

      // Keep last 200 points per device
      if (!devices.has(deviceId)) {
        devices.set(deviceId, []);
      }
      const history = devices.get(deviceId);
      history.push(data);
      if (history.length > 200) history.shift();

      // Send Telegram boot alarm if device just came online
      const now = Date.now();
      const last = deviceLastSeen.get(deviceId) || 0;
      if (now - last > 30000) {
        sendBootAlarm(deviceId, data);
      }
      deviceLastSeen.set(deviceId, now);

      // Broadcast to all connected browsers
      io.emit('data', data);
    } catch (e) {
      console.error('Parse error:', e.message);
    }
  });

  client.on('error', (e) => console.error('MQTT error:', e.message));
  client.on('close', () => {
    console.log('MQTT disconnected, reconnecting...');
    setTimeout(mqtt_connect, 5000);
  });

  return client;
}

mqtt_connect();

io.on('connection', (socket) => {
  // Send full history on new connection
  for (const [deviceId, history] of devices) {
    socket.emit('history', { deviceId, data: history });
  }
});

app.get('/api/devices', (req, res) => {
  const result = {};
  for (const [id, history] of devices) {
    result[id] = history.length > 0 ? history[history.length - 1] : null;
  }
  res.json(result);
});

const PORT = parseInt(process.env.PORT || '3000');
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard: http://localhost:${PORT}`);
});
