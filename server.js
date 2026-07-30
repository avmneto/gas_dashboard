require('dotenv').config({ path: __dirname + '/.env' });
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');

process.on('uncaughtException', (e) => console.error('FATAL:', e));
process.on('unhandledRejection', (e) => console.error('FATAL:', e));

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname + '/public'));

const devices = new Map();

function mqtt_connect() {
  const host = process.env.MQTT_HOST;
  const port = process.env.MQTT_PORT;
  const user = process.env.MQTT_USERNAME;
  const pass = process.env.MQTT_PASSWORD;
  if (!host) { console.log('MQTT: no host, skipping'); return; }

  const proto = port === '8883' ? 'mqtts' : 'mqtt';
  const url = proto + '://' + host + ':' + (port || '1883');
  const opts = {
    username: user,
    password: pass,
    reconnectPeriod: 5000,
    clientId: 'gas_' + Math.random().toString(36).slice(2, 8),
    rejectUnauthorized: false,
  };
  console.log('MQTT: connecting to ' + url);
  const client = mqtt.connect(url, opts);
  client.on('connect', () => {
    console.log('MQTT connected');
    client.subscribe(process.env.MQTT_TOPIC || 'gas_monitor/+/status');
  });
  client.on('message', (topic, raw) => {
    try {
      const deviceId = topic.split('/')[1] || 'unknown';
      const data = JSON.parse(raw.toString());
      data._device = deviceId;
      data._time = Date.now();
      if (!devices.has(deviceId)) devices.set(deviceId, []);
      const h = devices.get(deviceId);
      h.push(data);
      if (h.length > 200) h.shift();
      io.emit('data', data);
    } catch (e) { console.error('Parse:', e.message); }
  });
  client.on('error', (e) => console.error('MQTT:', e.message));
  client.on('close', () => setTimeout(mqtt_connect, 5000));
}

mqtt_connect();

io.on('connection', (socket) => {
  for (const [id, h] of devices) socket.emit('history', { deviceId: id, data: h });
});

app.get('/api/devices', (req, res) => {
  const out = {};
  for (const [k, v] of devices) out[k] = v.length > 0 ? v[v.length - 1] : null;
  res.json(out);
});

const PORT = parseInt(process.env.PORT || '3000');
server.listen(PORT, '0.0.0.0', () => console.log('Dashboard on port ' + PORT));
