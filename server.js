require('dotenv').config({ path: __dirname + '/.env' });
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');

process.on('uncaughtException', (e) => console.error('UNCAUGHT:', e));
process.on('unhandledRejection', (e) => console.error('UNHANDLED:', e));

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname + '/public'));

const devices = new Map();

function mqtt_connect() {
  try {
    const host = process.env.MQTT_HOST || 'localhost';
    const port = process.env.MQTT_PORT || '1883';
    const proto = port === '8883' ? 'mqtts' : 'mqtt';
    const url = proto + '://' + host + ':' + port;

    const opts = {
      username: process.env.MQTT_USERNAME || undefined,
      password: process.env.MQTT_PASSWORD || undefined,
      reconnectPeriod: 5000,
      clientId: 'gas_dashboard_' + Math.random().toString(36).slice(2, 8),
      rejectUnauthorized: false,
    };

    const client = mqtt.connect(url, opts);

    client.on('connect', () => {
      console.log('MQTT connected to ' + url);
      client.subscribe(process.env.MQTT_TOPIC || 'gas_monitor/+/status', { qos: 0 });
    });

    client.on('message', (topic, raw) => {
      try {
        const parts = topic.split('/');
        const deviceId = parts.length >= 3 ? parts[1] : 'unknown';
        const data = JSON.parse(raw.toString());
        data._device = deviceId;
        data._time = Date.now();
        if (!devices.has(deviceId)) devices.set(deviceId, []);
        const history = devices.get(deviceId);
        history.push(data);
        if (history.length > 200) history.shift();
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
  } catch (e) {
    console.error('MQTT connect error:', e.message);
    setTimeout(mqtt_connect, 5000);
  }
}

mqtt_connect();

io.on('connection', (socket) => {
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
  console.log('Dashboard: http://localhost:' + PORT);
});
