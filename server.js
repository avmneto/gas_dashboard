require('dotenv').config({ path: __dirname + '/.env' });
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mqtt = require('mqtt');
const { Pool } = require('pg');

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

// Postgres pool for OHLC persistence
const dns = require('dns');

function buildPgConfig(url) {
  // Garante sslmode=require para Supabase (necessário em alguns ambientes)
  const u = new URL(url);
  if (!u.searchParams.has('sslmode')) {
    u.searchParams.set('sslmode', 'require');
  }
  // Log sanitizado (sem senha)
  const sanitized = u.toString().replace(/:([^:@]+)@/, ':****@');
  console.log('PG target:', sanitized);

  const hostname = u.hostname;
  const port = parseInt(u.port || '5432');
  const database = u.pathname.slice(1) || 'postgres';
  const user = u.username;
  const password = u.password;

  // Detecta se é pooler (porta 6543 ou hostname contém pooler.supabase.com)
  const isPooler = port === 6543 || hostname.includes('pooler.supabase.com');

  // SNI para pooler: usa PROJECT REF (extraído do direct hostname db.<ref>.supabase.co)
  // O pooler Supabase espera o project ref como SNI, não o pooler hostname
  let sniHostname = hostname;
  if (isPooler) {
    // Tenta extrair project ref da env var ou inferir do hostname original
    // Se DATABASE_URL for pooler, não temos o direct hostname aqui.
    // Fallback: usa o hostname do pooler mesmo, mas alguns pools precisam do ref.
    // Para Supabase, o project ref costuma ser a parte antes de .supabase.co no direct.
    // Se não tiver, usa hostname do pooler.
    sniHostname = process.env.PG_SNI_HOSTNAME || hostname;
  }

  // SSL config: pooler PRECISA de servername (SNI) para ENOIDENTIFIER
  const sslConfig = isPooler
    ? { rejectUnauthorized: false, servername: sniHostname }
    : { rejectUnauthorized: false };

  const baseConfig = {
    database,
    user,
    password,
    ssl: sslConfig,
    max: 5,
    connectionTimeoutMillis: 30000,
    idleTimeoutMillis: 30000,
    statement_timeout: 15000,
  };

  // Para pooler: SEMPRE usa hostname (tem IPv4 + SNI no TLS)
  // Para direct: tenta IPv4, fallback para hostname
  if (isPooler) {
    console.log('PG pooler detectado — usando hostname direto (SNI:', sniHostname + ')');
    return Promise.resolve({ ...baseConfig, host: hostname, port });
  }

  // Direct connection: tenta IPv4 explicitamente
  return new Promise((resolve) => {
    dns.lookup(hostname, { family: 4 }, (err, ipv4) => {
      if (err) {
        // ENOTFOUND = sem registro A (só IPv6) → fallback para hostname (usa IPv6 do SO)
        if (err.code === 'ENOTFOUND') {
          console.warn('DNS IPv4 lookup ENOTFOUND (sem registro A), usando hostname direto:', err.message);
          resolve({ ...baseConfig, host: hostname, port });
        } else {
          console.error('DNS IPv4 lookup failed:', err);
          resolve({ ...baseConfig, host: hostname, port });
        }
        return;
      }
      console.log('PG resolved IPv4:', ipv4);
      resolve({ ...baseConfig, host: ipv4, port });
    });
  });
}

// Inicialização assíncrona do pool
let pgPool = null;
async function initPgPool() {
  if (!process.env.DATABASE_URL) {
    console.warn('DATABASE_URL not set — OHLC persistence disabled');
    return;
  }
  try {
    const config = await buildPgConfig(process.env.DATABASE_URL);
    pgPool = new Pool(config);
    pgPool.on('error', (e) => console.error('PG pool error:', e));
    console.log('Postgres pool initialized');
    // Warm-up com retry (3 tentativas, backoff exponencial)
    async function warmUp(attempt = 1) {
      try {
        await pgPool.query('SELECT 1');
        console.log('PG warm-up OK');
      } catch (e) {
        if (attempt < 3) {
          const delay = attempt * 2000;
          console.warn(`PG warm-up failed (attempt ${attempt}/3), retrying in ${delay}ms:`, e.code || e.message);
          await new Promise(r => setTimeout(r, delay));
          return warmUp(attempt + 1);
        }
        console.error('PG warm-up failed after 3 attempts:', e);
      }
    }
    warmUp();
  } catch (e) {
    console.error('PG pool init failed:', e);
  }
}

initPgPool();

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

// Upsert OHLC 15-min candle (called ~1/min per metric)
async function upsertOhlc1m(metric, value, ts = Date.now()) {
  if (!pgPool) return;
  const maxRetries = 2;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      await pgPool.query(
        `SELECT public.upsert_ohlc_1m($1, $2, to_timestamp($3/1000.0) AT TIME ZONE 'UTC')`,
        [metric, value, ts]
      );
      return; // sucesso
    } catch (e) {
      const isLast = attempt === maxRetries;
      const isTransient = e.code === 'ECONNREFUSED' || e.code === 'ETIMEDOUT' || e.code === '08006' || e.code === '08001' || e.code === '57P01';
      if (!isLast && isTransient) {
        const delay = attempt * 1000;
        console.warn(`OHLC upsert (${metric}) transient error, retry ${attempt}/${maxRetries} in ${delay}ms:`, e.code);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      if (isLast) console.error(`OHLC upsert error (${metric}):`, e);
      throw e;
    }
  }
}

function mqtt_connect() {
  // Backoff exponencial: 5s, 10s, 20s, 40s, max 60s
  let reconnectDelay = 5000;
  const maxReconnectDelay = 60000;

  const opts = {
    host: process.env.MQTT_HOST || 'localhost',
    port: parseInt(process.env.MQTT_PORT || '1883'),
    protocol: process.env.MQTT_PORT === '8883' ? 'mqtts' : 'mqtt',
    username: process.env.MQTT_USERNAME || undefined,
    password: process.env.MQTT_PASSWORD || undefined,
    // reconnectPeriod como função = backoff exponencial (mqtt.js v5+)
    reconnectPeriod: () => {
      const delay = reconnectDelay;
      reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
      return delay;
    },
    clientId: 'gas_dashboard_' + Math.random().toString(36).slice(2, 8),
    // Limita pacotes em fila para evitar memory pressure
    queueQoSZero: true,
    maxQueueSize: 100,
  };

  const client = mqtt.connect(opts);

  client.on('connect', () => {
    console.log(`MQTT connected to ${opts.host}:${opts.port}`);
    reconnectDelay = 5000; // reset backoff on successful connect
    client.subscribe(process.env.MQTT_TOPIC || 'gas_monitor/+/status', { qos: 0 });
  });

  client.on('message', async (topic, raw) => {
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

      // Persist OHLC (map device fields to metrics)
      const ts = data._time;
      // Firmware JÁ publica escalado (main.c:1108-1110): pureza=%, fluxo=m³/h, pressão=bar
      // NÃO dividir novamente — isso gravava 10×/100× errado no Supabase
      await Promise.all([
        Number.isFinite(data.s1) ? upsertOhlc1m('purity', data.s1, ts) : Promise.resolve(),
        Number.isFinite(data.s2) ? upsertOhlc1m('flow', data.s2, ts) : Promise.resolve(),
        Number.isFinite(data.s4) ? upsertOhlc1m('pressure', data.s4, ts) : Promise.resolve(),
      ]);
      // s3 (dewpoint) is faulty — not persisted
      // await upsertOhlc1m('dewpoint', data.s3 / 10, ts);
    } catch (e) {
      console.error('Parse error:', e.message);
    }
  });

  client.on('error', (e) => console.error('MQTT error:', e.message));
  // REMOVIDO: client.on('close') com setTimeout manual — a lib já gerencia com reconnectPeriod

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

// GET /api/history?metric=purity&from=2026-10-20&to=2026-10-22
// Returns OHLC 15-min candles from Supabase view
app.get('/api/history', async (req, res) => {
  if (!pgPool) {
    return res.status(503).json({ error: 'Persistence not configured' });
  }
  const { metric, from, to } = req.query;

  // Whitelist de métricas persistidas (dewpoint não é persistido — sensor s3 defeituoso)
  const allowedMetrics = ['purity', 'flow', 'pressure'];
  if (!metric || !allowedMetrics.includes(metric)) {
    return res.status(400).json({ error: `metric required: ${allowedMetrics.join('|')}` });
  }

  // Helper: parse ISO 8601 date, retorna null se inválido
  const parseDate = (s) => {
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  };

  // Default: últimas 24h se from/to não fornecidos
  const now = new Date();
  const defaultFrom = new Date(now.getTime() - 24 * 60 * 60 * 1000);

  const dFrom = from ? parseDate(from) : defaultFrom;
  const dTo = to ? parseDate(to) : now;

  if ((from && !dFrom) || (to && !dTo)) {
    return res.status(400).json({ error: 'from/to inválidos (use ISO 8601)' });
  }

  // Se `to` for só data (AAAA-MM-DD), somar 1 dia para inclusivo
  if (to && /^\d{4}-\d{2}-\d{2}$/.test(to)) {
    dTo.setDate(dTo.getDate() + 1);
  }

  try {
    let sql = `SELECT bucket, open, high, low, close, avg, cnt, bucket_br
               FROM public.v_ohlc_15m
               WHERE metric = $1
                 AND bucket >= $2
                 AND bucket <= $3
               ORDER BY bucket ASC
               LIMIT 10000`;
    const params = [metric, dFrom, dTo];
    const { rows } = await pgPool.query(sql, params);
    res.json({ metric, data: rows });
  } catch (e) {
    console.error('History query error:', e);
    res.status(500).json({ error: 'internal' });
  }
});

const PORT = parseInt(process.env.PORT || '3000');
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Dashboard: http://localhost:${PORT}`);
});
