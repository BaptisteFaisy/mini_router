// PROTECTION COMBO/CHEAP — NE PAS TOUCHER : combo/cheap TOUJOURS DISPONIBLE et FONCTIONNELLE (JP 2026-09-19).
// Test durable : UNE API DOWN NE DOWN PAS TOUT.
// Un provider primaire en panne (500) + un provider de secours OK :
// la passerelle doit servir la requête via le secours, en non-stream
// comme en stream, puis mettre le primaire en cooldown (visible /healthz).
// Runner : `node --test test/router-failover.test.mjs` (sans dépendance).
import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const GATEWAY = path.join(HERE, '..', 'inferhub-failover.mjs');
const CIRCUIT_FILE = path.join(HERE, '..', 'inferhub-failover-circuit.json');
const MARKER = 'served-by-second';

function freePort() {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolve(p));
    });
  });
}

function readBody(req) {
  return new Promise((resolve) => {
    let b = '';
    req.on('data', (c) => (b += c));
    req.on('end', () => resolve(b));
  });
}

// Primaire simulé : /models OK (pour le tri boot), /chat/completions HS.
function startPrimary() {
  const hits = { completions: 0 };
  const srv = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/v1/models') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"data":[]}');
      return;
    }
    if (req.method === 'POST' && req.url === '/v1/chat/completions') {
      hits.completions++;
      await readBody(req);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"primary down (simulé)"}}');
      return;
    }
    res.writeHead(404); res.end();
  });
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port, hits }));
  });
}

// Secours simulé : OpenAI-compatible minimal, stream + non-stream.
function startSecondary() {
  const srv = http.createServer(async (req, res) => {
    if (req.method === 'GET' && req.url === '/v1/models') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"data":[]}');
      return;
    }
    if (req.method === 'POST' && req.url === '/v1/chat/completions') {
      const raw = await readBody(req);
      let body = {};
      try { body = JSON.parse(raw); } catch {}
      if (body.stream) {
        res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
        res.write(`data: {"choices":[{"delta":{"role":"assistant","content":"${MARKER}"}}]}\n\n`);
        res.write('data: [DONE]\n\n');
        res.end();
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        choices: [{ message: { role: 'assistant', content: MARKER } }],
        usage: { completion_tokens: 5 },
      }));
      return;
    }
    res.writeHead(404); res.end();
  });
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

function gwRequest(port, { method = 'GET', path = '/', body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? null : JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1', port, method, path,
      headers: { 'Content-Type': 'application/json', ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}) },
      timeout: 30000,
    }, (res) => {
      let b = '';
      res.on('data', (c) => (b += c));
      res.on('end', () => resolve({ status: res.statusCode, text: b }));
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

let primary, secondary, gw, gwPort, gwStderr = '', cfgPath;

before(async () => {
  primary = await startPrimary();
  secondary = await startSecondary();
  gwPort = await freePort();
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}
  const cfg = {
    port: gwPort,
    upstream: `http://127.0.0.1:${primary.port}/v1`,
    keyEnv: 'ROUTER_TEST_PRIMARY_KEY_ABSENTE',
    attemptsPerModel: 1,
    chainPasses: 1,
    retryDelayMs: 1,
    fetchTimeoutMs: 3000,
    ttftTimeoutMs: 3000,
    maxConcurrent: 50,
    maxQueue: 100,
    chains: { 'model-a': [] },
    defaultBackups: [],
    secondChance: [],
    providers: [{
      name: 'second',
      base: `http://127.0.0.1:${secondary.port}/v1`,
      key: 'test-key', // le faux secours n'authentifie pas ; teste le chemin clé statique
      models: ['model-b'],
    }],
  };
  cfgPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'router-test-')), 'gw.json');
  fs.writeFileSync(cfgPath, JSON.stringify(cfg));
  gw = spawn(process.execPath, [GATEWAY], {
    env: { ...process.env, FAILOVER_CONFIG: cfgPath, FAILOVER_PORT: String(gwPort) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  gw.stderr.on('data', (c) => { gwStderr += c.toString(); });
  // Attente dispo : /v1/models est proximisé vers le primaire simulé.
  const t0 = Date.now();
  for (;;) {
    try {
      const r = await gwRequest(gwPort, { path: '/v1/models' });
      if (r.status === 200) break;
    } catch {}
    if (Date.now() - t0 > 45000) {
      throw new Error(`passerelle jamais prête. stderr:\n${gwStderr.slice(-3000)}`);
    }
    await new Promise((r) => setTimeout(r, 250));
  }
});

after(async () => {
  try { gw.kill(); } catch {}
  try { primary.srv.close(); } catch {}
  try { secondary.srv.close(); } catch {}
  try { fs.unlinkSync(cfgPath); fs.rmdirSync(path.dirname(cfgPath)); } catch {}
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}
});

test('primaire down → requête servie par le secours (non-stream)', async () => {
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: 'model-a', messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  assert.equal(r.status, 200, `attendu 200 via secours, reçu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(r.text, new RegExp(MARKER), 'la réponse doit venir du provider de secours');
});

test('primaire down → requête servie par le secours (stream)', async () => {
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: 'model-a', messages: [{ role: 'user', content: 'ping' }], stream: true },
  });
  assert.equal(r.status, 200, `attendu 200 via secours, reçu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(r.text, new RegExp(MARKER), 'le stream doit venir du provider de secours');
  assert.match(r.text, /\[DONE\]/, 'le stream doit se terminer par [DONE]');
});

test('le provider down passe en cooldown (/healthz), le service reste OK', async () => {
  const h = await gwRequest(gwPort, { path: '/healthz' });
  assert.equal(h.status, 200, `attendu /healthz 200, reçu ${h.status}`);
  const j = JSON.parse(h.text);
  assert.equal(j.ok, true, 'le service doit rester OK malgré le primaire down');
  assert.ok(j.providers && j.providers.inferhub, 'healthz doit exposer l’état du primaire');
  assert.equal(j.providers.inferhub.down, true, 'le primaire en panne doit être marqué down (cooldown)');
  // Et une 3e requête passe toujours (primaire sauté, secours direct).
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: 'model-a', messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  assert.equal(r.status, 200);
  assert.match(r.text, new RegExp(MARKER));
});

test('GET / = page de statut 200 (pas de 405 dans le navigateur)', async () => {
  const r = await gwRequest(gwPort, { path: '/' });
  assert.equal(r.status, 200, `attendu 200 sur /, reçu ${r.status}`);
  const j = JSON.parse(r.text);
  assert.equal(j.name, 'inferhub-failover');
  assert.equal(j.ok, true);
});
