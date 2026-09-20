// PROTECTION COMBO/CHEAP — NE PAS TOUCHER — DO NOT TOUCH.
// combo/cheap DOIT rester TOUJOURS DISPONIBLE et FONCTIONNELLE (directive JP
// 2026-09-19). Ce test verrouille le contrat : modele par defaut + chaine
// restauree automatiquement si la config l'oublie.
// Runner : `node --test test/combo-cheap.test.mjs` (sans dependance).
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
const REAL_CFG = path.join(HERE, '..', 'inferhub-failover.json');
const CIRCUIT_FILE = path.join(HERE, '..', 'inferhub-failover-circuit.json');
const DIRECT = 'cbcn/deepseek-v4.1-flash';

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

// Amont simule : l'alias routeur 'combo/cheap' est EN PANNE (500), les modeles
// directs repondent OK en echo du modele servi.
function startUpstream() {
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
      if (body.model === 'combo/cheap') {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end('{"error":{"message":"router combo/cheap down (simule)"}}');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        choices: [{ message: { role: 'assistant', content: `served-by:${body.model}` } }],
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

let upstream, gw, gwPort, gwStderr = '', cfgPath;

before(async () => {
  upstream = await startUpstream();
  gwPort = await freePort();
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}
  const cfg = {
    port: gwPort,
    upstream: `http://127.0.0.1:${upstream.port}/v1`,
    keyEnv: 'ROUTER_TEST_COMBOCHEAP_KEY_ABSENTE',
    attemptsPerModel: 1,
    chainPasses: 1,
    retryDelayMs: 1,
    fetchTimeoutMs: 3000,
    ttftTimeoutMs: 3000,
    maxConcurrent: 50,
    maxQueue: 100,
    chains: {}, // volontairement SANS combo/cheap : la passerelle doit la restaurer
    defaultBackups: [],
    secondChance: [],
    providers: [{
      name: 'dead',
      base: 'http://127.0.0.1:9/v1', // port ferme : secours hermetique (pas de reseau externe)
      key: 'test-key',
      models: ['never-used'],
      coolMs: 5000,
    }],
  };
  cfgPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'combo-cheap-test-')), 'gw.json');
  fs.writeFileSync(cfgPath, JSON.stringify(cfg));
  gw = spawn(process.execPath, [GATEWAY], {
    env: { ...process.env, FAILOVER_CONFIG: cfgPath, FAILOVER_PORT: String(gwPort) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  gw.stderr.on('data', (c) => { gwStderr += c.toString(); });
  const t0 = Date.now();
  for (;;) {
    try {
      const r = await gwRequest(gwPort, { path: '/v1/models' });
      if (r.status === 200) break;
    } catch {}
    if (Date.now() - t0 > 45000) {
      throw new Error(`passerelle jamais prete. stderr:\n${gwStderr.slice(-3000)}`);
    }
    await new Promise((r) => setTimeout(r, 250));
  }
});

after(async () => {
  try { gw.kill(); } catch {}
  try { upstream.srv.close(); } catch {}
  try { fs.unlinkSync(cfgPath); fs.rmdirSync(path.dirname(cfgPath)); } catch {}
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}
});

test('combo/cheap restauree : servie par un direct meme si le routeur est down', async () => {
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: 'combo/cheap', messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  assert.equal(r.status, 200, `attendu 200 via chaine restauree, recu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(r.text, new RegExp(`served-by:${DIRECT.replace('/', '\\/')}`), 'la reponse doit venir du modele direct restaure');
});

test('combo/cheap est le modele par defaut (sans champ model)', async () => {
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  assert.equal(r.status, 200, `attendu 200 via defaut combo/cheap, recu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(r.text, new RegExp(`served-by:${DIRECT.replace('/', '\\/')}`), 'le defaut doit servir combo/cheap restauree');
});

test('config livree : chains.combo/cheap fonctionnelle (directs puis routeur)', async () => {
  const cfg = JSON.parse(fs.readFileSync(REAL_CFG, 'utf8'));
  const chain = cfg?.chains?.['combo/cheap'];
  assert.ok(Array.isArray(chain), 'chains["combo/cheap"] doit exister dans inferhub-failover.json');
  assert.ok(chain.length >= 2, 'la chaine combo/cheap doit contenir au moins 2 maillons');
  assert.equal(chain[chain.length - 1], 'combo/cheap', "le routeur 'combo/cheap' doit etre le dernier maillon (ROUTER-LAST)");
  assert.ok(!String(chain[0]).startsWith('combo/'), 'le premier maillon doit etre un modele direct (pas le routeur)');
});
