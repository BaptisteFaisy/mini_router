// Test durable : BUDGET TEMPS PAR PROVIDER.
// Contexte (19/09/2026) : Muse en `reasoning_effort=max` génère 4-5 k tokens de
// raisonnement AVANT le contenu — mesuré 42-50 s. Le budget global de 25 s le
// coupait systématiquement, le routeur le déclarait « down » puis renvoyait un
// 502 « tous les providers ont échoué » : Muse était inutilisable en max.
// Correctif : `timeoutMs` / `ttftTimeoutMs` déclarés sur un provider priment sur
// le défaut global (`CFG.fetchTimeoutMs` / `CFG.ttftTimeoutMs`).
//
// Ce test verrouille les 3 comportements, sans réseau externe :
//   1. `timeoutMs` du provider est bien appliqué (et non le défaut global) ;
//   2. `ttftTimeoutMs` du provider est bien appliqué en stream ;
//   3. un provider SANS budget garde le défaut global (pas de régression).
//
// Runner : `node --test test/router-provider-timeout.test.mjs` (sans dépendance).
//
// ⚠️ Le gateway écrit son état disjoncteur dans `<dir du script>/inferhub-failover-circuit.json`
// (chemin non configurable) et ce test le supprime avant/après. Lancé DANS le
// dossier `scripts/` du projet, il efface donc les cooldowns du routeur LIVE.
// Pour un run sans impact : copier `inferhub-failover.mjs` + ce fichier dans un
// dossier temporaire (ex. `<tmp>/gw/test/`) et lancer depuis là — le `__dirname`
// du gateway devient le dossier temporaire et l'état reste isolé.
// (Même contrainte que le test préexistant `router-failover.test.mjs`.)
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

// Budgets du test — courts pour que la suite reste rapide.
const GLOBAL_FETCH_MS = 1500; // défaut global (CFG.fetchTimeoutMs)
const GLOBAL_TTFT_MS = 1200; // défaut global (CFG.ttftTimeoutMs)
const PROV_FETCH_MS = 700; // budget du provider « muet »
const PROV_TTFT_MS = 600; // budget du provider « headers-only »

function freePort() {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const p = s.address().port;
      s.close(() => resolve(p));
    });
  });
}

// Provider muet : accepte la requête et ne répond JAMAIS (ni headers ni corps).
// Déclenche le fetch timeout.
function startMute() {
  const srv = http.createServer((req) => {
    req.resume();
    req.on('end', () => {});
  });
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

// Provider « headers-only » : répond 200 + en-têtes SSE, puis n'émet aucun chunk.
// Déclenche le watchdog TTFT (et non le fetch timeout).
function startHeadersOnly() {
  const srv = http.createServer((req, res) => {
    req.resume();
    req.on('end', () => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      res.flushHeaders?.();
      // puis plus rien : ni chunk, ni [DONE]
    });
  });
  return new Promise((resolve) => {
    srv.listen(0, '127.0.0.1', () => resolve({ srv, port: srv.address().port }));
  });
}

function gwRequest(port, { method = 'GET', path: p = '/', body = null } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body == null ? null : JSON.stringify(body);
    const req = http.request({
      host: '127.0.0.1', port, method, path: p,
      headers: { 'Content-Type': 'application/json', ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}) },
      timeout: 60000,
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

const MODEL_PROV = 'm-provider-timeout'; // provider AVEC timeoutMs
const MODEL_TTFT = 'm-ttft-timeout'; // provider AVEC ttftTimeoutMs
const MODEL_DEFAULT = 'm-default-timeout'; // provider SANS budget -> défaut global

let mute, headersOnly, deadUpstream, gw, gwPort, gwStderr = '', cfgPath;

before(async () => {
  mute = await startMute();
  headersOnly = await startHeadersOnly();
  // Upstream Inferhub simulé « mort » : port fermé -> échec immédiat, on ne
  // paie pas le budget global sur le chemin primaire.
  deadUpstream = await freePort();
  gwPort = await freePort();
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}

  const cfg = {
    port: gwPort,
    upstream: `http://127.0.0.1:${deadUpstream}/v1`,
    keyEnv: 'ROUTER_TEST_PRIMARY_KEY_ABSENTE',
    attemptsPerModel: 1,
    chainPasses: 1,
    retryDelayMs: 1,
    fetchTimeoutMs: GLOBAL_FETCH_MS,
    ttftTimeoutMs: GLOBAL_TTFT_MS,
    maxConcurrent: 50,
    maxQueue: 100,
    chains: { [MODEL_PROV]: [], [MODEL_TTFT]: [], [MODEL_DEFAULT]: [] },
    defaultBackups: [],
    secondChance: [],
    providers: [
      {
        name: 'prov-with-timeout',
        base: `http://127.0.0.1:${mute.port}/v1`,
        key: 'test-key',
        models: [MODEL_PROV],
        timeoutMs: PROV_FETCH_MS,
        ttftTimeoutMs: PROV_TTFT_MS,
      },
      {
        name: 'prov-with-ttft',
        base: `http://127.0.0.1:${headersOnly.port}/v1`,
        key: 'test-key',
        models: [MODEL_TTFT],
        timeoutMs: PROV_FETCH_MS,
        ttftTimeoutMs: PROV_TTFT_MS,
      },
      {
        name: 'prov-without-timeout',
        base: `http://127.0.0.1:${mute.port}/v1`,
        key: 'test-key',
        models: [MODEL_DEFAULT],
        // aucun timeoutMs / ttftTimeoutMs : doit retomber sur le global
      },
    ],
  };
  cfgPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'router-timeout-test-')), 'gw.json');
  fs.writeFileSync(cfgPath, JSON.stringify(cfg));
  gw = spawn(process.execPath, [GATEWAY], {
    env: { ...process.env, FAILOVER_CONFIG: cfgPath, FAILOVER_PORT: String(gwPort) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  gw.stderr.on('data', (c) => { gwStderr += c.toString(); });

  const t0 = Date.now();
  for (;;) {
    try {
      const r = await gwRequest(gwPort, { path: '/' });
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
  try { mute.srv.close(); } catch {}
  try { headersOnly.srv.close(); } catch {}
  try { fs.unlinkSync(cfgPath); fs.rmdirSync(path.dirname(cfgPath)); } catch {}
  try { fs.unlinkSync(CIRCUIT_FILE); } catch {}
});

test('timeoutMs du provider prime sur le défaut global', async () => {
  const t0 = Date.now();
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: MODEL_PROV, messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  const ms = Date.now() - t0;
  assert.equal(r.status, 502, `attendu 502 (provider muet), reçu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(
    r.text,
    new RegExp(`timeout ${PROV_FETCH_MS}ms`),
    `le budget du provider (${PROV_FETCH_MS} ms) doit être appliqué, pas le global (${GLOBAL_FETCH_MS} ms). Reçu: ${r.text.slice(0, 300)}`
  );
  assert.doesNotMatch(
    r.text,
    new RegExp(`timeout ${GLOBAL_FETCH_MS}ms`),
    'le défaut global ne doit PAS s’appliquer à un provider qui déclare timeoutMs'
  );
  // Garde-fou de vivacité : on doit couper bien avant le budget global.
  assert.ok(ms < GLOBAL_FETCH_MS + 1000, `coupure trop tardive (${ms} ms) — le budget provider ne semble pas appliqué`);
});

test('ttftTimeoutMs du provider prime sur le défaut global (stream)', async () => {
  const t0 = Date.now();
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: MODEL_TTFT, messages: [{ role: 'user', content: 'ping' }], stream: true },
  });
  const ms = Date.now() - t0;
  assert.equal(r.status, 502, `attendu 502 (aucun chunk SSE), reçu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(
    r.text,
    new RegExp(`TTFT timeout ${PROV_TTFT_MS}ms`),
    `le budget TTFT du provider (${PROV_TTFT_MS} ms) doit être appliqué, pas le global (${GLOBAL_TTFT_MS} ms). Reçu: ${r.text.slice(0, 300)}`
  );
  assert.ok(ms < GLOBAL_FETCH_MS + 1000, `coupure trop tardive (${ms} ms) — le budget TTFT provider ne semble pas appliqué`);
});

test('un provider sans budget garde le défaut global (pas de régression)', async () => {
  const r = await gwRequest(gwPort, {
    method: 'POST', path: '/v1/chat/completions',
    body: { model: MODEL_DEFAULT, messages: [{ role: 'user', content: 'ping' }], stream: false },
  });
  assert.equal(r.status, 502, `attendu 502, reçu ${r.status}: ${r.text.slice(0, 300)}`);
  assert.match(
    r.text,
    new RegExp(`timeout ${GLOBAL_FETCH_MS}ms`),
    `un provider sans timeoutMs doit conserver le défaut global (${GLOBAL_FETCH_MS} ms). Reçu: ${r.text.slice(0, 300)}`
  );
});
