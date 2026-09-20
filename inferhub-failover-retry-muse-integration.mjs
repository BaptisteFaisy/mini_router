// Test d'INTÉGRATION : vrai fichier routeur + VRAIE config de production
// (`inferhub-failover.json`), seuls les endpoints (`upstream` et la base du
// provider `meta`) sont redirigés vers des mocks locaux. Aucun appel réel
// vers Inferhub ou Muse n'est effectué.
//
// Objectif : prouver sur la configuration RÉELLE (chains combo/cheap,
// secondChance, escalade, chainPasses=2, circuit, timeouts) que :
//   - le modèle de TÊTE de la vraie chaîne est appelé 2×, espacés de 1 s ;
//   - AUCUN autre modèle de la chaîne n'est essayé (ni 2e maillon, ni
//     secondChance, ni escalade, ni 2e passe) ;
//   - Muse sert la requête, et le client reçoit 200.
//
// Usage : node integration-real-config.mjs

import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const SCRIPTS = 'C:\\Users\\jeanp\\Documents\\Switch-PrepApp\\scripts';
const ROUTER_SRC = path.join(SCRIPTS, 'inferhub-failover.mjs');
const REAL_CFG = path.join(SCRIPTS, 'inferhub-failover.json');

const TMP_DIR = 'C:\\Users\\jeanp\\.minimax\\tmp\\router-integration';
const CFG_FILE = path.join(TMP_DIR, 'integration.json');
const ROUTER_COPY = path.join(TMP_DIR, 'inferhub-failover-under-test.mjs');

const PORT_PROXY = 18140;
const PORT_PRIMARY = 18141;
const PORT_MUSE = 18142;

const posts = [];   // appels chat (POST) : { source, model, t }
const gets = [];    // sondes /models
let routerStderr = '';

const json = (res, status, obj) => {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
};

const primary = http.createServer((req, res) => {
  const url = (req.url || '').split('?')[0];
  if (req.method === 'GET') { gets.push({ source: 'inferhub', url, t: Date.now() }); return json(res, 200, { data: [] }); }
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    let model = '';
    try { model = JSON.parse(raw)?.model || ''; } catch {}
    posts.push({ source: 'inferhub', model, t: Date.now() });
    // erreur TRANSITOIRE (400, ni morte ni panne provider) → doit déclencher le retry
    return json(res, 400, { error: { message: 'upstream error' } });
  });
});

const muse = http.createServer((req, res) => {
  const url = (req.url || '').split('?')[0];
  if (req.method === 'GET') { gets.push({ source: 'meta', url, t: Date.now() }); return json(res, 200, { data: [] }); }
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    let model = '';
    try { model = JSON.parse(raw)?.model || ''; } catch {}
    posts.push({ source: 'meta', model, t: Date.now() });
    return json(res, 200, {
      choices: [{ message: { role: 'assistant', content: 'muse-reponse' } }],
      model, usage: { prompt_tokens: 10, completion_tokens: 5 },
    });
  });
});

let pass = 0, fail = 0;
const check = (n, c, e = '') => { if (c) { pass++; console.log(`PASS ${n}`); } else { fail++; console.log(`FAIL ${n} ${e}`); } };
const waitHealthz = async (ms) => {
  const dl = Date.now() + ms;
  while (Date.now() < dl) {
    try { const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/healthz`); if (r.ok) return true; } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  return false;
};

let proxy = null;
try {
  const cfg = JSON.parse(fs.readFileSync(REAL_CFG, 'utf8'));
  console.log(`INFO config réelle : attemptsPerModel=${cfg.attemptsPerModel} retryDelayMs=${cfg.retryDelayMs} skipChainOnError=${cfg.skipChainOnError} chainPasses=${cfg.chainPasses}`);
  const headModel = cfg.chains['combo/cheap'][0];
  console.log(`INFO tête de chaîne combo/cheap = ${headModel}`);

  // Seules les ADRESSES changent. `key` (staticKey) évite de dépendre de la
  // vraie clé Muse tout en gardant l'en-tête Authorization envoyé.
  cfg.upstream = `http://127.0.0.1:${PORT_PRIMARY}`;
  const meta = cfg.providers.find((p) => p.name === 'meta');
  meta.base = `http://127.0.0.1:${PORT_MUSE}`;
  meta.key = 'test-key';

  fs.mkdirSync(TMP_DIR, { recursive: true });
  fs.writeFileSync(CFG_FILE, JSON.stringify(cfg, null, 2), 'utf8');
  fs.copyFileSync(ROUTER_SRC, ROUTER_COPY);

  await new Promise((r) => primary.listen(PORT_PRIMARY, '127.0.0.1', r));
  await new Promise((r) => muse.listen(PORT_MUSE, '127.0.0.1', r));

  proxy = spawn(process.execPath, [ROUTER_COPY], {
    stdio: ['ignore', 'ignore', 'pipe'],
    env: { ...process.env, FAILOVER_PORT: String(PORT_PROXY), FAILOVER_CONFIG: CFG_FILE },
  });
  proxy.stderr.on('data', (c) => (routerStderr += c.toString()));

  check('routeur démarré (config réelle)', await waitHealthz(40000), 'timeout 40 s');

  const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/v1/chat/completions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'combo/cheap', messages: [{ role: 'user', content: 'ping' }] }),
  });
  const text = await r.text();

  const prim = posts.filter((p) => p.source === 'inferhub');
  const museCalls = posts.filter((p) => p.source === 'meta');
  const d = prim.length >= 2 ? prim[1].t - prim[0].t : -1;

  console.log(`INFO séquence POST = ${posts.map((p) => `${p.source}:${p.model}`).join(' , ')}`);

  check('le modèle de tête de la vraie chaîne est appelé 2×',
    prim.length === 2 && prim.every((p) => p.model === headModel),
    `appels=${prim.length} modèles=${JSON.stringify(prim.map((p) => p.model))}`);
  check('les 2 essais sont espacés de ~1 s (>= 950 ms)', d >= 950, `délai=${d}ms`);
  check('AUCUN autre modèle de la chaîne essayé (ni 2e maillon, ni secondChance, ni 2e passe)',
    prim.every((p) => p.model === headModel),
    `modèles=${JSON.stringify([...new Set(prim.map((p) => p.model))])}`);
  check('Muse a servi la requête', museCalls.length >= 1, `appels Muse=${museCalls.length}`);
  check('Muse a reçu la requête après l abandon du primaire',
    museCalls.length >= 1 && museCalls[0].t >= prim[prim.length - 1].t,
    'ordre temporel');
  check('client reçoit 200 + contenu Muse',
    r.status === 200 && text.includes('muse-reponse'), `status=${r.status} body=${text.slice(0, 120)}`);
} catch (err) {
  fail++;
  console.log(`FAIL harnais: ${err?.stack || err}`);
} finally {
  console.log(`TOTAL ${pass + fail} (${pass} pass, ${fail} fail)`);
  if (fail) console.log('--- stderr routeur ---\n' + routerStderr);
  try { proxy?.kill(); } catch {}
  await new Promise((r) => setTimeout(r, 200));
  for (const s of [primary, muse]) { try { s.close(); } catch {} }
  for (const f of [CFG_FILE, ROUTER_COPY, path.join(TMP_DIR, 'inferhub-failover-circuit.json')]) {
    try { fs.rmSync(f, { force: true }); } catch {}
  }
  process.exit(fail ? 1 : 0);
}
