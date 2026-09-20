// Harnais bout-en-bout autonome : NOUVELLE politique de retry / bascule du
// routeur `inferhub-failover.mjs`.
//
//   A. `skipChainOnError` : le primaire échoue (400 transitoire) → 2 essais du
//      MÊME modèle (espacés de `retryDelayMs`) → abandon du primaire → service
//      par le provider de secours Muse. Le modèle de `chains` n'est JAMAIS appelé.
//   B. le retry s'applique AUSSI aux providers de secours : primaire 2× puis
//      Muse 2× (espacés) → 502.
//   C. les morts déterministes (404) ne sont PAS rejouées : 1 seul appel, puis
//      bascule sur le modèle suivant de `chains` → 200.
//
// Isolation : le routeur persiste son disjoncteur dans
// `<__dirname>/inferhub-failover-circuit.json`. Pour ne PAS écrire dans le
// dossier de production (routeur vivant à côté), on exécute une COPIE du
// routeur dans le dossier temporaire autorisé. Le fichier livrable n'édite
// jamais `inferhub-failover.mjs` ni `inferhub-failover.json`.
//
// Usage : node inferhub-failover-retry-muse-test.mjs
// Sortie : PASS/FAIL par assertion, puis TOTAL, puis exit(0|1).

import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROUTER_SRC = path.join(__dirname, 'inferhub-failover.mjs');

// Dossier temporaire autorisé (jamais le Bureau, jamais le dossier du routeur).
const TMP_DIR = path.join(os.homedir(), '.minimax', 'tmp', 'router-retry-muse');
const CFG_FILE = path.join(TMP_DIR, 'retry-muse-test.json');
const ROUTER_COPY = path.join(TMP_DIR, 'inferhub-failover-under-test.mjs');

const PORT_PROXY = 18110;   // routeur (18100 = prod vivante, 18102/18103 = harnais existant)
const PORT_PRIMARY = 18111; // mock Inferhub (primaire)
const PORT_MUSE = 18112;    // mock provider de secours Muse

const M_PRIMARY = 'test/primary';        // modèle demandé, échoue côté primaire
const M_CHAIN_BACKUP = 'test/chain-backup'; // repli déclaré dans `chains` : ne doit JAMAIS être appelé
const M_DEAD = 'test/dead';              // 404 déterministe
const M_ALIVE = 'test/alive';            // 200 côté primaire
const M_MUSE = 'muse-spark-1.3-contributor'; // modèle servi par le provider de secours

const calls = [];        // [{ source: 'inferhub'|'meta', model, t }]
let museFail = false;    // scénario B : le provider Muse échoue aussi
let routerStderr = '';

const json = (res, status, obj) => {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
};
const okBody = (model, content) => ({ choices: [{ message: { role: 'assistant', content } }], model });

// Lit le corps JSON puis délègue au handler métier.
function readJson(req, res, handler) {
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    let model = '';
    try { model = JSON.parse(raw)?.model || ''; } catch {}
    handler(model);
  });
}

// ---- Mock primaire (Inferhub) -------------------------------------------
const primary = http.createServer((req, res) => {
  const url = (req.url || '').split('?')[0];
  // Le routeur sonde /models au démarrage (lecture des prix) : réponse vide.
  if (req.method === 'GET' && (url === '/models' || url === '/v1/models')) return json(res, 200, { data: [] });
  if (req.method !== 'POST' || !url.endsWith('/chat/completions')) return json(res, 404, { error: { message: 'not found' } });
  readJson(req, res, (model) => {
    calls.push({ source: 'inferhub', model, t: Date.now() });
    if (model === M_DEAD) return json(res, 404, { error: { message: 'model not found' } });
    if (model === M_ALIVE || model === M_CHAIN_BACKUP) return json(res, 200, okBody(model, `ok-${model}`));
    // erreur transitoire (ni morte, ni panne provider) : doit déclencher le retry
    return json(res, 400, { error: { message: 'upstream error' } });
  });
});

// ---- Mock provider de secours (Muse) ------------------------------------
const muse = http.createServer((req, res) => {
  const url = (req.url || '').split('?')[0];
  if (req.method === 'GET' && (url === '/models' || url === '/v1/models')) return json(res, 200, { data: [] });
  if (req.method !== 'POST' || !url.endsWith('/chat/completions')) return json(res, 404, { error: { message: 'not found' } });
  readJson(req, res, (model) => {
    calls.push({ source: 'meta', model, t: Date.now() });
    if (museFail) return json(res, 400, { error: { message: 'upstream error' } });
    return json(res, 200, okBody(model, 'muse-ok'));
  });
});

// ---- Config temporaire ---------------------------------------------------
// Modèle demandé VOLONTAIREMENT absent de providers[].models : sinon le routeur
// court-circuite le primaire (comportement « direct provider ») et la bascule
// ne serait pas testée.
const cfg = {
  port: PORT_PROXY,
  upstream: `http://127.0.0.1:${PORT_PRIMARY}`,
  keyEnv: 'INFERHUB_API_KEY',
  forwardHeaders: [],
  attemptsPerModel: 2,   // 2 essais du MÊME modèle avant bascule
  retryDelayMs: 1000,    // délai entre les deux essais (assertion >= 950 ms)
  skipChainOnError: true, // NOUVEAU : échec d'un modèle → abandon du primaire
  chainPasses: 1,
  chains: {
    [M_PRIMARY]: [M_CHAIN_BACKUP],
    [M_DEAD]: [M_ALIVE],
  },
  defaultBackups: [],
  secondChance: [],
  escalation: { maxModels: 0 },
  // Disjoncteur neutralisé (fails très haut) pour ne pas sauter de modèle en
  // cours de scénario ; l'état vit dans le dossier temporaire, pas en prod.
  circuit: { fails: 99, windowMs: 60000, coolMs: 1000, slowTps: 0, slowCoolMs: 1000, slowMinTokens: 100000 },
  providers: [
    { name: 'meta', base: `http://127.0.0.1:${PORT_MUSE}`, models: [M_MUSE], noAuth: true, coolMs: 45000 },
  ],
};

let pass = 0;
let fail = 0;
function check(name, cond, extra = '') {
  if (cond) { pass++; console.log(`PASS ${name}`); }
  else { fail++; console.log(`FAIL ${name} ${extra}`); }
}
const seq = () => calls.map((c) => `${c.source}:${c.model}`).join(',');
const ofSource = (src) => calls.filter((c) => c.source === src);

async function chat(model) {
  const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, messages: [{ role: 'user', content: 'ping' }] }),
  });
  return { status: r.status, text: await r.text() };
}

async function waitHealthz(timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/healthz`);
      if (r.ok) return true;
    } catch {}
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

const listen = (srv, port) => new Promise((res) => srv.listen(port, '127.0.0.1', res));

let proxy = null;
try {
  fs.mkdirSync(TMP_DIR, { recursive: true });
  fs.writeFileSync(CFG_FILE, JSON.stringify(cfg, null, 2), 'utf8');

  // Copie du routeur sous test (isolation du fichier de disjoncteur).
  fs.copyFileSync(ROUTER_SRC, ROUTER_COPY);

  // Mocks D'ABORD, puis le routeur (il sonde /models au boot).
  await listen(primary, PORT_PRIMARY);
  await listen(muse, PORT_MUSE);

  proxy = spawn(process.execPath, [ROUTER_COPY], {
    stdio: ['ignore', 'ignore', 'pipe'],
    env: { ...process.env, FAILOVER_PORT: String(PORT_PROXY), FAILOVER_CONFIG: CFG_FILE },
  });
  proxy.stderr.on('data', (c) => (routerStderr += c.toString()));

  if (!(await waitHealthz())) {
    console.log(`FAIL routeur: /healthz ne répond pas sur 127.0.0.1:${PORT_PROXY} après 15 s`);
    console.log('--- stderr routeur ---\n' + routerStderr);
    process.exitCode = 1;
  } else {
    // ---------------- Scénario A : bascule directe sur Muse ----------------
    museFail = false;
    calls.length = 0;
    let r = await chat(M_PRIMARY);
    const seqA = seq();
    const primA = ofSource('inferhub');
    check('A: séquence inferhub×2 puis meta', seqA === `inferhub:${M_PRIMARY},inferhub:${M_PRIMARY},meta:${M_MUSE}`, `observé=${seqA}`);
    check('A: modèle de chains jamais appelé', !seqA.includes(M_CHAIN_BACKUP), `observé=${seqA}`);
    const dA = primA.length >= 2 ? primA[1].t - primA[0].t : -1;
    check('A: délai entre les 2 essais primaire >= 950 ms', dA >= 950, `délai=${dA}ms`);
    check('A: client reçoit 200 + contenu Muse', r.status === 200 && r.text.includes('muse-ok'), `status=${r.status} body=${r.text.slice(0, 120)}`);

    // ---------------- Scénario B : retry côté provider de secours ----------
    museFail = true;
    calls.length = 0;
    r = await chat(M_PRIMARY);
    const primB = ofSource('inferhub');
    const museB = ofSource('meta');
    const dB = museB.length >= 2 ? museB[1].t - museB[0].t : -1;
    check('B: primaire appelé 2×', primB.length === 2 && primB.every((c) => c.model === M_PRIMARY), JSON.stringify(primB.map((c) => c.model)));
    check('B: Muse appelé 2×', museB.length === 2 && museB.every((c) => c.model === M_MUSE), JSON.stringify(museB.map((c) => c.model)));
    check('B: les 2 essais Muse espacés >= 950 ms', dB >= 950, `délai=${dB}ms`);
    check('B: client reçoit 502', r.status === 502, `status=${r.status} body=${r.text.slice(0, 120)}`);
    museFail = false;

    // ---------------- Scénario C : mort déterministe non rejouée ----------
    calls.length = 0;
    r = await chat(M_DEAD);
    const seqC = seq();
    const deadCalls = calls.filter((c) => c.model === M_DEAD);
    check('C: 404 déterministe appelé 1 seule fois', deadCalls.length === 1, `appels=${deadCalls.length}`);
    check('C: bascule sur le modèle suivant de chains', seqC === `inferhub:${M_DEAD},inferhub:${M_ALIVE}`, `observé=${seqC}`);
    check('C: client reçoit 200', r.status === 200 && r.text.includes(`ok-${M_ALIVE}`), `status=${r.status} body=${r.text.slice(0, 120)}`);
  }
} catch (err) {
  fail++;
  console.log(`FAIL harnais: ${err?.stack || err}`);
} finally {
  console.log(`TOTAL ${pass + fail} (${pass} pass, ${fail} fail)`);
  if (fail) console.log('--- stderr routeur (diagnostic) ---\n' + routerStderr);
  try { proxy?.kill(); } catch {}
  await new Promise((r) => setTimeout(r, 200));
  for (const s of [primary, muse]) { try { s.close(); } catch {} }
  try { fs.rmSync(TMP_DIR, { recursive: true, force: true }); } catch {}
  process.exit(fail ? 1 : 0);
}
