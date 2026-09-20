// Auto-test : faux fournisseur pour vérifier « 2 tentatives par modèle avant
// bascule » + failover complet + erreur quelconque (502, error 200, SSE).
import http from 'node:http';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT_PROXY = 18102;
const PORT_MOCK = 18103;
const calls = [];

const mock = http.createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => (raw += c));
  req.on('end', () => {
    const parsed = JSON.parse(raw);
    const model = parsed.model;
    const wantsStream = !!parsed.stream;
    calls.push(model);
    const sse = (m) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n');
      res.write(`data: {"choices":[{"delta":{"content":"ok-${m}"}}]}\n\n`);
      res.write('data: [DONE]\n\n'); res.end();
    };
    const ok = (m) => ({ status: 200, body: JSON.stringify({ choices: [{ message: { role: 'assistant', content: 'ok-' + m } }], model: m }) });
    let r;
    if (model === 't502') r = { status: 502, body: '{"error":"bad gateway"}' };
    else if (model === 'terr200') r = { status: 200, body: '{"error":"chaîne épuisée"}' };
    else if (model === 'tsse-err') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"error":"boom"}\n\n'); res.end(); return;
    } else if (model === 'tsse-ok') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n');
      res.write('data: {"choices":[{"delta":{"content":"salut"}}]}\n\n');
      res.write('data: [DONE]\n\n'); res.end(); return;
    } else if (model === 'tbon') { if (wantsStream) { sse(model); return; } r = ok(model); }
    // [SECOURS-INDEPENDANT 20260920] « agnes-3.0-flash » est le modèle du
    // provider de SECOURS déclaré dans la config de test : il doit répondre OK.
    // Il renvoyait 404 « inconnu », ce qui rendait le chemin « abandon de la
    // chaîne → provider de secours » intestable (le client finissait en échec
    // pour une raison de décor, pas de routeur).
    else if (model === 'agnes-3.0-flash') { if (wantsStream) { sse(model); return; } r = ok(model); }
    else r = { status: 404, body: '{"error":"inconnu"}' };
    res.writeHead(r.status, { 'Content-Type': 'application/json' });
    res.end(r.body);
  });
});

// [ISOLATION 20260919] le routeur persistait son disjoncteur dans
// `<__dirname>/inferhub-failover-circuit.json` — c'est-à-dire dans le dossier de
// PRODUCTION. Conséquence observée : un `tbon` resté en cooldown par une
// exécution précédente était sauté, la requête partait sur le VRAI provider
// Agnes (appel facturé + assertion « tbon appelé » en échec). On exécute donc
// une COPIE du routeur dans un dossier temporaire (son fichier de disjoncteur
// y est recréé), et on déclare un provider Agnes factice pointant sur le mock :
// plus aucun appel réseau réel, plus aucune écriture en production.
const TMP_DIR = path.join(os.homedir(), '.minimax', 'tmp', 'router-selftest');
fs.mkdirSync(TMP_DIR, { recursive: true });
const ROUTER_COPY = path.join(TMP_DIR, 'inferhub-failover-under-test.mjs');
fs.copyFileSync(path.join(__dirname, 'inferhub-failover.mjs'), ROUTER_COPY);

const cfgFile = path.join(TMP_DIR, '.failover-selftest.json');
fs.writeFileSync(cfgFile, JSON.stringify({
  port: PORT_PROXY,
  upstream: `http://127.0.0.1:${PORT_MOCK}`,
  keyEnv: 'INFERHUB_API_KEY',
  forwardHeaders: [],
  attemptsPerModel: 2,
  retryDelayMs: 10,
  chains: {},
  defaultBackups: ['tbon'],
  // Provider de secours factice : sans cette clé, le routeur retombe sur le
  // VRAI Agnes (apihub.agnes-ai.com) et le test consommerait du quota réel.
  providers: [
    { name: 'agnes', base: `http://127.0.0.1:${PORT_MOCK}`, models: ['agnes-3.0-flash'], noAuth: true, coolMs: 45000 },
  ],
}));

const proxy = spawn(process.execPath, [ROUTER_COPY], {
  stdio: ['ignore', 'ignore', 'inherit'],
  env: { ...process.env, FAILOVER_PORT: String(PORT_PROXY), FAILOVER_CONFIG: cfgFile },
});
// [BOOT-WAIT 20260919] le routeur lit 2 clés via PowerShell au démarrage
// (lecture de variables UTILISATEUR) : mesuré 1,5 à 3 s. L'attente d'origine de
// 1,2 s laissait le routeur endormi et TOUT le harnais échouait en
// ECONNREFUSED sur 18102 — panne du test, pas du routeur (vérifié : l'ancienne
// version du routeur échouait exactement pareil). On attend le port, pas une
// durée fixe.
const bootDeadline = Date.now() + 30000;
let booted = false;
while (Date.now() < bootDeadline) {
  try {
    const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/healthz`);
    if (r.ok) { booted = true; break; }
  } catch {}
  await new Promise((r) => setTimeout(r, 100));
}
if (!booted) { console.log(`FAIL routeur: /healthz muet sur 127.0.0.1:${PORT_PROXY} après 30 s`); process.exit(1); }
mock.listen(PORT_MOCK, '127.0.0.1');

async function chat(model, stream = false) {
  const r = await fetch(`http://127.0.0.1:${PORT_PROXY}/v1/chat/completions`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, messages: [{ role: 'user', content: 'x' }], ...(stream ? { stream: true } : {}) }),
  });
  const t = await r.text();
  return { status: r.status, text: t };
}

let pass = 0, fail = 0;
function check(name, cond, extra = '') {
  if (cond) { pass++; console.log(`PASS ${name}`); } else { fail++; console.log(`FAIL ${name} ${extra}`); }
}

// [ASSERTION-SOLDEE 20260920] Attendu historique : « t502,t502,tbon » (bascule
// sur le modèle SUIVANT de la chaîne). Obsolète : un 502 est classé « panne
// PROVIDER » par isProviderOutage() → règle MULTI-PROVIDER délibérée (les autres
// modèles du même provider sont HS aussi). Après ATTEMPTS_PER_MODEL essais, la
// chaîne est donc abandonnée et le relais passe au provider de SECOURS
// indépendant : le modèle suivant de la chaîne (tbon) n'est jamais appelé.
// Comportement vérifié identique avant/après la modification « retry 1 s +
// bascule Muse » du chat 9 (aucune régression) : l'assertion était périmée.
// Contraste volontaire avec `terr200` ci-dessous : un corps d'erreur en HTTP 200
// est une panne de MODÈLE → la chaîne continue jusqu'à tbon.
calls.length = 0;
let r = await chat('t502');
check('t502 : 2 tentatives puis abandon de la chaîne (502 = panne provider)', calls.join(',') === 't502,t502,agnes-3.0-flash', JSON.stringify(calls));
check('t502 : le provider de secours sert la requête (200)', r.status === 200 && r.text.includes('ok-agnes-3.0-flash'), `${r.status} ${r.text.slice(0, 120)}`);

calls.length = 0;
r = await chat('terr200');
check('erreur 200 : 2 tentatives puis bascule', calls.join(',') === 'terr200,terr200,tbon', JSON.stringify(calls));

calls.length = 0;
r = await chat('tsse-err', true);
check('SSE erreur avant contenu : 2 tentatives puis bascule', calls.join(',') === 'tsse-err,tsse-err,tbon', JSON.stringify(calls));
check('SSE fallback : réponse JSON du tbon', r.status === 200 && r.text.includes('ok-tbon'), r.text.slice(0, 120));

calls.length = 0;
r = await chat('tsse-ok', true);
check('SSE nominal : 1 seule tentative', calls.join(',') === 'tsse-ok', JSON.stringify(calls));
check('SSE nominal : [DONE] transféré', r.text.includes('[DONE]') && r.text.includes('salut'));

calls.length = 0;
r = await chat('tbon');
check('nominal : aucun retry', calls.join(',') === 'tbon', JSON.stringify(calls));

console.log(`TOTAL ${pass + fail} (${pass} pass, ${fail} fail)`);
// [TEARDOWN 20260919] `process.exit` lancé juste après `proxy.kill()` faisait
// planter le runtime Node sous Windows (assertion libuv
// `!(handle->flags & UV_HANDLE_CLOSING)`, code de sortie 0xC0000409) : le
// résultat du test devenait illisible alors que les assertions étaient bonnes.
// On laisse le process enfant se terminer avant de sortir.
proxy.kill();
await new Promise((r) => setTimeout(r, 300));
try { mock.close(); } catch {}
try { fs.rmSync(cfgFile, { force: true }); } catch {}
process.exit(fail ? 1 : 0);
