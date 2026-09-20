#!/usr/bin/env node
// [INferhub-FAILOVER-v2] Passerelle OpenAI-compatible locale : Zed → ici →
// api.inferhub.dev. TOUT message d'erreur déclenche le modèle suivant de la
// chaîne : statut HTTP ≥400, corps 200 avec {"error":...}, réponse sans
// "choices", événement d'erreur dans le flux SSE, stream interrompu avant
// [DONE], erreur réseau. Config : inferhub-failover.json. Logs : stderr.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execSync } from 'node:child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CFG = JSON.parse(fs.readFileSync(process.env.FAILOVER_CONFIG || path.join(__dirname, 'inferhub-failover.json'), 'utf8'));
// FAILOVER_UPSTREAM permet de tester contre un faux fournisseur
if (process.env.FAILOVER_UPSTREAM) CFG.upstream = process.env.FAILOVER_UPSTREAM;

function loadRealKey() {
  // [KEY-USER-FIRST 20260915] la variable utilisateur fait foi ; process.env
  // du lanceur peut contenir une clé périodée (rotation).
  try {
    const u = execSync(
      `powershell -NoProfile -Command "[Environment]::GetEnvironmentVariable('${CFG.keyEnv}','User')"`,
      { encoding: 'utf8', timeout: 15000 }
    ).trim();
    if (u) return u;
  } catch {}
  if (process.env[CFG.keyEnv]) return process.env[CFG.keyEnv].trim();
  return '';
}
const REAL_KEY = loadRealKey();
function loadAgnesKey() {
  // [AGNES-KEY-USER-FIRST 20260915] la variable UTILISATEUR Windows fait foi
  // (c'est la source de vérité — Zed et moi la lisons comme ça). process.env
  // du lanceur peut contenir une CLE PÉRIODÉE (rotation sk-fMr4E→sk-6Yz67mz
  // constatée 23h00 : la passerelle héritait de la vieille clé du process
  // parent et Agnes renvoyait 401 « invalid api key » à chaque fois).
  try {
    const k = execSync(
      `powershell -NoProfile -Command "[Environment]::GetEnvironmentVariable('AGNES_API_KEY','User')"`,
      { encoding: 'utf8', timeout: 15000 }
    ).trim();
    if (k) return k;
  } catch {}
  return (process.env.AGNES_API_KEY || '').trim();
}
function loadEnvKey(envName) {
  // [MULTI-PROVIDER 20260919] lecture générique : variable UTILISATEUR
  // d'abord (rotation), puis process.env — même règle que les clés historiques.
  if (!envName) return '';
  try {
    const u = execSync(
      `powershell -NoProfile -Command "[Environment]::GetEnvironmentVariable('${envName}','User')"`,
      { encoding: 'utf8', timeout: 15000 }
    ).trim();
    if (u) return u;
  } catch {}
  return (process.env[envName] || '').trim();
}

function chainFor(model) {
  const backups = CFG.chains[model] ?? CFG.defaultBackups ?? [];
  // [ROUTER-LAST 20260915] combo/* est un meta-routeur lent (11-12 s TTFT,
  // il re-route lui-meme vers gemini). On tente d'abord les modeles directs
  // (qwen3.8-max ~4 s en tete), le routeur ne sert que de dernier recours.
  if (String(model).startsWith('combo/')) {
    const dedup = backups.filter((m) => m !== model);
    return [...dedup, model];
  }
  return [model, ...backups.filter((m) => m !== model)];
}

// [ESSAIS-PAR-MODELE] nombre de tentatives DU MÊME modèle avant de passer au
// suivant de la chaîne (défaut 2). Un stream déjà engagé (contenu envoyé au
// client) n'est jamais repris : on passe directement au modèle suivant.
const ATTEMPTS_PER_MODEL = Math.max(1, Number(CFG.attemptsPerModel ?? 2));
const RETRY_DELAY_MS = Number(CFG.retryDelayMs ?? 1000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// [RETRY-1S-MUSE 20260919] Politique demandée : un fournisseur renvoie une
// erreur → nouvel essai du MÊME modèle après RETRY_DELAY_MS (1 s) → si l'erreur
// persiste, on abandonne le fournisseur en échec et on bascule DIRECTEMENT sur
// Muse (provider `meta`), sans parcourir le reste de la chaîne Inferhub (ni
// seconde chance, ni escalade prix). Le retry s'applique à TOUS les points
// d'appel : chaîne principale (boucle `a`), seconde chance, escalade et
// providers de secours — via `attemptWithRetry`.
// Opt-in explicite (`skipChainOnError: true` dans la config) : sans la clé, le
// comportement historique est conservé à l'identique.
const SKIP_CHAIN_ON_ERROR = CFG.skipChainOnError === true;

function log(...a) { process.stderr.write(`[${new Date().toISOString()}] ${a.join(' ')}\n`); }
// [AGNES-DIAG-20260915] Empreinte de la clé Agnes effective : le prefix 8 +
// la longueur suffisent pour comparer avec la clé USER à l'instant T si
// Agnes renvoie 401 (sans jamais imprimer la clé complète).
function agnesKeyFp() {
  if (!agnesKey) return 'VIDE';
  return `${agnesKey.slice(0, 8)}…(${agnesKey.length})`;
}

// [AGNES-DEBUG 20260915] diagnostic temporaire du 401 "invalid api key" :
// on loggue le prefixe + longueur + hash court de la clé effective ENVOYEE,
// sans jamais imprimer la clé complete.
function agnesKeyFingerprint(key) {
  if (!key) return 'VIDE';
  try {
    const h = require('node:crypto').createHash('sha256').update(String(key)).digest('hex').slice(0, 8);
    return `${String(key).slice(0, 6)}…(${String(key).length} chars, sha256:${h})`;
  } catch { return `${String(key).slice(0, 6)}…(${String(key).length} chars)`; }
}

// [ANTI-CRASH 20260915] la passerelle ne doit JAMAIS mourir en silence (constaté
// 22h53 : process disparu en pleine rafale, log coupé net, port fermé → Zed
// sans réponse). On loggue et on survit : le proxy est stateless, les slots
// pool sont once-guardés, une requête isolée peut échouer sans tuer le service.
process.on('unhandledRejection', (err) => log('UNHANDLED-REJECTION (survie):', err?.stack || String(err)));
process.on('uncaughtException', (err) => log('UNCAUGHT-EXCEPTION (survie):', err?.stack || String(err)));

// [FAIL-FAST-413] un 413/request_too_large est deterministe : le meme body
// renvoye aux autres modeles echouera pareil (limite gateway Inferhub, pas
// le modele). On aborte la chaine immediatement avec un 413 explicite cote
// Zed au lieu de 12 appels voues a l'echec.
// [FAIL-FAST-CONTEXTE 20260915] idem quand l'input depasse le max du PLUS GROS
// maillon de la chaine (« exceeds the maximum number of tokens allowed N »
// avec N >= maxChainTokens) : aucun modele ne pourra l'accepter, les
// 5 modeles x 2 passes + 4 retries Zed sont tous voues a l'echec. En revanche
// si N < maxChainTokens (ex. qwen 262k refuse, gemini 1M accepterait), on
// CONTINUE le failover vers les plus gros modeles.
const MAX_CHAIN_TOKENS = Number(CFG.maxChainTokens ?? 1048576);
function isBodyTooLarge(err) {
  const s = String(err || '');
  if (/\b413\b|request_too_large|request body exceeds configured limit/i.test(s)) return true;
  if (/exceeds the maximum number of tokens|maximum number of tokens allowed/i.test(s)) {
    const m = /allowed\s*(\d{4,})/i.exec(s);
    const lim = m ? Number(m[1]) : Infinity;
    if (lim >= MAX_CHAIN_TOKENS) return true;
  }
  return false;
}

// [SKIP-MODELE-MORT] erreur deterministe PAR MODELE (403 disabled, 404,
// 401, 402 no_provider_under_bid = aucun provider routable pour ce modele
// avec l'enchere actuelle) : le meme modele re-essaye echouera pareil, on
// saute directement au modele suivant (contraire au 413 qui aborte tout).
function isModelDead(err) {
  return /\b403\b.*disabled|\b404\b|\b401\b|model disabled in your preferences|no_provider_under_bid|no provider within|within your max-per-mtok bid|no available provider|all offers exhausted|exhausted or in cooldown/i.test(String(err || ''));
}
// [TIMEOUT-FAILOVER 20260919] timeout lent (fetch/TTFT) = modèle lent,
// PAS provider mort. Signaux émis par attemptUpstream : `timeout ...ms sur
// ...` (fetch) et `TTFT timeout ...` (stream sans chunk). Sert à basculer au
// modèle suivant sans re-essai + skip disjoncteur immédiat.
function isSlowTimeout(err) {
  return /^(timeout|TTFT)|ETIMEDOUT|timed out/i.test(String(err || ''));
}

// [POOL-CONCURRENCY] sémaphore bornant les fetchs simultanés vers l'upstream
// (défaut 4, configurable via CFG.maxConcurrent). Au-delà, la requête attend
// dans la file — évite de submerger api.inferhub.dev quand Zed martèle.
const POOL_MAX = Math.max(1, Number(CFG.maxConcurrent ?? 4));
let poolActive = 0;
const poolQueue = [];
function poolAcquire() {
  if (poolActive < POOL_MAX) { poolActive++; return Promise.resolve(); }
  return new Promise((resolve) => poolQueue.push(resolve));
}
function poolRelease() {
  if (poolQueue.length) poolQueue.shift()();
  else poolActive--;
}
// [FETCH-TIMEOUT] timeout par tentitive d'upstream (défaut 25 s, CFG.fetchTimeoutMs)
// [TTFT-TIMEOUT 20260915] un upstream peut repondre 200 (headers) puis ne
// jamais envoyer de chunk SSE (stall). Sans garde, Zed attend indefiniment.
// On aborte si aucun chunk valide sous CFG.ttftTimeoutMs (defaut 20 s).
const FETCH_TIMEOUT_MS = Number(CFG.fetchTimeoutMs ?? 25000);
const TTFT_TIMEOUT_MS = Number(CFG.ttftTimeoutMs ?? 20000);

// un corps 200 est une ERREUR s'il contient error / pas de choices / message
// d'échec lisible
function bodyIsError(status, text, wantsStream) {
  if (status >= 400) return true;
  let obj;
  try { obj = JSON.parse(text); } catch { return /"error"|chain exhausted|balance too low/i.test(text); }
  if (obj.error) return true;
  if (wantsStream) return false; // les vrais clients stream ne passent pas ici
  if (!Array.isArray(obj.choices)) return true;
  return false;
}

// [VITESSE 20260919] inspecteur SSE single-parse : UN trim + UN JSON.parse
// par ligne (avant : jusqu'à 2 parses + 3 regex via sseLineError +
// sseLineHasContent + goodChunk). Retour : 0 rien, 1 erreur, 2 chunk valide
// (choices, ex-goodChunk), 3 contenu-sans-choices (ex-sseLineHasContent).
// Sémantique strictement identique, ordre de priorité inchangé (erreur > tout).
const SSE_ERR_RX = /"error"\s*:|chain exhausted|balance too low/i;
const SSE_CONTENT_RX = /"content"\s*:\s*"[^"]|delta"?\s*:\s*\{[^}]*content/;
function sseInspect(line) {
  const t = line.trim();
  if (!t.startsWith('data:')) return 0;
  const payload = t.slice(5).trim();
  if (!payload || payload === '[DONE]') return 0;
  let o = null, parsed = true;
  try { o = JSON.parse(payload); } catch { parsed = false; }
  if (parsed) {
    const isObj = o !== null && typeof o === 'object';
    if (isObj && (o.error || o.type === 'error')) return 1;
    if (isObj && Array.isArray(o.choices) && !o.error) return 2;
    return (SSE_CONTENT_RX.test(t) || t.includes('"role"')) ? 3 : 0;
  }
  if (SSE_ERR_RX.test(payload)) return 1;
  return (SSE_CONTENT_RX.test(t) || t.includes('"role"')) ? 3 : 0;
}

// [USAGE-QUOTA 20260919] extraction LOCALE des tokens consommés. L'API Muse
// n'expose AUCUN endpoint d'usage/quota (/usage, /quota, /me, /credits… → 404)
// et aucun en-tête de quota : la seule source fiable est le bloc `usage` de la
// réponse (non-stream) ou d'un chunk SSE final. En son absence on ESTIME
// (~4 caractères/token) et on marque le relevé `estimated` pour rester traçable.
function usageOf(text) {
  try {
    const u = JSON.parse(text)?.usage;
    if (!u || typeof u !== 'object') return null;
    const p = Number(u.prompt_tokens ?? u.input_tokens ?? 0) || 0;
    const c = Number(u.completion_tokens ?? u.output_tokens ?? 0) || 0;
    const t = Number(u.total_tokens ?? 0) || (p + c);
    if (!p && !c && !t) return null;
    return { prompt_tokens: p, completion_tokens: c, total_tokens: t, estimated: 0 };
  } catch { return null; }
}
// estimation complétion (non-stream) : longueur des contenus de `choices`
function estCompletionOf(text) {
  try {
    const o = JSON.parse(text);
    let n = 0;
    for (const ch of (o.choices || [])) {
      const m = ch?.message ?? ch?.delta ?? {};
      n += String(m.content ?? '').length;
      if (Array.isArray(m.tool_calls)) n += JSON.stringify(m.tool_calls).length;
    }
    return Math.round(n / 4);
  } catch { return 0; }
}
// relevé estimé (aucun `usage` amont disponible) — `estimated: 1` compte le recours
function estUsage(fwdBody, compTokens) {
  const p = estimateTokens(fwdBody);
  const c = Math.max(0, Number(compTokens) || 0);
  return { prompt_tokens: p, completion_tokens: c, total_tokens: p + c, estimated: 1 };
}
// usage porté par une ligne SSE (`"usage"` est rare : on ne paie le parse que là)
function sseUsage(line) {
  if (!line.includes('"usage"')) return null;
  const t = line.trim();
  if (!t.startsWith('data:')) return null;
  const payload = t.slice(5).trim();
  if (!payload || payload === '[DONE]') return null;
  return usageOf(payload);
}

// ===================== [STATS-TOKENS 20260919] =====================
// Compteurs EN MÉMOIRE (aucune I/O, aucun JSON.parse ajouté sur le chemin
// chaud) + endpoint GET /stats pour l'app Rust locale : consommation de tokens,
// erreurs d'API classées, état des disjoncteurs.
// Bornes mémoire OBLIGATOIRES : ring buffer 200 erreurs max, map des modèles
// plafonnée à 200 entrées (au-delà on évince l'entrée dont `last_ts` est le plus
// ancien), `msg` tronqué à 300 caractères. Aucune ligne de log ajoutée sur le
// succès (le log stderr est déjà volumineux et surveillé) ; les erreurs sont
// déjà logguées par les appelants, on ne double-logge pas.
const ST_ERR_RING_MAX = 200;
const ST_MODELS_MAX = 200;
const ST_MSG_MAX = 300;
// enum FERMÉE des causes d'échec (contrat /stats)
const ST_KINDS = ['timeout', 'http_400', 'http_401', 'http_403', 'http_404', 'http_413', 'http_429', 'http_5xx',
  'network', 'sse', 'empty_stream', 'stream_cut', 'client_gone', 'context_too_large', 'other'];
const stats = {
  requests: 0, served: 0, failed: 0,
  attempts: 0, attempt_ok: 0, attempt_err: 0, timeouts: 0,
  tokens_in: 0, tokens_out: 0, tokens_estimated: 0,
  providers: {},      // name -> {attempts, ok, errors, timeouts, tokens_in, tokens_out}
  models: new Map(),  // model -> {…} (plafonné à ST_MODELS_MAX)
  errors: [],         // ring buffer, plus récentes EN PREMIER (ST_ERR_RING_MAX)
};
function stProvider(name) {
  let p = stats.providers[name];
  if (!p) p = stats.providers[name] = { attempts: 0, ok: 0, errors: 0, timeouts: 0, tokens_in: 0, tokens_out: 0 };
  return p;
}
function stProviderNames() { return ['inferhub', ...PROVIDERS.map((p) => p.name)]; }
// provider RÉEL d'une tentative : l'objet `prov` s'il est fourni, sinon la base
// appelée (agnes-ai → `agnes`), sinon le primaire `inferhub`.
function stProviderOf(prov, upstreamBase) {
  if (prov && prov.name) return String(prov.name);
  if (upstreamBase && String(upstreamBase).includes('agnes-ai')) return 'agnes';
  return 'inferhub';
}
function stModel(model, provider) {
  let m = stats.models.get(model);
  if (!m) {
    if (stats.models.size >= ST_MODELS_MAX) {
      // éviction : l'entrée dont `last_ts` est le plus ancien
      let victim = null, victimTs = Infinity;
      for (const [k, v] of stats.models) if (v.last_ts < victimTs) { victimTs = v.last_ts; victim = k; }
      if (victim !== null) stats.models.delete(victim);
    }
    m = {
      model, provider, attempts: 0, ok: 0, errors: 0, timeouts: 0,
      tokens_in: 0, tokens_out: 0, estimated: 0, sum_ms: 0, last_ms: 0, last_ts: 0, last_err: '',
    };
    stats.models.set(model, m);
  }
  if (provider) m.provider = provider;
  return m;
}
// `kind` (enum fermée) + statut HTTP, déduits du texte d'erreur réellement
// observé dans le log (timeout Nms, TTFT timeout, 502 body, stream mort/coupé,
// event erreur SSE, stream vide/inachevé, client parti…).
function stClassify(err) {
  const s = String(err || '');
  const m = /^\s*(\d{3})\b/.exec(s);
  const status = m ? Number(m[1]) : null;
  if (/client parti avant traitement/i.test(s)) return { kind: 'client_gone', status };
  if (/TTFT timeout|timeout \d+ms/i.test(s)) return { kind: 'timeout', status };
  if (/exceeds the maximum number of tokens|maximum number of tokens allowed|context_too_large|request_too_large/i.test(s)) {
    return { kind: /request_too_large|\b413\b/i.test(s) ? 'http_413' : 'context_too_large', status };
  }
  if (/^réseau|fetch failed|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|ETIMEDOUT|socket hang up|operation was aborted/i.test(s)) return { kind: 'network', status };
  if (/event erreur SSE|erreur SSE/i.test(s)) return { kind: 'sse', status };
  if (/stream vide|stream terminé sans chunk valide/i.test(s)) return { kind: 'empty_stream', status };
  if (/stream mort avant commit|stream coupé|stream inachevé|pas de \[DONE\]/i.test(s)) return { kind: 'stream_cut', status };
  if (status === null) return { kind: 'other', status };
  if (status === 400) return { kind: 'http_400', status };
  if (status === 401) return { kind: 'http_401', status };
  if (status === 403) return { kind: 'http_403', status };
  if (status === 404) return { kind: 'http_404', status };
  if (status === 413) return { kind: 'http_413', status };
  if (status === 429) return { kind: 'http_429', status };
  if (status >= 500) return { kind: 'http_5xx', status };
  return { kind: 'other', status };
}
function stPushError(e) {
  stats.errors.unshift(e);
  if (stats.errors.length > ST_ERR_RING_MAX) stats.errors.length = ST_ERR_RING_MAX;
}
// enregistre UNE tentative upstream (succès OU échec). Tokens : on réutilise le
// bloc `usage` déjà produit par la tentative (réel, sinon estimation marquée
// `estimated`) — aucun parse supplémentaire ; un échec ne compte AUCUN token.
function stRecord(model, prov, upstreamBase, r, ms) {
  const name = stProviderOf(prov, upstreamBase);
  const P = stProvider(name);
  const M = stModel(model, name);
  const now = Date.now();
  stats.attempts++; P.attempts++; M.attempts++;
  M.last_ms = Math.round(ms); M.sum_ms += ms; M.last_ts = now;
  if (r.ok) {
    stats.attempt_ok++; P.ok++; M.ok++;
    const u = r.usage || null;
    const ti = u ? Math.round(Number(u.prompt_tokens) || 0) : 0;
    const to = u ? Math.round(Number(u.completion_tokens) || 0) : 0;
    stats.tokens_in += ti; stats.tokens_out += to;
    P.tokens_in += ti; P.tokens_out += to;
    M.tokens_in += ti; M.tokens_out += to;
    if (u && u.estimated) { const e = ti + to; stats.tokens_estimated += e; M.estimated += e; }
    return;
  }
  stats.attempt_err++; P.errors++; M.errors++;
  const cls = stClassify(r.err);
  if (cls.kind === 'timeout') { stats.timeouts++; P.timeouts++; M.timeouts++; }
  M.last_err = String(r.err || '').slice(0, ST_MSG_MAX);
  stPushError({ ts: now, model, provider: name, kind: cls.kind, status: cls.status, msg: String(r.err || '').slice(0, ST_MSG_MAX) });
}
// `requests` à l'entrée du handler chat/completions, `served`/`failed` à la FIN
// de la requête CLIENT : on s'accroche à la réponse HTTP (aucun `return` du
// handler à instrumenter → aucun chemin oublié, y compris les retours en boucle).
function stRequestStart(res) {
  stats.requests++;
  let done = false;
  const fin = () => {
    if (done) return;
    done = true;
    if (res.writableFinished && res.statusCode >= 200 && res.statusCode < 400) stats.served++;
    else stats.failed++;
  };
  res.on('finish', fin);
  res.on('close', fin);
}
// charge utile de GET /stats (contrat consommé par l'app Rust locale)
function statsObj() {
  const now = Date.now();
  const providers = {};
  for (const name of stProviderNames()) {
    const e = stats.providers[name] || {};
    providers[name] = {
      down: provSkip(name),
      attempts: num(e.attempts), ok: num(e.ok), errors: num(e.errors), timeouts: num(e.timeouts),
      tokens_in: num(e.tokens_in), tokens_out: num(e.tokens_out),
    };
  }
  const models = [...stats.models.values()]
    .map((m) => ({
      model: m.model, provider: m.provider,
      attempts: num(m.attempts), ok: num(m.ok), errors: num(m.errors), timeouts: num(m.timeouts),
      tokens_in: num(m.tokens_in), tokens_out: num(m.tokens_out), estimated: num(m.estimated),
      avg_ms: m.attempts ? Math.round(m.sum_ms / m.attempts) : 0,
      last_ms: num(m.last_ms), last_ts: num(m.last_ts), last_err: m.last_err || '',
    }))
    .sort((a, b) => ((b.tokens_in + b.tokens_out) - (a.tokens_in + a.tokens_out)) || (b.attempts - a.attempts));
  const circuit = [...cbState].map(([model, s]) => ({ model, fails: num(s.fails), skip_until: num(s.skipUntil) }));
  return {
    ok: true,
    version: GW_VERSION,
    generated_at: now,
    uptime_s: Math.floor((now - GW_BOOT_TS) / 1000),
    pool: { active: poolActive, queued: poolQueue.length },
    totals: {
      requests: num(stats.requests), served: num(stats.served), failed: num(stats.failed),
      attempts: num(stats.attempts), attempt_ok: num(stats.attempt_ok), attempt_err: num(stats.attempt_err),
      timeouts: num(stats.timeouts),
      tokens_in: num(stats.tokens_in), tokens_out: num(stats.tokens_out),
      tokens_total: num(stats.tokens_in) + num(stats.tokens_out),
      tokens_estimated: num(stats.tokens_estimated),
    },
    providers,
    models,
    errors: stats.errors,
    circuit,
  };
}
// =================== fin [STATS-TOKENS 20260919] ===================

async function attemptUpstreamCore(model, fwdHeaders, body, res, started, upstreamBase, prov) {
  // renvoie {ok, forwarded, err}
// [AGNES-FALLBACK] `upstreamBase` (6e arg, `started` inutilisé en 5e) permet
// de pointer vers une autre API (ex. Agnes) sans toucher le reste du
// pipeline. ⚠️ Ne PAS passer la base en 5e : elle atterrirait dans `started`
// (ignoré) et l'appel partirait sur Inferhub avec la clé Agnes → 401.
  const base = upstreamBase || CFG.upstream;
  // [AGNES-MAXTOKENS 20260916] Agnes plafonne la sortie à 65536 tokens :
  // Zed demande jusqu'à 128000 (max_output_tokens de combo/cheap) → 400
  // systématique sur le fallback/direct Agnes. On écrête, sans toucher au
  // body Inferhub.
  // [MULTI-PROVIDER 20260919] écrêtage sortie générique : `maxOut` du
  // provider, sinon 65536 historique pour agnes-ai.
  const clipOut = Number(prov?.maxOut ?? 0) || ((upstreamBase || '').includes('agnes-ai') ? 65536 : 0);
  let fwdBody = body;
  if (clipOut > 0) {
    const over = (v) => typeof v === 'number' && v > clipOut;
    if (over(body?.max_tokens) || over(body?.max_completion_tokens)) {
      fwdBody = { ...body };
      if (over(body.max_tokens)) fwdBody.max_tokens = clipOut;
      if (over(body.max_completion_tokens)) fwdBody.max_completion_tokens = clipOut;
      log(`[provider] sortie écrêtée à ${clipOut} (demandé ${body.max_tokens ?? body.max_completion_tokens})`);
    }
  }
  // [POOL-CONCURRENCY] acquire slot dans le pool avant de toucher l'upstream
  await poolAcquire();
  // [POOL-LEAK-FIX 20260915] UN SEUL point de sortie pour le slot : avant ce
  // fix, les returns/throw du catch fetch (timeouts !) et les doubles
  // 'close'+'finish' fuyaient ou dupliquaient les slots — après une rafale de
  // timeouts le pool se vidait et TOUTES les requêtes s'entassaient pour
  // toujours (deadlock constaté 22h30 : 40 s sans une ligne de log).
  let slotReleased = false;
  const releaseSlot = () => { if (!slotReleased) { slotReleased = true; poolRelease(); } };
  // client déjà parti pendant l'attente pool ? ne pas consommer d'upstream.
  if (res.destroyed || res.writableEnded) { releaseSlot(); return { ok: false, err: 'client parti avant traitement', forwarded: true }; }
  // [PROVIDER-TIMEOUT 20260919] budget temps PROPRE au provider. Muse avec
  // `reasoning_effort=max` émet 4-5 k tokens de raisonnement AVANT le contenu :
  // mesuré 42-50 s, donc systématiquement coupé par le budget global (25 s) puis
  // déclaré « down » → bascule inutile et Muse inutilisable en max.
  // `timeoutMs`/`ttftTimeoutMs` du provider priment ; sinon défaut global.
  const fetchTmoMs = Number(prov?.timeoutMs ?? 0) || FETCH_TIMEOUT_MS;
  const ttftTmoMs = Number(prov?.ttftTimeoutMs ?? 0) || TTFT_TIMEOUT_MS;
  const ac = new AbortController();
  let tmo = setTimeout(() => ac.abort(), fetchTmoMs);
  // [AGNES-DIAG-20260915] on loggue le "fingerprint" de la clé effective
  // (prefixe 8 + longueur) avant tout appel Agnes, pour comparer avec la
  // clé USER si Agnes renvoie 401. La clé n'est JAMAIS imprimée.
  const effectiveKey = fwdHeaders?.['Authorization'] || fwdHeaders?.authorization || '';
  if (upstreamBase && upstreamBase.includes('agnes-ai')) {
    const k = effectiveKey.replace(/^Bearer\s+/i, '');
    log(`[agnes-diag] clé effective envoyée: ${k ? k.slice(0, 8) + '…(' + k.length + ' chars)' : 'VIDE'}, base=${upstreamBase}`);
  }
  // [TTFT-TIMEOUT] partage avec le watchdog post-headers (aborte le fetch
  // lui-meme : annuler le body est impossible pendant le for-await qui le
  // verrouille — ac.abort() fait sortir la boucle en erreur, rattrapee plus
  // bas et convertie en erreur retryable vers le modele suivant).
  let committed = false;
  let ttftExpired = false;
  const armTtftWatchdog = () => {
    clearTimeout(tmo);
    tmo = setTimeout(() => {
      if (!committed) { ttftExpired = true; ac.abort(); }
    }, ttftTmoMs);
  };
  const disarmWatchdog = () => clearTimeout(tmo);
  let up;
  const tStart = Date.now(); // mesure débit sortie (règle lenteur), hors attente pool
  // [VITESSE 20260919] modèle posé EN PLACE (pas de clone {...body} à chaque
  // tentative — le body fait jusqu'à 1 Mo+). `body` nous appartient (parsé par
  // requête) ; `requested`/`isDirectAgnes` déjà capturés avant les tentatives.
  fwdBody.model = model;
  try {
    up = await fetch(`${base}/chat/completions`, {
      method: 'POST',
      headers: { ...fwdHeaders },
      body: JSON.stringify(fwdBody),
      signal: ac.signal,
    });
    // [AGNES-DIAG-20260915] on loggue l'URL exacte pour s'assurer que le mode
    // Agnes va vers l'API Agnes et non vers Inferhub (cause du 401 "invalid
    // api key" si erreur : la clé Agnes envoyée à Inferhub).
    // [DIAG-LABEL 20260919] le libellé "(clé agnes)" était faux pour tout
    // provider autre qu'Agnes (les appels meta étaient étiquetés "clé agnes",
    // ce qui a égaré un diagnostic) : on nomme la base réellement appelée.
    if (base !== CFG.upstream) log(`[diag] POST ${base}/chat/completions (clé de ce provider)`);
  } catch (err) {
    disarmWatchdog();
    releaseSlot();
    if (ac.signal.aborted) return { ok: false, err: `timeout ${fetchTmoMs}ms sur ${model}` };
    throw err; // erreur réseau réelle (réseau coupé, etc.) — remontée au caller
  }

  if (!body.stream) {
    disarmWatchdog();
    let text;
    try {
      text = await up.text();
    } catch (err) {
      releaseSlot();
      throw err;
    }
    if (bodyIsError(up.status, text, false)) {
      releaseSlot();
      return { ok: false, err: `${up.status} ${text.slice(0, 160)}` };
    }
    // [CLIENT-PARTI 20260919] client déconnecté pendant la génération amont :
    // ne pas écrire sur une réponse morte (même garde qu'à l'entrée de la tentative).
    if (res.destroyed || res.writableEnded) { releaseSlot(); return { ok: false, err: 'client parti avant traitement', forwarded: true }; }
    res.writeHead(up.status, { 'Content-Type': up.headers.get('content-type') || 'application/json' });
    res.end(text);
    releaseSlot();
    // [USAGE-QUOTA 20260919] `usage` réel de l'amont si présent, sinon estimation.
    const uReal = usageOf(text);
    return {
      ok: true,
      slow: tpsSlow(model, tStart, compTokensOf(text)),
      usage: uReal || estUsage(fwdBody, estCompletionOf(text)),
    };
  }
  // mode stream : le watchdog couvre désormais le TTFT (headers recus, on
  // attend le premier chunk SSE valide).
  armTtftWatchdog();

  // [POOL-CONCURRENCY] en mode stream on garde le slot jusqu'à la fin du
  // stream (sinon le pool se vide trop vite et d'autres streams prennent
  // la place avant que celui-ci soit terminé).
  // [POOL-RELEASE-ON-CLOSE] on libère le slot à la fin, dans tous les cas
  // (via releaseSlot : les doubles 'close'+'finish' ne comptent qu'une fois).
  const releaseOnClose = () => { releaseSlot(); };
  res.on('close', releaseOnClose);
  res.on('finish', releaseOnClose);
  const safeRelease = () => { res.removeListener('close', releaseOnClose); res.removeListener('finish', releaseOnClose); releaseSlot(); };

  // mode STREAM : rien n'est envoyé au client tant que le premier chunk
  // n'est pas confirmé propre (sinon impossible de reprendre sur un autre
  // modèle). Les lignes sont gardées en réserve jusqu'à la confirmation.
  if (up.status >= 400) {
    const text = await up.text().catch(() => '');
    disarmWatchdog();
    safeRelease();
    return { ok: false, err: `${up.status} ${text.slice(0, 160)}` };
  }
  let sawDone = false;
  let lineBuf = '';
  let outChars = 0; // estimation sortie stream (règle lenteur : chars/4)
  let usageSeen = null; // [USAGE-QUOTA 20260919] `usage` d'un chunk SSE final
  let held = [];
  const dec = new TextDecoder();
  const commit = () => {
    disarmWatchdog();
    res.writeHead(200, {
      'Content-Type': up.headers.get('content-type') || 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    for (const l of held) res.write(l);
    held = [];
    committed = true;
  };
  try {
    for await (const chunk of up.body) {
      const __dec = dec.decode(chunk, { stream: true });
      outChars += __dec.length;
      lineBuf += __dec;
      let idx;
      while ((idx = lineBuf.indexOf('\n')) >= 0) {
        const line = lineBuf.slice(0, idx + 1);
        lineBuf = lineBuf.slice(idx + 1);
        const sse = sseInspect(line); // 0 rien, 1 erreur, 2 chunk, 3 contenu
        // [USAGE-QUOTA 20260919] un chunk final peut porter `usage` (source réelle).
        if (!usageSeen) { const su = sseUsage(line); if (su) usageSeen = su; }
        if (sse === 1) {
          if (!committed) { disarmWatchdog(); safeRelease(); return { ok: false, err: 'event erreur SSE avant premier chunk propre: ' + line.trim().slice(5, 165) }; }
          res.write(line); res.end();
          return { ok: false, err: 'event erreur SSE en plein stream', forwarded: true };
        }
        if (!committed) {
          held.push(line);
          if (sse === 2 || sse === 3) commit();
          continue;
        }
        if (line.includes('[DONE]')) sawDone = true;
        res.write(line);
      }
    }
    if (lineBuf) {
      const sseTail = sseInspect(lineBuf);
      if (!usageSeen) { const su = sseUsage(lineBuf); if (su) usageSeen = su; }
      if (sseTail === 1) {
          if (!committed) { disarmWatchdog(); safeRelease(); return { ok: false, err: 'erreur SSE finale: ' + lineBuf.trim().slice(5, 165) }; }
          res.write(lineBuf); res.end();
          return { ok: false, err: 'erreur SSE en plein stream', forwarded: true };
        }
      if (!committed && sseTail === 2) commit();
      else if (!committed) held.push(lineBuf);
      if (committed) { if (lineBuf.includes('[DONE]')) sawDone = true; outChars += lineBuf.length; res.write(lineBuf); }
    }
  } catch (err) {
      disarmWatchdog();
      if (!committed) { safeRelease(); return { ok: false, err: ttftExpired ? `TTFT timeout ${ttftTmoMs}ms sans chunk sur ${model}` : `stream mort avant commit: ${err.message}` }; }
      res.end();
      safeRelease();
      return { ok: false, err: `stream coupé: ${err.message}`, forwarded: true };
  }
    if (!committed) { disarmWatchdog(); safeRelease(); if (ttftExpired) return { ok: false, err: `TTFT timeout ${ttftTmoMs}ms sans chunk sur ${model}` }; return { ok: false, err: sawDone ? 'stream vide' : 'stream terminé sans chunk valide (retryable)' }; }
  res.end();
  if (!sawDone) return { ok: false, err: 'stream inachevé (pas de [DONE])', forwarded: true };
  // [USAGE-QUOTA 20260919] `usage` SSE réel si l'amont en a émis un, sinon estimation.
  return {
    ok: true,
    slow: tpsSlow(model, tStart, Math.round(outChars / 4)),
    usage: usageSeen || estUsage(fwdBody, Math.round(outChars / 4)),
  };
}

// [STATS-TOKENS 20260919] enveloppe d'INSTRUMENTATION : TOUS les points de
// retour de la tentative (succès, erreurs HTTP/SSE/stream/timeout/client parti)
// passent par ici — un seul point d'enregistrement, donc aucune sortie oubliée
// (l'alternative, instrumenter chaque `return`, en compte une quinzaine et
// dérive à chaque évolution du failover). Un `throw` (erreur réseau réelle) est
// compté puis RELANCÉ tel quel : les appelants le convertissent déjà en
// `réseau <message>` et le catch ci-dessous enregistre la même chaîne.
// NB : la latence mesurée ici englobe l'attente de slot du pool (elle n'est pas
// accessible depuis l'appelant) ; l'ordre de grandeur reste celui de la tentative.
async function attemptUpstream(model, fwdHeaders, body, res, started, upstreamBase, prov) {
  const stT0 = Date.now();
  let r;
  try {
    r = await attemptUpstreamCore(model, fwdHeaders, body, res, started, upstreamBase, prov);
  } catch (err) {
    stRecord(model, prov, upstreamBase, { ok: false, err: `réseau ${err.message}` }, Date.now() - stT0);
    throw err;
  }
  stRecord(model, prov, upstreamBase, r, Date.now() - stT0);
  return r;
}

// [RETRY-1S-MUSE 20260919] enveloppe « un essai, puis un nouvel essai après
// RETRY_DELAY_MS » pour les phases HORS chaîne principale (seconde chance,
// escalade prix, providers de secours) : avant ce correctif, ces trois phases
// n'avaient aucun rejeu (un seul essai par modèle). Politique identique à la
// chaîne : on ne rejoue JAMAIS une mort déterministe (404/403/401/402), un
// contexte trop gros, ni un flux déjà engagé côté client (`forwarded`).
// ⚠️ ORDRE DES ARGUMENTS : (model, headers, body, res, upstreamBase, prov) —
// soit 6 paramètres, SANS le `started` d'`attemptUpstream`. Passer `undefined`
// en 5e position comme pour `attemptUpstream` décale tout : la base retombe sur
// `CFG.upstream` et `prov` reçoit une chaîne (bug constaté au test du 19/09).
async function attemptWithRetry(model, headers, b, res, upstreamBase, prov) {
  const call = () => attemptUpstream(model, headers, b, res, undefined, upstreamBase, prov)
    .catch((err) => ({ ok: false, err: `réseau ${err.message}` }));
  let r = await call();
  if (r.ok || r.forwarded) return r;
  if (ATTEMPTS_PER_MODEL <= 1) return r;
  if (isModelDead(r.err) || isBodyTooLarge(r.err)) return r;
  log(`↻ ${model} en échec (${String(r.err).slice(0, 90)}) – nouvel essai dans ${RETRY_DELAY_MS} ms`);
  await sleep(RETRY_DELAY_MS);
  return await call();
}

// [AGNES-FALLBACK 20260915] si TOUT INFERHUB echoue (chaîne+passes épuisées)
// et que le contexte est SOUS maxChainTokens, on bascule sur le provider
// Agnes (AGNES_API_KEY, API hub agnes-ai) : combo/cheap redevient un vrai
// filet de secours independant d'Inferhub. Les modeles agnes-* déclarés dans
// `chains` ne vont QUE sur Agnes (zéro rejeu Inferhub) : le "model" demand
// est transmis tel quel + caps Inferhub retirees.
// [AGNES-CAPS-MIRROR] les 2 headers x-max-input-price/output-price (Zed) sont
// forwardés vers Agnes MÊME pour les modeles agnes-* (Agnes les lit et sert
// la cap de 0.01/0.05 USDC). La "retirer" ne marcherait qu'avec une clé
// admin.
const AGNES_API = 'https://apihub.agnes-ai.com/v1';
// [MULTI-PROVIDER 20260919] N providers de secours ordonnés (config
// `providers`). Sans config : repli historique = Agnes seule. Entrée :
// {name, base, keyEnv, key?, models?, coolMs?, maxOut?}. `models` = modèles
// servis par ce provider (défaut : le modèle demandé tel quel).
const PROVIDERS = (() => {
  const list = Array.isArray(CFG.providers) ? CFG.providers : null;
  const raw = list && list.length ? list : [
    { name: 'agnes', base: AGNES_API, keyEnv: 'AGNES_API_KEY', models: ['agnes-3.0-flash'] },
  ];
  return raw
    .filter((p) => p && typeof p.base === 'string' && p.base)
    .map((p) => ({
      name: String(p.name || p.base),
      base: p.base.replace(/\/$/, ''),
      keyEnv: p.keyEnv || '',
      staticKey: typeof p.key === 'string' ? p.key : '',
      models: Array.isArray(p.models) ? p.models.filter((m) => typeof m === 'string') : [],
      coolMs: Number(p.coolMs ?? 45000),
      maxOut: Number(p.maxOut ?? 0) || 0,
      // [PROVIDER-TIMEOUT 20260919] budget temps propre à ce provider (0 = global).
      // Indispensable pour un modèle à raisonnement long (Muse en `max` : 42-50 s
      // de génération, très au-delà des 25 s globaux).
      timeoutMs: Number(p.timeoutMs ?? 0) || 0,
      ttftTimeoutMs: Number(p.ttftTimeoutMs ?? 0) || 0,
      noAuth: !!p.noAuth, // ex. Ollama local sans clé
      // [BODY-FIX-PROVIDER 20260919] nettoyage du corps propre à ce provider.
      bodyFix: p.bodyFix && typeof p.bodyFix === 'object' ? p.bodyFix : null,
      // [PROVIDER-HEADERS 20260919] en-têtes propres à ce provider, ajoutés
      // par-dessus les en-têtes communs. Meta (Muse) n'honore `reasoning_effort`
      // sur `muse-spark-1.3-contributor` QUE pour un User-Agent `muse-build/x.y.z`
      // (sinon 400 générique) : le routeur doit donc s'annoncer comme le client.
      headers: p.headers && typeof p.headers === 'object' ? p.headers : null,
      // [PROVIDER-FORCE-BODY 20260919] champs IMPOSÉS à ce provider, écrasant
      // ce que le client a envoyé (ex. effort de raisonnement `max`).
      forceBody: p.forceBody && typeof p.forceBody === 'object' ? p.forceBody : null,
      _key: null, // cache clé (rechargée sur 401)
    }));
})();
// [RETRY-1S-MUSE 20260919] cible de la bascule « erreur persistante » : le
// provider qui sert Muse. Déclaré explicitement (`museProvider: "meta"`) sinon
// détecté par nom de provider (`meta`) puis par modèle (`muse-*`). Null si
// aucun provider ne sert Muse → on retombe sur la liste complète des secours.
const MUSE_PROVIDER = (() => {
  if (CFG.museProvider) {
    const named = PROVIDERS.find((p) => p.name === String(CFG.museProvider));
    if (named) return named;
  }
  return PROVIDERS.find((p) => /^meta$/i.test(p.name))
    || PROVIDERS.find((p) => p.models.some((m) => /^muse-/i.test(m)))
    || null;
})();
// [BODY-FIX-PROVIDER 20260919] toutes les API ne parlent pas le même [OI] :
// Meta (Muse) refuse par 400 des paramètres acceptés par Inferhub/Agnes
// (`stop`, `logprobs`, `reasoning`, `stream_options` sans `stream`,
// `max_tokens` + `max_completion_tokens` ensemble). Sans nettoyage, la bascule
// vers ce provider échouerait sur des paramètres que le client envoie
// légitimement. `bodyFix` (config du provider) décrit ce qu'il faut retirer ;
// le body des autres providers n'est jamais modifié.
function applyBodyFix(body, fix) {
  if (!fix || !body || typeof body !== 'object') return body;
  const dropped = (Array.isArray(fix.drop) ? fix.drop : []).filter((k) => body[k] !== undefined);
  const bothMax = !!fix.exclusiveMaxTokens && body.max_tokens !== undefined && body.max_completion_tokens !== undefined;
  const orphanStreamOpts = !!fix.dropStreamOptionsWhenNotStreaming && body.stream_options !== undefined && !body.stream;
  // [TOOLCHOICE-AUTO 20260919] Meta n'accepte QUE `tool_choice:"auto"` (ni
  // `none`/`required`, ni un outil nommé). Un client qui force un outil
  // recevrait un 400 au lieu d'une réponse : on retombe sur `auto`.
  const forcedChoice = !!fix.forceToolChoiceAuto && body.tool_choice !== undefined && body.tool_choice !== 'auto';
  if (!dropped.length && !bothMax && !orphanStreamOpts && !forcedChoice) return body;
  const out = { ...body };
  for (const k of dropped) delete out[k];
  if (bothMax) delete out.max_completion_tokens;
  if (orphanStreamOpts) delete out.stream_options;
  if (forcedChoice) out.tool_choice = 'auto';
  const removed = [...dropped, ...(bothMax ? ['max_completion_tokens'] : []), ...(orphanStreamOpts ? ['stream_options'] : [])];
  if (forcedChoice) removed.push('tool_choice→auto');
  log(`[provider] corps nettoyé pour ce provider (retiré : ${removed.join(', ')})`);
  return out;
}
// [PROVIDER-FORCE-BODY 20260919] champs IMPOSÉS au provider (ils écrasent la
// valeur du client). Sert à garantir un réglage qui doit valoir pour CE
// provider quel que soit le client : ex. `reasoning_effort:"max"` sur Muse.
function applyForceBody(body, force) {
  if (!force || !body || typeof body !== 'object') return body;
  const keys = Object.keys(force).filter((k) => body[k] !== force[k]);
  if (!keys.length) return body;
  const out = { ...body };
  for (const k of keys) out[k] = force[k];
  log(`[provider] champs imposés pour ce provider (${keys.map((k) => `${k}=${JSON.stringify(force[k])}`).join(', ')})`);
  return out;
}
// [TOKEN-COUNT-LOCAL 20260919] estimation locale (~4 caractères/token) : sert
// UNIQUEMENT au comptage pre-vol des clients, jamais au routage ni aux prix.
function estimateTokens(body) {
  try { return Math.max(1, Math.ceil(JSON.stringify(body).length / 4)); } catch { return 1; }
}
function provKey(pv, force) {
  if (pv.staticKey) return pv.staticKey;
  if (!pv.keyEnv) return '';
  if (pv.keyEnv === 'AGNES_API_KEY') return findAgnesKey(force, force ? '401-retry' : '');
  if (!pv._key || force) {
    const k = loadEnvKey(pv.keyEnv);
    if (k) { pv._key = k; if (force) log(`[provider] clé ${pv.name} re-chargée (${k.slice(0, 8)}…)`); }
  }
  return pv._key || '';
}
// [MULTI-PROVIDER] panne PROVIDER (vs panne modèle) : réseau/timeout/5xx.
// Sur panne provider on abandonne SES autres modèles et on passe au provider
// suivant — sans brûler N×timeouts. Seules ces erreurs arment le disjoncteur.
function isProviderOutage(err) {
  return /^(réseau|timeout|TTFT)|fetch failed|ECONNREFUSED|ENOTFOUND|EAI_AGAIN|ETIMEDOUT|socket hang up|stream (mort|coupé|inachevé)|terminé sans chunk valide|\b5\d\d\b|bad gateway|service unavailable|gateway timeout|overloaded/i.test(String(err || ''));
}
const provState = new Map(); // name -> {fails, first, skipUntil}
function provCoolMs(name) {
  const pv = PROVIDERS.find((p) => p.name === name);
  return Math.max(5000, Number(pv?.coolMs ?? 45000));
}
function provSkip(name) {
  // [USAGE-QUOTA 20260919] un provider dont la CLÉ est épuisée (quota) est
  // traité comme en cooldown : le failover ne rebrûle pas d'appels voués à
  // l'échec. La reprise se fait par expiration de `quotaUntil` ou par
  // POST /v1/usage/reset (cf. section USAGE-QUOTA plus bas).
  if (provQuotaExhausted(name)) return true;
  const s = provState.get(name);
  return !!(s && Date.now() < s.skipUntil);
}
// [LAST-CHANCE 20260919] cadence mini entre deux "dernières chances"
// (cf. boucle de secours) : borne le coût si la panne est réellement globale.
const LAST_CHANCE_GAP_MS = 10000;
let lastChanceAt = 0;
function provNote(name, ok, err) {
  const now = Date.now();
  if (ok) { if (provState.delete(name)) { log(`[provider] ${name} rétabli`); cbSave(); } return; }
  if (!isProviderOutage(err)) return; // erreur modèle → pas une panne provider
  let s = provState.get(name);
  if (!s || now - s.first > 60000) s = { fails: 0, first: now, skipUntil: 0 };
  s.fails++;
  if (s.fails >= 2) {
    const cool = provCoolMs(name);
    s.skipUntil = Math.max(s.skipUntil || 0, now + cool); s.fails = 0; s.first = now;
    log(`[provider] ${name} DOWN (panne/API) — cooldown ${(cool / 1000) | 0}s, bascule sur le suivant`);
  }
  provState.set(name, s);
  cbSave();
}
// [AGNES-GUARD 20260916] disjoncteur dédié Agnes + mode "TOUJOURS AGNES" :
// si Agnes est lui-même en panne (timeout/5xx), on le saute (45 s de
// cooldown) et on passe le service en AGNES-ONLY (le plus rapide) pendant la
// panne d'Inferhub. L'inverse ne se produit pas : si Inferhub se rétablit,
// on repasse au mode normal.
const AGNES_GUARD = (() => {
  const g = CFG.agnesGuard || {};
  return {
    enabled: g.enabled !== false,
    windowMs: g.windowMs ?? 60000,
    minTps: g.minTps ?? 20,
  };
})();
let agnesKey = loadAgnesKey();
function findAgnesKey(force, reason) {
  if (!agnesKey || force) {
    const k = loadAgnesKey();
    if (k && k !== agnesKey) {
      agnesKey = k;
      log(`[agnes] clé re-chargee ${reason || ''} (${k.slice(0, 8)}…, ${k.length} chars)`);
    }
  }
  return agnesKey;
}
const AGNES_KEY = (() => { findAgnesKey(true, 'boot'); return agnesKey; })();

// [AGNES-GUARD] état du disjoncteur Agnes (sauté si >= 3 échecs dans windowMs)
const agnesState = { fails: 0, first: 0, skipUntil: 0 };
function agnesGuardRecord(ok, tps) {
  if (!AGNES_GUARD.enabled) return;
  const now = Date.now();
  if (ok) {
    if (tps != null && tps > 0 && tps < AGNES_GUARD.minTps) {
      log(`[agnes-guard] Agnes lent (${tps} tok/s < ${AGNES_GUARD.minTps})`);
    }
    agnesState.fails = 0;
    agnesState.first = 0;
    agnesState.skipUntil = 0;
  } else {
    if (agnesState.fails === 0) agnesState.first = now;
    agnesState.fails += 1;
    if (agnesState.fails >= 3 && now - agnesState.first <= AGNES_GUARD.windowMs) {
      agnesState.skipUntil = now + 45000;
      log(`[agnes-guard] Agnes en cooldown 45 s (${agnesState.fails} échecs)`);
    }
  }
}
function agnesGuardBlocked() {
  return AGNES_GUARD.enabled && Date.now() < agnesState.skipUntil;
}
// [TOUJOURS-AGNES] mode dérivé : si Inferhub est en panne (disjoncteur global)
// OU si Agnes est le seul qui marche, on sert Agnes en premier.
// On active ce mode quand : (a) le circuit global d'Inferhub a 5+ modèles en
// cooldown, ou (b) 2 échecs consécutifs de chaîne entière.
let consecutiveChainFails = 0;
function shouldForceAgnes() {
  if (consecutiveChainFails >= 2) return true;
  if (agnesGuardBlocked()) return false;
  let inCooldown = 0;
  for (const [, st] of circuit) { if (st.skipUntil > Date.now()) inCooldown++; }
  return inCooldown >= 5;
}
function recordChainOutcome(servedOk) {
  if (servedOk) { consecutiveChainFails = 0; return; }
  consecutiveChainFails += 1;
  if (consecutiveChainFails === 2) {
    log(`[toujours-agnes] 2 échecs de chaîne consécutifs → mode AGNES-ONLY activé`);
  }
}
function isAgnesModel(model) { return /^agnes-/i.test(String(model)); }
function isContextTooLargeForAll(err) {
  return isBodyTooLarge(err) || /exceeds the maximum number of tokens allowed\s*\(?(1[0-9]{5,})\)/i.test(String(err || ''));
}

// [PRIORITE-VITESSE 20260916] (JP : à prix égal, prioriser la vitesse si
// ≥3× d'écart de TPS). EMA des débits mesurés par modèle (succès ≥100
// tokens) ; cmpVitesse tranche les égalités de prix dans les tris.
// [FIX-TDZ 20260916] déclaré AVANT le tri boot (utilisé par sortPrix ligne
// ~417 : en const/let, toute utilisation avant init = TDZ → tri tombé).
const TPS_RATIO = Number((CFG.speedPriority && CFG.speedPriority.tpsRatio) ?? 3);
const TPS_EMA_A = 0.3;
const tpsEma = new Map(); // model -> tok/s moyens
function noteTps(model, tps) {
  if (!(tps > 0)) return;
  const p = tpsEma.get(model);
  tpsEma.set(model, p == null ? tps : p + TPS_EMA_A * (tps - p));
}
function cmpVitesse(a, b) {
  const ta = tpsEma.get(a), tb = tpsEma.get(b);
  if (ta == null || tb == null || ta <= 0 || tb <= 0) return 0;
  if (ta >= tb * TPS_RATIO) return -1;
  if (tb >= ta * TPS_RATIO) return 1;
  return 0;
}

// [PRIX-TRI-BOOT 20260916] règle JP : le MOINS CHER toujours en premier.
// Tri live au démarrage (une fois, ~1-2 s) sur min_ask_in ; à prix égal,
// 3.8-high passe avant 3.7-high (tie-break explicite JP). Sans prix (alias)
// = dernier. En cas d'échec on garde l'ordre configuré. Mouvements intra-day
// repris au prochain restart.
try {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 12000);
  const mr = await fetch(`${CFG.upstream}/models`, { headers: { Authorization: `Bearer ${REAL_KEY}` }, signal: ctl.signal });
  clearTimeout(t);
  const md = await mr.json();
  const px = {};
  for (const m of (md.data || [])) {
    px[m.id] = m?.pricing?.min_ask_in ?? null;
  }
  const pri = (id) => {
    const p = px[id];
    return (typeof p === 'number' ? p : Infinity);
  };
  // Tie-break JP : à prix égal, 3.8-high avant 3.7-high.
  const tie = (a, b) => {
    if (a === 'ag/gemini-3.8-flash-high' && b === 'ag/gemini-3.7-flash-high') return -1;
    if (a === 'ag/gemini-3.7-flash-high' && b === 'ag/gemini-3.8-flash-high') return 1;
    return 0;
  };
  const sortPrix = (arr) => arr
    .map((m, i) => ({ m, i }))
    .sort((x, y) => (pri(x.m) - pri(y.m)) || cmpVitesse(x.m, y.m) || tie(x.m, y.m) || (x.i - y.i))
    .map((e) => e.m);
  for (const key of ['combo/cheap']) {
    if (Array.isArray(CFG.chains?.[key])) {
      CFG.chains[key] = sortPrix(CFG.chains[key]);
      log(`[prix] ordre ${key} : ${CFG.chains[key].map((m) => `${m}(${px[m] ?? 'alias'})`).join(' > ')}`);
    }
  }
  if (Array.isArray(CFG.defaultBackups)) {
    CFG.defaultBackups = sortPrix(CFG.defaultBackups);
  }
  if (Array.isArray(CFG.secondChance)) {
    CFG.secondChance = sortPrix(CFG.secondChance);
  }
  // [ESCALADE 20260916] ronde "plus cher" (JP, plafond 0.005/0.03) : mêmes
  // familles (gemini/qwen/deepseek/glm, hors image), prix ≤ plafond, ctx 1M
  // mini, triés croissant, hors modèles déjà essayés (chaîne/backups/
  // secondeChance/boucle). Construite au boot, rejouée après les passes.
  {
    const E = CFG.escalation || {};
    const capIn = Number(E.maxIn ?? 0.005), capOut = Number(E.maxOut ?? 0.03);
    const maxN = Math.max(1, Number(E.maxModels ?? 8));
    const seen = new Set([...(CFG.chains?.['combo/cheap'] || []), ...(CFG.defaultBackups || []), ...(CFG.secondChance || []), 'combo/cheap']);
    const fam = /gemini|qwen|deepseek|glm/i;
    CFG.escalationModels = (md.data || [])
      .filter((m) => typeof m.id === 'string' && fam.test(m.id) && !/image/i.test(m.id) && !seen.has(m.id))
      .filter((m) => typeof m?.pricing?.min_ask_in === 'number' && typeof m?.pricing?.min_ask_out === 'number')
      .filter((m) => m.pricing.min_ask_in <= capIn && m.pricing.min_ask_out <= capOut)
      .filter((m) => (m.input_token_limit ?? 0) >= 1000000)
      .sort((a, b) => a.pricing.min_ask_in - b.pricing.min_ask_in)
      .slice(0, maxN)
      .map((m) => m.id);
    log(`[prix] escalade (≤${capIn}/${capOut}, ${CFG.escalationModels.length}) : ${CFG.escalationModels.join(' > ') || '(aucun candidat)'}`);
  }
} catch (err) { log(`[prix] tri indisponible (${err.message}) → ordre configuré gardé`); }

// [DISJONCTEUR 20260916] un modèle qui échoue en rafale est sauté pendant
// un cooldown (évite de re-brûler 20-25 s de timeout à chaque requête
// pendant les orages). Succès = reset. Seuils en CFG.circuit. Ne couvre
// PAS Agnes (filet de secours : on le tente toujours).
const CB_CONF = CFG.circuit || {};
const CB_FAILS = Math.max(1, Number(CB_CONF.fails ?? 3));
const CB_WIN_MS = Number(CB_CONF.windowMs ?? 180000);
const CB_COOL_MS = Number(CB_CONF.coolMs ?? 120000);
const cbState = new Map(); // model -> {fails, first, skipUntil}
// [DISJONCTEUR-PERSIST 20260916] l'état survit aux restarts (sinon chaque
// restart effaçait les cooldowns et les 3 échecs repartaient de zéro).
const CB_STATE_FILE = path.join(__dirname, 'inferhub-failover-circuit.json');
// [PERF-100RPS 20260919] persistance throttlée : un fsync à chaque erreur
// bloquerait l'event loop sous rafale. Erreur isolée = persistée aussitôt
// (comportement historique) ; rafale = un seul flush différé (2 s).
let cbSaveTimer = null;
let cbSaveQueued = false;
function cbSaveNow() {
  cbSaveQueued = false;
  try {
    fs.writeFileSync(CB_STATE_FILE, JSON.stringify({ models: Object.fromEntries(cbState), providers: Object.fromEntries(provState) }), 'utf8');
  } catch {}
}
function cbSave() {
  if (!cbSaveTimer) {
    cbSaveNow();
    cbSaveTimer = setTimeout(() => { cbSaveTimer = null; if (cbSaveQueued) cbSaveNow(); }, 2000);
    if (cbSaveTimer.unref) cbSaveTimer.unref();
    return;
  }
  cbSaveQueued = true;
}
try {
  const raw = fs.readFileSync(CB_STATE_FILE, 'utf8');
  const obj = JSON.parse(raw);
  const mods = (obj && obj.models) || obj || {};
  let n = 0;
  for (const [k, v] of Object.entries(mods)) {
    if (v && typeof v.skipUntil === 'number') { cbState.set(k, v); n++; }
  }
  const provs = (obj && obj.providers) || {};
  let m = 0;
  for (const [k, v] of Object.entries(provs)) {
    if (v && typeof v.skipUntil === 'number' && v.skipUntil > Date.now()) { provState.set(k, v); m++; }
  }
  if (n || m) log(`[disjoncteur] état restauré (${n} modèle(s), ${m} provider(s))`);
} catch {}
// ===================== [USAGE-QUOTA 20260919] =====================
// Comptage LOCAL des tokens par provider/modèle + détection d'ÉPUISEMENT de clé.
// Constats établis : l'API Muse n'expose AUCUN endpoint d'usage/quota (/usage,
// /quota, /me, /user, /credits, /balance, /dashboard/billing/*, /rate_limits,
// /limits → 404) et les réponses ne portent aucun en-tête de quota (seulement
// x-request-id / x-route) ; le CLI `muse` n'a pas de commande d'usage. Le
// comptage est donc local (bloc `usage` des réponses quand il existe, sinon
// estimation chars/4 marquée `estimated`) et l'épuisement est détecté par
// SIGNATURE D'ERREUR (402, 429 + texte quota/crédit, texte quota seul).
// Persistance : même modèle que cbSave (flush immédiat + un flush différé max
// toutes les 2 s) — le fichier survit au restart, comme le disjoncteur.
const USAGE_STATE_FILE = path.join(__dirname, 'inferhub-failover-usage.json');
const usage = { since: Date.now(), updated: Date.now(), providers: {}, models: {} };
const quotaState = new Map(); // provider -> {exhausted, since, until, msg}
let usageSaveTimer = null;
let usageSaveQueued = false;
function usageSaveNow() {
  usageSaveQueued = false;
  usage.updated = Date.now();
  try {
    fs.writeFileSync(USAGE_STATE_FILE, JSON.stringify({
      since: usage.since,
      updated: usage.updated,
      providers: usage.providers,
      models: usage.models,
      // état quota persisté dans le MÊME fichier (survit au restart : sinon le
      // routeur rebrûlerait un appel voué à l'échec après chaque redémarrage).
      quota: Object.fromEntries(quotaState),
    }), 'utf8');
  } catch {}
}
function usageSave() {
  if (!usageSaveTimer) {
    usageSaveNow();
    usageSaveTimer = setTimeout(() => { usageSaveTimer = null; if (usageSaveQueued) usageSaveNow(); }, 2000);
    if (usageSaveTimer.unref) usageSaveTimer.unref();
    return;
  }
  usageSaveQueued = true;
}
try {
  const obj = JSON.parse(fs.readFileSync(USAGE_STATE_FILE, 'utf8'));
  if (obj && typeof obj === 'object') {
    if (Number.isFinite(obj.since)) usage.since = obj.since;
    if (obj.providers && typeof obj.providers === 'object') usage.providers = obj.providers;
    if (obj.models && typeof obj.models === 'object') usage.models = obj.models;
    let nq = 0;
    for (const [k, v] of Object.entries(obj.quota || {})) {
      if (!v || !v.exhausted) continue;
      if (Number(v.until) > Date.now()) { quotaState.set(k, v); nq++; }
    }
    log(`[usage] compteurs restaurés (${Object.keys(usage.providers).length} provider(s), ${Object.keys(usage.models).length} modèle(s)${nq ? `, ${nq} quota(s) actif(s)` : ''})`);
  }
} catch {}

// [USAGE-QUOTA] attribution d'un relevé à un provider (et au modèle servi).
// `u` = {prompt_tokens, completion_tokens, total_tokens, estimated} ou null
// (échec : on n'invente AUCUN token, on ne compte que requests/failed).
function usageNote(name, model, u, err) {
  if (!name) return;
  const now = Date.now();
  let p = usage.providers[name];
  if (!p) {
    p = usage.providers[name] = {
      requests: 0, ok: 0, failed: 0, prompt_tokens: 0, completion_tokens: 0,
      total_tokens: 0, estimated: 0, lastUsed: 0, lastError: null,
    };
  }
  p.requests += 1;
  p.lastUsed = now;
  let m = null;
  if (model) {
    m = usage.models[model];
    if (!m) m = usage.models[model] = { provider: name, requests: 0, prompt_tokens: 0, completion_tokens: 0, total_tokens: 0, estimated: 0 };
    m.provider = name;
    m.requests += 1;
  }
  if (!u) {
    p.failed += 1;
    if (err) p.lastError = String(err).slice(0, 200);
    usageSave();
    return;
  }
  const pt = Number(u.prompt_tokens) || 0;
  const ct = Number(u.completion_tokens) || 0;
  const tt = Number(u.total_tokens) || (pt + ct);
  const est = u.estimated ? 1 : 0;
  p.ok += 1;
  p.prompt_tokens += pt; p.completion_tokens += ct; p.total_tokens += tt; p.estimated += est;
  if (m) { m.prompt_tokens += pt; m.completion_tokens += ct; m.total_tokens += tt; m.estimated += est; }
  usageSave();
}
// [USAGE-QUOTA] motifs de quota — surchargeables par CFG.quotaPatterns.
const QUOTA_PATTERNS = (Array.isArray(CFG.quotaPatterns) && CFG.quotaPatterns.length
  ? CFG.quotaPatterns.map((s) => String(s))
  : [
    'insufficient_quota', 'exceeded your current quota', 'quota exceeded', 'quota',
    'out of tokens', 'no tokens left', 'not enough tokens', 'token limit',
    'credit balance', 'insufficient credits', 'balance too low', 'billing',
    'payment required', 'usage limit', 'limit reached',
  ]).map((s) => s.toLowerCase());
function retryAfterMsOf(s) {
  const m = /retry[-_ ]?after["'\s:=]*(\d+)/i.exec(String(s || ''));
  if (!m) return 0;
  const n = Number(m[1]);
  if (!(n > 0)) return 0;
  return n >= 1000 ? n : n * 1000; // >=1000 → déjà en ms, sinon en secondes
}
// [USAGE-QUOTA] signature d'épuisement de clé. NE classe JAMAIS une panne
// réseau/5xx en quota (un 5xx n'est pas un problème de solde).
function quotaHit(err) {
  const s = String(err || '');
  if (!s) return null;
  const m = /^\s*(\d{3})\b/.exec(s);
  const status = m ? Number(m[1]) : null;
  if (status !== null && status >= 500) return null;
  // [USAGE-QUOTA] 402 `no_provider_under_bid` = enchère sous le marché (déjà
  // géré par cbNote : skip modèle 45 s), PAS une clé sans tokens. Sans cette
  // exception, le premier 402 de prix verrouillerait TOUT Inferhub 15 min.
  if (status === 402 && /no_provider_under_bid|no provider within|within your max-per-mtok bid/i.test(s)) return null;
  const low = s.toLowerCase();
  const hit = QUOTA_PATTERNS.some((p) => low.includes(p));
  if (status === 402) return { quota: true, message: s.slice(0, 200), retryAfterMs: retryAfterMsOf(s) };
  if (status === 429 && hit) return { quota: true, message: s.slice(0, 200), retryAfterMs: retryAfterMsOf(s) };
  if (hit) return { quota: true, message: s.slice(0, 200), retryAfterMs: retryAfterMsOf(s) };
  return null;
}
function quotaCoolMs() { return Math.max(1000, Number(CFG.quotaCoolMs ?? 900000)); }
// [USAGE-QUOTA] arme l'état quota d'un provider. Log UNE SEULE FOIS par
// transition (l'état persiste tant que `quotaUntil` n'a pas expiré).
function provQuota(name, message) {
  const now = Date.now();
  const q = quotaHit(message) || { retryAfterMs: 0 };
  const prev = quotaState.get(name);
  const st = {
    exhausted: true,
    since: (prev && prev.exhausted && prev.since) || now,
    until: now + (q.retryAfterMs || quotaCoolMs()),
    msg: String(message || '').slice(0, 200),
  };
  quotaState.set(name, st);
  if (!prev || !prev.exhausted) {
    log(`[quota] provider ${name} ÉPUISÉ — clé sans tokens : ${st.msg}`);
  }
  usageSave();
  return st;
}
function quotaClear(name, reason) {
  const prev = quotaState.get(name);
  if (!prev) return false;
  quotaState.delete(name);
  log(`[quota] provider ${name} de nouveau disponible${reason ? ` (${reason})` : ''}`);
  usageSave();
  return true;
}
// vrai tant que le provider est en quota ; l'expiration lève le drapeau (log de reprise)
function provQuotaExhausted(name) {
  const q = quotaState.get(name);
  if (!q) return false;
  if (Number(q.until) > 0 && Date.now() >= q.until) { quotaClear(name, 'cooldown quota expiré'); return false; }
  return !!q.exhausted;
}
// [USAGE-QUOTA] point d'entrée unique côté appelants : détecte + arme + dit si
// le provider vient d'être marqué (l'appelant peut alors abandonner sa file).
function quotaWatch(name, err) {
  const q = quotaHit(err);
  if (!q) return false;
  provQuota(name, err);
  return true;
}
function num(v) { const n = Number(v); return Number.isFinite(n) ? Math.round(n) : 0; }
function tokensOf(e) {
  e = e || {};
  return { prompt: num(e.prompt_tokens), completion: num(e.completion_tokens), total: num(e.total_tokens), estimated: num(e.estimated) };
}
// [USAGE-QUOTA] limites déclarées (CFG.limits, optionnel) — null si absentes,
// on n'invente JAMAIS une limite (aucune n'est exposée par l'amont).
function limitsFor(key) {
  const l = (CFG.limits && typeof CFG.limits === 'object') ? CFG.limits[key] : null;
  if (!l || typeof l !== 'object') return { context: null, output: null };
  const c = Number(l.context), o = Number(l.output);
  return { context: Number.isFinite(c) ? c : null, output: Number.isFinite(o) ? o : null };
}
function providerNames() { return [...new Set(['inferhub', ...PROVIDERS.map((p) => p.name)])]; }
// charge utile de GET /v1/usage (+ alias) et source du dashboard
function usageObj() {
  const now = Date.now();
  const providers = [];
  const quotaAlerts = [];
  const totals = { requests: 0, tokens: { prompt: 0, completion: 0, total: 0 } };
  for (const name of providerNames()) {
    const e = usage.providers[name] || {};
    const q = provQuotaExhausted(name) ? quotaState.get(name) : null;
    const st = provState.get(name);
    const cd = (st && st.skipUntil > now) ? Math.ceil((st.skipUntil - now) / 1000) : 0;
    providers.push({
      name,
      up: !provSkip(name),
      cooldown_s: cd,
      quota_exhausted: !!q,
      quota_msg: q ? (q.msg || null) : null,
      quota_since: (q && q.since) ? new Date(q.since).toISOString() : null,
      quota_until: (q && q.until) ? new Date(q.until).toISOString() : null,
      requests: num(e.requests), ok: num(e.ok), failed: num(e.failed),
      tokens: tokensOf(e),
      limits: limitsFor(name),
      last_error: e.lastError ?? null,
    });
    if (q) quotaAlerts.push({ provider: name, since: new Date(q.since || now).toISOString(), message: q.msg || '' });
    totals.requests += num(e.requests);
    totals.tokens.prompt += num(e.prompt_tokens);
    totals.tokens.completion += num(e.completion_tokens);
    totals.tokens.total += num(e.total_tokens);
  }
  const models = Object.entries(usage.models)
    .map(([name, e]) => ({ name, provider: e.provider || '', requests: num(e.requests), tokens: tokensOf(e), limits: limitsFor(name) }))
    .sort((a, b) => (b.tokens.total - a.tokens.total));
  return {
    ok: true,
    name: 'inferhub-failover',
    version: GW_VERSION,
    uptime_s: Math.floor((now - GW_BOOT_TS) / 1000),
    since: new Date(usage.since).toISOString(),
    updated: new Date(usage.updated).toISOString(),
    providers,
    models,
    totals,
    quota_alerts: quotaAlerts,
  };
}
// [USAGE-QUOTA] dashboard HTML AUTONOME (aucun asset externe, aucun CDN) :
// rendu SERVEUR (lisible sans JS) + rafraîchissement meta 5 s.
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function nfmt(v) { return num(v).toLocaleString('fr-FR'); }
function limTxt(l) { return (l == null) ? '—' : nfmt(l); }
function dashboardHtml() {
  const u = usageObj();
  const provRows = u.providers.map((p) => {
    const etat = p.quota_exhausted
      ? '<span class="bad">ÉPUISÉE</span>'
      : (p.cooldown_s > 0 ? `<span class="warn">cooldown ${p.cooldown_s}s</span>` : '<span class="good">OK</span>');
    return `<tr><td><b>${esc(p.name)}</b></td><td>${etat}</td>`
      + `<td class="n">${nfmt(p.tokens.prompt)}</td><td class="n">${nfmt(p.tokens.completion)}</td><td class="n">${nfmt(p.tokens.total)}</td>`
      + `<td class="n">${nfmt(p.tokens.estimated)}</td>`
      + `<td class="n">${nfmt(p.ok)} / ${nfmt(p.failed)}</td>`
      + `<td>${limTxt(p.limits.context)} / ${limTxt(p.limits.output)}</td>`
      + `<td class="err">${esc(p.last_error || '—')}</td></tr>`;
  }).join('');
  const modelRows = u.models.map((m) => `<tr><td>${esc(m.name)}</td><td>${esc(m.provider)}</td>`
    + `<td class="n">${nfmt(m.requests)}</td><td class="n">${nfmt(m.tokens.prompt)}</td>`
    + `<td class="n">${nfmt(m.tokens.completion)}</td><td class="n">${nfmt(m.tokens.total)}</td>`
    + `<td>${limTxt(m.limits.context)} / ${limTxt(m.limits.output)}</td></tr>`).join('');
  const alerts = u.quota_alerts.length
    ? `<div class="alert">⚠️ Clé épuisée : ${u.quota_alerts.map((a) => `<b>${esc(a.provider)}</b> depuis ${esc(a.since)} — ${esc(a.message)}`).join(' | ')}</div>`
    : '';
  return `<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="5">
<title>inferhub-failover — usage</title>
<style>
:root{--fg:#111;--bg:#fff;--mut:#666;--bd:#ddd;--good:#0a7d32;--warn:#a86400;--bad:#c0392b;--row:#f7f7f7}
@media (prefers-color-scheme:dark){:root{--fg:#e8e8e8;--bg:#16181c;--mut:#9aa0a6;--bd:#333;--good:#5fd08a;--warn:#e0a83c;--bad:#ff7b6b;--row:#1d2025}}
body{margin:0;padding:1.2rem;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,Segoe UI,Roboto,sans-serif}
h1{font-size:1.25rem;margin:0 0 .3rem}h2{font-size:1rem;margin:1.4rem 0 .4rem}
.mut{color:var(--mut)}table{border-collapse:collapse;width:100%;margin-top:.4rem}
th,td{border:1px solid var(--bd);padding:.35rem .5rem;text-align:left;vertical-align:top}
th{background:var(--row)}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
.err{max-width:32ch;overflow-wrap:anywhere;color:var(--mut)}
.good{color:var(--good);font-weight:600}.warn{color:var(--warn);font-weight:600}.bad{color:var(--bad);font-weight:700}
.alert{border:1px solid var(--bad);border-radius:6px;padding:.5rem .7rem;margin:.8rem 0;color:var(--bad)}
.kpi{display:flex;gap:1.4rem;flex-wrap:wrap;margin:.6rem 0}.kpi div{background:var(--row);border:1px solid var(--bd);border-radius:6px;padding:.5rem .8rem}
</style></head><body>
<h1>inferhub-failover — usage &amp; quota <span class="mut">v${esc(u.version)}</span></h1>
<div class="mut">uptime ${nfmt(u.uptime_s)} s · comptage depuis ${esc(u.since)} · mis à jour ${esc(u.updated)} · rafraîchi toutes les 5 s</div>
${alerts}
<div class="kpi"><div><b>${nfmt(u.totals.requests)}</b><br><span class="mut">requêtes amont</span></div>
<div><b>${nfmt(u.totals.tokens.prompt)}</b><br><span class="mut">tokens prompt</span></div>
<div><b>${nfmt(u.totals.tokens.completion)}</b><br><span class="mut">tokens complétion</span></div>
<div><b>${nfmt(u.totals.tokens.total)}</b><br><span class="mut">tokens total</span></div></div>
<h2>Providers</h2>
<table><thead><tr><th>Provider</th><th>État</th><th class="n">Prompt</th><th class="n">Complétion</th><th class="n">Total</th><th class="n">Estimés</th><th class="n">ok / échecs</th><th>Limites ctx/sortie</th><th>Dernière erreur</th></tr></thead>
<tbody>${provRows || '<tr><td colspan="9" class="mut">aucun relevé</td></tr>'}</tbody></table>
<h2>Modèles</h2>
<table><thead><tr><th>Modèle</th><th>Provider</th><th class="n">Requêtes</th><th class="n">Prompt</th><th class="n">Complétion</th><th class="n">Total</th><th>Limites ctx/sortie</th></tr></thead>
<tbody>${modelRows || '<tr><td colspan="7" class="mut">aucun relevé</td></tr>'}</tbody></table>
<p class="mut">Comptage LOCAL (aucun endpoint d'usage côté API amont) : les tokens viennent du bloc <code>usage</code> des réponses quand il existe, sinon d'une estimation ~4 caractères/token comptée dans « Estimés ». Épuisement de clé détecté par signature d'erreur (402 / 429 / texte quota-crédit). Remise à zéro : <code>POST /v1/usage/reset {"provider":"meta"}</code> · sonde réelle : <code>POST /v1/usage/probe</code> · JSON : <code>GET /v1/usage</code></p>
</body></html>`;
}
// [USAGE-QUOTA] sonde RÉELLE d'un provider : requête minimale (max_tokens 1)
// vers SA base, avec SA clé et SES en-têtes/forceBody. Sert à distinguer
// « clé épuisée » de « clé invalide » de « panne ».
async function probeProvider(pv) {
  const t0 = Date.now();
  const model = pv.models[0] || 'default';
  const mk = (status, http, message) => ({ name: pv.name, model, status, http, latency_ms: Date.now() - t0, message: String(message || '').slice(0, 200) });
  const key = provKey(pv, false);
  if (!key && !pv.noAuth) return mk('auth', null, `aucune clé configurée (${pv.keyEnv || 'pas de keyEnv'})`);
  const pBody = applyForceBody(applyBodyFix({ model, messages: [{ role: 'user', content: 'ping' }], max_tokens: 1, stream: false }, pv.bodyFix), pv.forceBody);
  const ac = new AbortController();
  const tmo = setTimeout(() => ac.abort(), Math.min(Number(pv.timeoutMs ?? 0) || FETCH_TIMEOUT_MS, 20000));
  let r, text = '';
  try {
    const uh = { 'Content-Type': 'application/json', ...(pv.headers || {}) };
    if (key) uh.Authorization = `Bearer ${key}`;
    r = await fetch(`${pv.base}/chat/completions`, { method: 'POST', headers: uh, body: JSON.stringify(pBody), signal: ac.signal });
    text = await r.text().catch(() => '');
  } catch (err) {
    clearTimeout(tmo);
    return mk('error', null, `réseau ${err.message}`);
  }
  clearTimeout(tmo);
  const errStr = `${r.status} ${String(text).slice(0, 200)}`;
  if (r.ok && !bodyIsError(r.status, text, false)) return mk('ok', r.status, 'réponse OK');
  if (quotaHit(errStr)) { provQuota(pv.name, errStr); return mk('quota', r.status, errStr); }
  if (r.status === 401 || r.status === 403) return mk('auth', r.status, errStr);
  return mk('error', r.status, errStr);
}
function isPath(u, p) { return u === p || u.startsWith(p + '?'); }
async function readJsonLoose(req) {
  try {
    const raw = await new Promise((resolve, reject) => {
      const chunks = []; req.on('data', (c) => chunks.push(c)); req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))); req.on('error', reject);
    });
    if (!raw || !raw.trim()) return { ok: true, body: {} };
    return { ok: true, body: JSON.parse(raw) };
  } catch { return { ok: false, body: {} }; }
}
// =================== fin [USAGE-QUOTA 20260919] ===================

function cbSkip(model) {
  const s = cbState.get(model);
  return !!(s && Date.now() < s.skipUntil);
}
function cbNote(model, ok, err) {
  const now = Date.now();
  if (ok) { if (cbState.delete(model)) { log(`[disjoncteur] ${model} rétabli`); cbSave(); } return; }
  // [DISJONCTEUR-402 20260916] 402 no_provider_under_bid = provider épuisé,
  // COÛTE (le bid est au-dessus du marché) PAS une panne : un seul 402 =
  // skip immédiat 45 s, sans compte ni sans le disjoncteur de 3 échecs qui
  // le verrouillait 120 s à chaque fois.
  if (/402|no_provider_under_bid/i.test(String(err || ''))) {
    const s = cbState.get(model) || { fails: 0, first: now, skipUntil: 0, slowFails: 0 };
    s.skipUntil = Math.max(s.skipUntil || 0, now + CB_COOL_MS); s.fails = 0; s.first = now;
    cbState.set(model, s); cbSave();
    log(`[disjoncteur] ${model} 402 — skip rapide ${CB_COOL_MS / 1000}s (provider épuisé/coût)`);
    return;
  }
  // [DISJONCTEUR-TIMEOUT 20260919] timeout TTFT/fetch ou 503 "offers
  // exhausted" = le modèle ne peut pas servir MAINTENANT : skip immédiat
  // CB_SLOW_COOL_MS (180 s, comme la lenteur), sans attendre 3 échecs.
  // Re-sondé ensuite ; un succès rétablit aussitôt (cf. branche ok ci-dessus).
  if (isSlowTimeout(err) || /no available provider|all offers exhausted|exhausted or in cooldown/i.test(String(err || ''))) {
    const s2 = cbState.get(model) || { fails: 0, first: now, skipUntil: 0, slowFails: 0 };
    s2.skipUntil = Math.max(s2.skipUntil || 0, now + CB_SLOW_COOL_MS); s2.fails = 0; s2.first = now;
    cbState.set(model, s2); cbSave();
    log(`[disjoncteur] ${model} timeout/épuisé — skip rapide ${CB_SLOW_COOL_MS / 1000}s`);
    return;
  }
  let s = cbState.get(model);
  if (!s || now - s.first > CB_WIN_MS) s = { fails: 0, first: now, skipUntil: 0 };
  s.fails++;
  if (s.fails >= CB_FAILS) {
    s.skipUntil = Math.max(s.skipUntil || 0, now + CB_COOL_MS); s.fails = 0; s.first = now;
    log(`[disjoncteur] ${model} en cooldown ${(CB_COOL_MS / 1000) | 0}s après ${CB_FAILS} échecs`);
  }
  cbState.set(model, s);
  cbSave();
}

// [DISJONCTEUR-LENTEUR 20260916] (JP : TPS < 20 → kick 3 min) : un succès
// TROP LENT compte quand même (débit sortie mesuré). Seuils en CFG.circuit
// (slowTps/slowCoolMs/slowMinTokens). Indépendant du cooldown échecs.
const CB_TPS = Number((CFG.circuit && CFG.circuit.slowTps) ?? 20);
const CB_SLOW_COOL_MS = Number((CFG.circuit && CFG.circuit.slowCoolMs) ?? 180000);
const CB_SLOW_MINTOK = Math.max(1, Number((CFG.circuit && CFG.circuit.slowMinTokens) ?? 100));
function compTokensOf(text) {
  try { return JSON.parse(text)?.usage?.completion_tokens ?? 0; } catch { return 0; }
}
function tpsSlow(model, tStart, compTokens) {
  const el = Date.now() - tStart;
  if (!(compTokens >= CB_SLOW_MINTOK) || !(el > 0)) return false;
  const tps = compTokens / (el / 1000);
  noteTps(model, tps);
  const now = Date.now();
  const s = cbState.get(model) || { fails: 0, first: now, skipUntil: 0, slowFails: 0 };
  if (tps >= CB_TPS) {
    if (s.slowFails) { s.slowFails = 0; cbState.set(model, s); cbSave(); }
    return false;
  }
  s.slowFails = (s.slowFails || 0) + 1;
  if (s.slowFails >= 3) {
    s.slowFails = 0; s.fails = 0; s.first = now;
    s.skipUntil = now + CB_SLOW_COOL_MS;
    cbState.set(model, s);
    cbSave();
    log(`[disjoncteur] ${model} lent 3× de suite (${tps.toFixed(1)} tok/s < ${CB_TPS}) — cooldown ${(CB_SLOW_COOL_MS / 1000) | 0}s`);
    return true;
  }
  cbState.set(model, s);
  cbSave();
  log(`[disjoncteur] ${model} lent (${tps.toFixed(1)} tok/s < ${CB_TPS}, ${s.slowFails}/3)`);
  return true; // lent : ne pas reset les compteurs côté appelant
}

// [PRIORITE-VITESSE 20260916] déclaré plus haut (avant tri boot, fix TDZ).
// [PROGRESSION-PRIX 20260916] prix live partagés (JP : "progressivement dans
// l'ordre croissant jusqu'à la limite puis Agnes"). Cache 60 s (1 fetch /
// minute max, timeout 8 s) ; en panne → null et comportement legacy.
let priceCache = { ts: 0, map: null };
async function livePrices() {
  const now = Date.now();
  if (priceCache.map && now - priceCache.ts < 60000) return priceCache.map;
  try {
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 8000);
    const r = await fetch(`${CFG.upstream}/models`, { headers: { Authorization: `Bearer ${REAL_KEY}` }, signal: ctl.signal });
    clearTimeout(t);
    const md = await r.json();
    const map = {};
    for (const m of (md.data || [])) {
      if (typeof m.id === 'string') map[m.id] = { in: m?.pricing?.min_ask_in ?? null, out: m?.pricing?.min_ask_out ?? null };
    }
    priceCache = { ts: now, map };
    return map;
  } catch { return priceCache.map; }
}

// [ACCUEIL 20260919] GET / = statut lisible (navigateur inclus) : fini le
// 405 vide quand on ouvre l'URL racine. Objet partagé avec /healthz.
const GW_VERSION = '3.2-usage-quota';
const GW_BOOT_TS = Date.now();
function statusObj() {
  const providers = {};
  for (const name of providerNames()) {
    const e = usage.providers[name] || {};
    providers[name] = {
      down: provSkip(name),
      quota_exhausted: provQuotaExhausted(name),
      tokens_total: num(e.total_tokens),
    };
  }
  return {
    ok: true, name: 'inferhub-failover', version: GW_VERSION,
    uptime_s: Math.floor((Date.now() - GW_BOOT_TS) / 1000),
    endpoints: ['GET /', 'GET /healthz', 'GET /v1/models', 'POST /v1/chat/completions', 'POST /v1/responses/input_tokens',
      'GET /v1/usage', 'GET /dashboard', 'POST /v1/usage/probe', 'POST /v1/usage/reset'],
    usage_endpoint: '/v1/usage',
    dashboard: '/dashboard',
    pool: { active: poolActive, queued: poolQueue.length }, providers,
  };
}

const server = http.createServer(async (req, res) => {
  // [VITESSE 20260919] routage sans `new URL` (aucun parse par requête ;
  // sémantique pathname conservée, query tolérée).
  const rurl = req.url || '';
  // [MULTI-PROVIDER 20260919] sonde locale : le service reste OK même si
  // des providers sont en cooldown (une API down ne down pas le service).
  if (req.method === 'GET' && (rurl === '/' || rurl.startsWith('/?'))) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(statusObj()));
    return;
  }
  if (req.method === 'GET' && (rurl === '/healthz' || rurl.startsWith('/healthz?'))) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(statusObj()));
    return;
  }
  // [STATS-TOKENS 20260919] compteurs en mémoire (tokens, erreurs classées,
  // disjoncteurs) pour l'app Rust locale. Route ADDITIVE : `/` et `/healthz`
  // restent strictement inchangés.
  if (req.method === 'GET' && (rurl === '/stats' || rurl.startsWith('/stats?'))) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(statsObj()));
    return;
  }
  if (req.method === 'GET' && (rurl === '/v1/models' || rurl.startsWith('/v1/models?'))) {
    // [MODELS-BUFFER 20260919] bufferise AVANT writeHead : si l'amont coupe
    // en plein corps, on renvoie un 502 propre au lieu d'un 200 tronqué
    // (qui plantait en ERR_HTTP_HEADERS_SENT dans le catch ci-dessous).
    try {
      const r = await fetch(`${CFG.upstream}/models`, { headers: { Authorization: `Bearer ${REAL_KEY}` } });
      const t = await r.text();
      if (res.destroyed || res.writableEnded) return;
      res.writeHead(r.status, { 'Content-Type': 'application/json' });
      res.end(t);
    } catch (err) { if (!res.headersSent && !res.writableEnded && !res.destroyed) { res.writeHead(502, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: String(err.message) })); } }
    return;
  }
  // [USAGE-QUOTA 20260919] consultation : consommation locale + état quota.
  // Placé AVANT la garde 405 et AVANT la lecture générique du corps (les routes
  // POST d'usage lisent elles-mêmes leur corps via readJsonLoose).
  if (req.method === 'GET' && ['/v1/usage', '/usage', '/v1/quota', '/quota'].some((p) => isPath(rurl, p))) {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(usageObj()));
    return;
  }
  if (req.method === 'GET' && isPath(rurl, '/dashboard')) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(dashboardHtml());
    return;
  }
  // [USAGE-QUOTA 20260919] sonde RÉELLE : une requête minimale par provider
  // pour distinguer « clé épuisée » de « clé invalide » de « panne ».
  if (req.method === 'POST' && isPath(rurl, '/v1/usage/probe')) {
    const rj = await readJsonLoose(req);
    const only = Array.isArray(rj.body?.providers) ? rj.body.providers.map(String) : null;
    const cibles = PROVIDERS.filter((pv) => !only || only.includes(pv.name));
    const resultats = await Promise.all(cibles.map((pv) => probeProvider(pv).catch((err) => ({ name: pv.name, model: pv.models[0] || 'default', status: 'error', http: null, latency_ms: 0, message: `sonde en échec : ${err.message}` }))));
    const okAll = resultats.every((x) => x.status === 'ok');
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: okAll, providers: resultats }));
    return;
  }
  // [USAGE-QUOTA 20260919] remise à zéro après recharge de la clé : lève le
  // drapeau quota ET le cooldown de panne du/des provider(s) visés.
  if (req.method === 'POST' && isPath(rurl, '/v1/usage/reset')) {
    const rj = await readJsonLoose(req);
    const demandes = rj.body?.provider ? [String(rj.body.provider)] : providerNames();
    const faits = [];
    for (const name of demandes) {
      const q = quotaClear(name, 'reset manuel');
      const st = provState.get(name);
      if (st) { provState.delete(name); log(`[provider] ${name} cooldown levé (reset manuel)`); }
      if (q || st) faits.push(name);
    }
    cbSave();
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, reset: faits }));
    return;
  }
  // [USAGE-QUOTA 20260919] fin des routes usage/quota.
  if (req.method !== 'POST') { res.writeHead(405, { 'Content-Type': 'application/json' }); res.end('{"error":"méthode non supportée (voir GET / pour les endpoints)"}'); return; }

  let body;
  try {
    const raw = await new Promise((resolve, reject) => {
      const chunks = []; req.on('data', (c) => chunks.push(c)); req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))); req.on('error', reject);
    });
    body = JSON.parse(raw);
  } catch { res.writeHead(400, { 'Content-Type': 'application/json' }); res.end('{"error":"body JSON invalide"}'); return; }

  // [TOKEN-COUNT-LOCAL 20260919] comptage de tokens LOCAL. Les clients (MiniMax
  // Code, Zed) sondent `/v1/responses/input_tokens` avant chaque tour. Sans
  // route dédiée, ces POST tombaient dans le handler chat/completions : un
  // APPEL MODÈLE COMPLET brûlé par comptage, pour un corps que le client
  // jugeait malformé (il retombait sur son estimation BPE de toute façon).
  // On répond ici, sans slot de pool ni appel amont.
  if (/^\/(?:v1\/)?(?:responses\/input_tokens|messages\/count_tokens)\b/.test(rurl)) {
    const n = estimateTokens(body);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      object: 'response.input_tokens',
      input_tokens: n,
      usage: { input_tokens: n, total_tokens: n },
    }));
    return;
  }

  // [STATS-TOKENS 20260919] entrée du handler `POST /v1/chat/completions` :
  // `requests` compté ici, `served`/`failed` à la fin de la requête CLIENT (via
  // les événements de la réponse — cf. stRequestStart).
  stRequestStart(res);

  // [PERF-100RPS 20260919] file bornée : au-delà on refuse vite (429) au
  // lieu d'entasser (latence explosive + mémoire). Dimensionner
  // maxConcurrent ≈ débit_souhaité × latence_moyenne_upstream (ex. 100 rps ×
  // 2 s ≈ 200 slots).
  const MAX_QUEUE = Math.max(0, Number(CFG.maxQueue ?? 1000));
  if (poolQueue.length >= MAX_QUEUE) {
    res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': '1' });
    res.end(JSON.stringify({ error: 'router saturé (file pleine), réessayez' }));
    return;
  }

  const requested = body.model || 'combo/cheap';
  const chain = chainFor(requested);
  const fwdHeaders = {
    'Content-Type': 'application/json',
    Authorization: REAL_KEY ? `Bearer ${REAL_KEY}` : (req.headers.authorization || ''),
  };
  for (const h of CFG.forwardHeaders || []) if (req.headers[h]) fwdHeaders[h] = req.headers[h];
  // Les plafonds de prix Inferhub ne concernent pas Agnes : on les retire
  // avant de re-ouvrir vers l'API Agnes (sinon Agnes les ignore mais on les
  // garde propres pour tracer quel modele a servi).
  const isDirectAgnes = isAgnesModel(requested);
  // [DIRECT-PROVIDER-MODEL 20260919] le modèle demandé appartient à un provider
  // déclaré (ex. `muse-spark-1.3-contributor` → provider `meta`) : Inferhub ne
  // le sert pas, on va DROIT à ce provider. Sans ce court-circuit, la requête
  // partait sur Inferhub et était servie par un AUTRE modèle (réponse trahissant
  // le modèle demandé). Agnes garde son chemin historique dédié.
  const ownerProv = isDirectAgnes ? null : PROVIDERS.find((p) => p.models.includes(requested)) || null;
  const isDirectProvider = !!ownerProv;
  const useAgnesChain = (isDirectAgnes || isDirectProvider) ? [requested] : chain;

  let lastErr = 'chaîne vide';
  // [VITESSE 20260919] stats body LAZY (fail-fast 413/contexte uniquement) :
  // évite un 2e stringify du body (jusqu'à 1 Mo+) à chaque requête.
  let bodyBytes = -1, bodyMsgs = -1;
  const bodyStats = () => {
    if (bodyBytes < 0) {
      try { bodyBytes = Buffer.byteLength(JSON.stringify(body)); } catch { bodyBytes = 0; }
      bodyMsgs = Array.isArray(body?.messages) ? body.messages.length : 0;
    }
    return [bodyBytes, bodyMsgs];
  };
  // [CHAIN-PASSES 20260915] l'amont flappe (400 upstream error en rafale sur
  // TOUS les modeles, avec des succes intermittents au milieu) : on rejoue la
  // chaine entiere CHAIN_PASSES fois avant le 502. Une passe supplementaire ne
  // coute que quelques secondes sur les erreurs rapides (400/402 ~1 s) et
  // evite d'attendre le retry Zed (22 s). Les timeouts 25 s restent le pire cas.
  const CHAIN_PASSES = (isDirectAgnes || isDirectProvider) ? 1 : Math.max(1, Number(CFG.chainPasses ?? 1));
  // [PROGRESSION-PRIX] pour combo/* : DOUBLE FILE (JP) — file pas-chère
  // (chaîne + secondeChance, dédup, triée prix-croissant LIVE) × 2 passes,
  // puis file enchère (escalade triée, filtrée au plafond), UNE passe —
  // puis Agnes. Sans prix live : phases legacy.
  let listChain = useAgnesChain;
  let listSecond = CFG.secondChance;
  let listEsc = CFG.escalationModels;
  let passesForReq = CHAIN_PASSES;
  if (!isDirectAgnes && !isDirectProvider && String(requested).startsWith('combo/')) {
    const px = await livePrices();
    if (px) {
      const E = CFG.escalation || {};
      const capIn = Number(E.maxIn ?? 0.005), capOut = Number(E.maxOut ?? 0.03);
      const escSet = new Set(CFG.escalationModels || []);
      const seen = new Set();
      const cheap = [], esc = [];
      for (const m of [...useAgnesChain, ...(CFG.secondChance || []), ...(CFG.escalationModels || [])]) {
        if (typeof m !== 'string' || seen.has(m)) continue;
        seen.add(m);
        (escSet.has(m) ? esc : cheap).push(m);
      }
      const pri = (id) => { const e = px[id]; return (e && typeof e.in === 'number') ? e.in : Infinity; };
      const over = (id) => { const e = px[id]; return !!(e && ((typeof e.in === 'number' && e.in > capIn) || (typeof e.out === 'number' && e.out > capOut))); };
      const tie = (a, b) => {
        if (a === 'ag/gemini-3.8-flash-high' && b === 'ag/gemini-3.7-flash-high') return -1;
        if (a === 'ag/gemini-3.7-flash-high' && b === 'ag/gemini-3.8-flash-high') return 1;
        return 0;
      };
      const tri = (arr) => arr.sort((a, b) => (pri(a) - pri(b)) || cmpVitesse(a, b) || tie(a, b));
      listChain = tri(cheap); listSecond = []; listEsc = tri(esc.filter((m) => !over(m))); passesForReq = CHAIN_PASSES;
      log(`[prix] double file : pas-chers (${listChain.map((m) => `${m}(${px[m]?.in ?? 'alias'})`).join(' > ')}) ×${passesForReq} → enchère (${listEsc.map((m) => `${m}(${px[m]?.in ?? '?'})`).join(' > ') || 'vide'}) → Agnes`);
    } else {
      log(`[prix] prix live indisponibles → phases legacy`);
    }
  }
  // [MULTI-PROVIDER 20260919] primaire en cooldown → on saute TOUTES ses
  // étapes et on va direct aux providers de secours (zéro timeout brûlé).
  if (!isDirectAgnes && provSkip('inferhub')) {
    log(`↻ Inferhub en cooldown (panne/API) – providers de secours directs`);
    passesForReq = 0; listSecond = []; listEsc = [];
    lastErr = 'inferhub en cooldown (panne/API)';
  }
  // [DIRECT-PROVIDER-MODEL 20260919] modèle possédé par un provider : on saute
  // TOUTE la file Inferhub (elle ne le sert pas) et on va droit à ce provider.
  if (isDirectProvider) {
    log(`↻ ${requested} servi par le provider ${ownerProv.name} – Inferhub sauté`);
    passesForReq = 0; listSecond = []; listEsc = [];
    lastErr = `modèle servi par le provider ${ownerProv.name}`;
  }
  let outageStreak = 0; // pannes provider consécutives (réseau/5xx/timeout)
  let lastOutageModel = null; // [TIMEOUT-FAILOVER] modèle du dernier outage (streak inter-modèles)
  let inferhubOut = false; // true → Inferhub abandonné pour cette requête
  passesLoop:
  for (let p = 1; p <= passesForReq; p++) {
  for (let i = 0; i < listChain.length; i++) {
    const model = listChain[i];
    if (!isAgnesModel(model) && cbSkip(model)) { log(`↻ ${model} en cooldown (disjoncteur) – sauté`); continue; }
    const agnesMode = isAgnesModel(model);
    let deadModel = false; // mort déterministe du modèle (404/403/401/402)
    for (let a = 1; a <= ATTEMPTS_PER_MODEL; a++) {
      let r;
      try {
        // [AGNES-FALLBACK] appels Agnes : URL/agresseur differents,
        // mais le reste du pipeline est partage (slot, watchdog TTFT,
        // stream, safeRelease).
// [AGNES-CAPS-MIRROR] on garde les caps Inferhub (0.01/0.05) dans les
// headers transmis à Agnes : Agnes les lit (le doc l'indique) et sert la cap.
// Donc on ne retire PAS les caps pour les modeles agnes-*.
        // [AGNES-RETRIE-CAPS] Agnes ne lit PAS les caps Inferhub (0.01/0.05 USDC) ;
        // leur presence dans les headers provoque un 401 "invalid api key" fake
        // (le serveur Agnes rejette la requete au pre-check). On retire donc
        // les headers x-max-input-price / x-max-output-price pour les modeles
        // agnes-* : seule l'Authorization doit etre envoyee.
        //
        // [AGNES-KEY-401-RETRY 20260915] si le pre-check renvoie 401 avec la
        // cle en memoire (clé periodee au boot, rotation), on force une
        // re-lecture de la variable utilisateur et on retente une seule fois.
        const upHeaders = agnesMode
          ? { 'Content-Type': 'application/json', Authorization: agnesKey ? `Bearer ${agnesKey}` : '' }
          : fwdHeaders;
        const upstreamBase = agnesMode ? AGNES_API : CFG.upstream;
        let rUp = await attemptUpstream(model, upHeaders, body, res, undefined, agnesMode ? upstreamBase : undefined).catch((err) => ({ ok: false, err: `réseau ${err.message}` }));
        if (agnesMode && /\b401\b|invalid api key|invalid_request/i.test(String(rUp.err || ''))) {
          const fresh = findAgnesKey(true, '401-retry');
          if (fresh && fresh !== agnesKey) {
            log(`↻ clé Agnes re-chargee (${fresh.slice(0, 8)}…), retente le mode ${model}`);
            const hRetry = { ...upHeaders, Authorization: `Bearer ${fresh}` };
            const rUp2 = await attemptUpstream(model, hRetry, body, res, undefined, AGNES_API).catch((err) => ({ ok: false, err: `réseau ${err.message}` }));
            if (rUp2.ok) {
              log(`✓ ${model} réussi via clé re-chargee (${fresh.slice(0, 8)}…)`);
              return;
            }
            rUp = rUp2;
          }
        }
        // [AGNES-DIAG 20260915] empreinte de la clé effective envoyée.
        if (agnesMode) log(`[agnes-diag] clé envoyée = ${agnesKeyFp()}`);
        if (agnesMode && /\b401\b|invalid api key|invalid_request/i.test(String(rUp.err || ''))) {
          // [AGNES-KEY-RETRY 20260915] la clé en mémoire est périmée (rotation)
          // : on en re-charge une fraiche et on retente une seule fois.
          const fresh = findAgnesKey(true, '401-retry');
          if (fresh && fresh !== agnesKey) {
            log(`↻ clé Agnes re-chargee, retente le mode ${model}`);
            const hRetry = { ...upHeaders, Authorization: `Bearer ${fresh}` };
            const rUp2 = await attemptUpstream(model, hRetry, body, res, undefined, AGNES_API).catch((err) => ({ ok: false, err: `réseau ${err.message}` }));
            if (rUp2.ok) {
              if (i > 0 || a > 1 || p > 1) log(`✓ ${model} réussi (modèle ${i + 1}/${listChain.length}, essai ${a}/${ATTEMPTS_PER_MODEL}, passe ${p}/${passesForReq} [agnes-retry])`);
              return;
            }
            rUp = rUp2;
          }
        }
        r = rUp;
      } catch (err) {
        r = { ok: false, err: `réseau ${err.message}` };
      }
      // [USAGE-QUOTA 20260919] provider qui a réellement servi ce modèle.
      const usageProv = agnesMode ? 'agnes' : 'inferhub';
      if (r.ok) {
        if (!isAgnesModel(model)) cbNote(model, !r.slow, r.err);
        if (!agnesMode) provNote('inferhub', true);
        usageNote(usageProv, model, r.usage, null);
        if (i > 0 || a > 1 || p > 1) log(`✓ ${model} réussi (modèle ${i + 1}/${listChain.length}, essai ${a}/${ATTEMPTS_PER_MODEL}, passe ${p}/${passesForReq}${agnesMode ? ' [agnes]' : ''})`);
        return;
      }
      if (!isAgnesModel(model)) cbNote(model, false, r.err);
      // [USAGE-QUOTA 20260919] échec : aucun token compté (on n'invente rien),
      // erreur retenue, et détection d'épuisement de la clé du provider concerné.
      usageNote(usageProv, model, null, r.err);
      if (quotaWatch(usageProv, r.err)) {
        log(`✗ ${usageProv} : clé épuisée (quota) — ${String(r.err).slice(0, 120)}`);
        if (!agnesMode) { inferhubOut = true; break passesLoop; }
      }
      lastErr = `${model} essai ${a}/${ATTEMPTS_PER_MODEL} passe ${p}/${passesForReq}${agnesMode ? ' [agnes]' : ''}: ${r.err}`;
      log(`✗ ${lastErr}`);
      if (r.forwarded) return; // contenu déjà envoyé au client : impossible de reprendre
      if (isBodyTooLarge(r.err) || (agnesMode === false && isContextTooLargeForAll(r.err))) {
        const isTokens = /exceeds the maximum number of tokens|maximum number of tokens allowed/i.test(String(r.err || ''));
        const [bb, bm] = bodyStats();
        log(`✗ ${isTokens ? 'contexte' : 'body'} trop gros (${bb} octets, ${bm} messages) – fail-fast, pas de bascule`);
        if (!res.headersSent) {
          res.writeHead(isTokens ? 400 : 413, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            error: {
              type: isTokens ? 'context_too_large' : 'request_too_large',
              message: isTokens
                ? `Contexte trop volumineux pour TOUS les modeles de la chaine (limite max ${MAX_CHAIN_TOKENS} tokens) : aucun retry ne passera. Cree un nouveau thread Zed (ou /compact) et reduis le contexte (retire les gros @fichiers / outputs d'outils).`
                : `Contexte trop volumineux (${bb} octets, ${bm} messages) : request body exceeds configured limit (Inferhub). Cree un nouveau thread Zed ou reduis le contexte (retire gros @fichiers / outputs).`,
              hint: 'fail-fast inferhub-failover : meme body rejette, bascule inutile vers les autres modeles.',
            },
            detail: lastErr,
          }));
        } else if (!res.writableEnded) res.end();
        return;
      }
      if (isModelDead(r.err)) {
        deadModel = true;
        log(`✗ ${model} mort deterministe – on saute au modele suivant sans re-essayer`);
        break;
      }
      // [TIMEOUT-FAILOVER 20260919] timeout lent sur UN modèle = modèle lent,
      // PAS provider mort : bascule au modèle suivant SANS brûler l'autre
      // essai (20 s de plus pour rien). MAIS les timeouts sur des modèles
      // DIFFÉRENTS signalent une panne provider (streak préservé → secours).
      if (!agnesMode && isSlowTimeout(r.err)) {
        if (model !== lastOutageModel) { outageStreak++; lastOutageModel = model; }
        else outageStreak = Math.max(outageStreak, 1);
        if (outageStreak >= 2) {
          inferhubOut = true;
          log(`✗ Inferhub en panne (timeouts ${String(r.err).slice(0, 100)}) – abandon, bascule providers`);
          break passesLoop;
        }
        deadModel = true;
        log(`✗ ${model} lent (${String(r.err).slice(0, 100)}) – bascule immédiate au modèle suivant`);
        break;
      }
      // [MULTI-PROVIDER 20260919] panne PROVIDER (pas modèle) : les autres
      // modèles Inferhub sont HS aussi — on abandonne Inferhub pour cette
      // requête et on passe aux providers de secours (sans brûler N×timeouts
      // sur les passes/seconde-chance/escalade, qui sont même-provider).
      // Les 400 flappy NE comptent pas : les passes anti-flap sont préservées.
      if (!agnesMode && isProviderOutage(r.err)) {
        if (++outageStreak >= 2 || ATTEMPTS_PER_MODEL <= 1) {
          inferhubOut = true;
          log(`✗ Inferhub en panne (${String(r.err).slice(0, 120)}) – abandon, bascule providers`);
          break passesLoop;
        }
      } else if (!agnesMode) outageStreak = 0;
      if (a < ATTEMPTS_PER_MODEL) await sleep(RETRY_DELAY_MS);
    }
    // [RETRY-1S-MUSE 20260919] le modèle a échoué APRÈS son nouvel essai à
    // RETRY_DELAY_MS : on n'explore pas le reste de la chaîne (ni les modèles
    // suivants, ni la seconde chance, ni l'escalade prix) et on va droit au
    // provider Muse. Exception : les morts DÉTERMINISTES du modèle
    // (404/403/401/402 « no_provider_under_bid ») restent de simples sauts —
    // sinon le 402 du modèle le moins cher (fréquent, cf. [DISJONCTEUR-402])
    // enverrait TOUTES les requêtes sur Muse et ruinerait le tri par prix.
    if (SKIP_CHAIN_ON_ERROR && !agnesMode && !deadModel) {
      inferhubOut = true;
      log(`✗ ${model} en échec après ${ATTEMPTS_PER_MODEL} essai(s) – abandon de la chaîne, bascule directe sur ${MUSE_PROVIDER ? MUSE_PROVIDER.name : 'les providers de secours'}`);
      break passesLoop;
    }
    log(`→ bascule sur le modèle suivant de la chaîne`);
  }
  if (p < passesForReq) log(`↻ passe ${p}/${passesForReq} épuisée sans succès — on rejoue la chaîne (amont flappy)`);
  }
  // [SECONDE-CHANCE 20260916] modèles essayés UNE fois après épuisement des
  // passes, AVANT la boucle qwen (demande JP : ali/deepseek après 2 chaînes
  // totales, avant Agnes). Pas de rejeu : un essai chacun, les morts
  // déterministes sont sautées.
  if (!isDirectAgnes && !inferhubOut && Array.isArray(listSecond)) {
    for (const model of listSecond) {
      if (res.destroyed || res.writableEnded) return;
      if (cbSkip(model)) { log(`↻ ${model} en cooldown (disjoncteur) – sauté`); continue; }
      const r2 = await attemptWithRetry(model, fwdHeaders, body, res);
      cbNote(model, !!(r2.ok && !r2.slow), r2.err);
      if (r2.ok) { provNote('inferhub', true); usageNote('inferhub', model, r2.usage, null); log(`✓ ${model} réussi (seconde chance après passes)`); return; }
      usageNote('inferhub', model, null, r2.err);
      quotaWatch('inferhub', r2.err);
      if (r2.forwarded) return; // contenu déjà envoyé : impossible de reprendre
      if (isModelDead(r2.err) || isBodyTooLarge(r2.err)) { log(`✗ ${model} seconde chance ignorée (${String(r2.err).slice(0, 120)})`); continue; }
      lastErr = `${model} seconde chance: ${r2.err}`;
      log(`✗ ${lastErr}`);
    }
  }
  // [ESCALADE] passes + seconde chance épuisées → on tente des modèles PLUS
  // CHERS (plafond JP 0.005/0.03), UN essai chacun, morts déterministes
  // sautées. Avant la boucle qwen et Agnes.
  if (!isDirectAgnes && !inferhubOut && Array.isArray(listEsc) && listEsc.length) {
    for (const model of listEsc) {
      if (res.destroyed || res.writableEnded) return;
      if (cbSkip(model)) { log(`↻ ${model} en cooldown (disjoncteur) – sauté`); continue; }
      const r3 = await attemptWithRetry(model, fwdHeaders, body, res);
      cbNote(model, !!(r3.ok && !r3.slow), r3.err);
      if (r3.ok) { provNote('inferhub', true); usageNote('inferhub', model, r3.usage, null); log(`✓ ${model} réussi (escalade prix)`); return; }
      usageNote('inferhub', model, null, r3.err);
      quotaWatch('inferhub', r3.err);
      if (r3.forwarded) return; // contenu déjà envoyé : impossible de reprendre
      if (isModelDead(r3.err) || isBodyTooLarge(r3.err)) { log(`✗ ${model} escalade ignorée (${String(r3.err).slice(0, 120)})`); continue; }
      lastErr = `${model} escalade: ${r3.err}`;
      log(`✗ ${lastErr}`);
    }
  }
  // [MULTI-PROVIDER 20260919] Inferhub épuisé (ou en panne) + contexte OK :
  // chaque provider de secours dans l'ordre config, chacun avec ses modèles.
  // Un provider DOWN (réseau/5xx/timeout) est abandonné aussitôt (cooldown) :
  // une API down ne down ni les autres ni le service. Remplace le fallback
  // Agnes codé en dur (préservé par défaut : `providers` absent = Agnes).
  // [DIRECT-PROVIDER-MODEL 20260919] modèle possédé par un provider : Inferhub
  // n'a jamais été sollicité, on ne peut donc PAS lui imputer un échec (sinon
  // une demande Muse ferait tomber le disjoncteur du primaire), et on ne
  // parcourt QUE le provider propriétaire.
  if (!isDirectAgnes && !isDirectProvider && !provSkip('inferhub')) provNote('inferhub', false, lastErr);
  // [RETRY-1S-MUSE 20260919] après un abandon de chaîne pour erreur persistante,
  // on ne parcourt PAS les autres providers de secours : Muse (meta) sert la
  // requête directement, conformément à la politique demandée.
  const provList = isDirectProvider
    ? [ownerProv]
    : (SKIP_CHAIN_ON_ERROR && inferhubOut && MUSE_PROVIDER ? [MUSE_PROVIDER] : PROVIDERS);
  if (!isDirectAgnes && !isContextTooLargeForAll(lastErr)) {
    // [LAST-CHANCE 20260919] TOUS les providers en cooldown en même temps =
    // signature d'un blip réseau local (les 3 disjoncteurs s'arment en < 1 s),
    // pas de trois API down. Renvoyer un 502 immédiat transformait ce blip de
    // 2 s en 45 s de panne totale (constaté le 19/09 à 16:52 et 16:53 : les
    // 3 providers armés à 6 ms d'intervalle). On retente donc le provider dont
    // le cooldown expire le plus tôt, cooldown ignoré — au plus une fois par
    // LAST_CHANCE_GAP_MS, pour ne pas payer un timeout à chaque requête si la
    // panne est bien réelle.
    const dispo = provList.filter((pv) => !provSkip(pv.name));
    for (const pv of provList) if (provSkip(pv.name)) log(`↻ provider ${pv.name} en cooldown – sauté`);
    let file = dispo;
    if (!dispo.length) {
      const nowMs = Date.now();
      // Un seul candidat, dans l'ordre de priorité configuré (celui que le
      // routeur aurait pris si aucun cooldown n'était armé) : prévisible et
      // borné à un seul essai. Tenter tous les providers à chaque requête
      // rejouerait exactement le coût que le cooldown sert à éviter.
      const cand = provList[0];
      if (cand && nowMs - lastChanceAt >= LAST_CHANCE_GAP_MS) {
        lastChanceAt = nowMs;
        log(`↻ tous les providers en cooldown – dernière chance sur ${cand.name} (cooldown ignoré)`);
        file = [cand];
      } else if (cand) {
        log(`↻ tous les providers en cooldown – 502 immédiat (dernière chance il y a ${((nowMs - lastChanceAt) / 1000) | 0}s)`);
      }
    }
    for (const pv of file) {
      if (res.destroyed || res.writableEnded) return;
      let pkey = provKey(pv, false);
      if (!pkey && !pv.noAuth) { log(`↻ provider ${pv.name} sans clé (${pv.keyEnv || 'pas de keyEnv'}) – sauté`); continue; }
      log(`↻ ${isDirectProvider ? `${requested} servi par` : 'Inferhub épuisé, bascule'} ${pv.name}${isDirectProvider ? '' : ' (secours indépendant)'}`);
      // [DIRECT-PROVIDER-MODEL 20260919] modèle demandé nommément : il passe
      // EN PREMIER, les autres modèles du provider ne servant qu'en repli (sinon
      // demander `muse-spark-1.3` renvoyait `…-contributor`, 1er de la liste).
      const pModels = isDirectProvider
        ? [requested, ...pv.models.filter((m) => m !== requested)]
        : (pv.models.length ? pv.models : [requested]);
      // [BODY-FIX-PROVIDER 20260919] corps adapté à CE provider (retrait des
      // paramètres qu'il refuse), sans toucher à celui des autres.
      // [PROVIDER-FORCE-BODY 20260919] puis champs imposés à ce provider.
      const pBody = applyForceBody(applyBodyFix(body, pv.bodyFix), pv.forceBody);
      let pvErr = 'aucun modèle';
      for (const pm of pModels) {
        if (res.destroyed || res.writableEnded) return;
        const uh = { 'Content-Type': 'application/json', ...(pv.headers || {}) };
        if (pkey) uh.Authorization = `Bearer ${pkey}`;
        let rf = await attemptWithRetry(pm, uh, pBody, res, pv.base, pv);
        if (!rf.ok && /\b401\b|invalid api key|invalid_request/i.test(String(rf.err || '')) && !pv.staticKey && pv.keyEnv) {
          const fresh = provKey(pv, true); // rotation : re-lecture variable utilisateur
          if (fresh && fresh !== pkey) {
            log(`↻ clé ${pv.name} re-chargée (${fresh.slice(0, 8)}…), nouvel essai`);
            pkey = fresh;
            rf = await attemptUpstream(pm, { ...uh, Authorization: `Bearer ${fresh}` }, pBody, res, undefined, pv.base, pv).catch((err) => ({ ok: false, err: `réseau ${err.message}` }));
          }
        }
        if (rf.ok) {
          provNote(pv.name, true);
          // [USAGE-QUOTA 20260919] tokens imputés au provider qui a réellement servi.
          usageNote(pv.name, pm, rf.usage, null);
          log(`✓ ${pm} réussi (provider ${pv.name})`);
          return;
        }
        pvErr = `${pm} [${pv.name}]: ${rf.err}`;
        lastErr = pvErr;
        log(`✗ ${lastErr}`);
        // [USAGE-QUOTA 20260919] échec : requests/failed seulement (aucun token
        // inventé) + détection d'épuisement de la clé de CE provider.
        usageNote(pv.name, pm, null, rf.err);
        if (quotaWatch(pv.name, rf.err)) {
          log(`✗ provider ${pv.name} : clé épuisée (quota) – provider suivant`);
          break; // clé sans tokens : inutile d'essayer ses autres modèles
        }
        if (rf.forwarded) return; // contenu déjà envoyé : impossible de reprendre
        if (isBodyTooLarge(rf.err)) break; // limite de CE provider : suivant
        if (isProviderOutage(rf.err)) break; // provider DOWN : suivant aussitôt
      }
      provNote(pv.name, false, pvErr);
    }
  }
  if (!res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: `failover: tous les providers ont échoué (inferhub${PROVIDERS.length ? ' + ' + PROVIDERS.map((p) => p.name).join(' + ') : ''})`, detail: lastErr }));
  } else if (!res.writableEnded) res.end();
});

const PORT = process.env.FAILOVER_PORT || CFG.port;
server.listen(PORT, '127.0.0.1', () => log(`inferhub-failover v2 sur 127.0.0.1:${PORT} (clé: ${REAL_KEY ? 'OK' : 'ABSENTE'}, agnes: ${agnesKey ? 'OK (' + agnesKey.slice(0, 8) + '…)' : 'ABSENTE'})`));
