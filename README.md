# mini_router — passerelle locale (failover multi-modèles)

Routeur HTTP local, compatible API OpenAI (`/v1/...`), qui se place entre un
client (Zed, pi, mcode, OpenCode, tout client OpenAI) et un amont unique
(`https://api.inferhub.dev/v1`, surchargeable par `FAILOVER_UPSTREAM`).

Principe : **tout message d'erreur fait passer au modèle suivant de la chaîne.**
Sont considérés comme erreurs : statut HTTP ≥ 400, corps 200 contenant
`{"error": ...}`, réponse sans `choices`, événement d'erreur dans le flux SSE,
stream interrompu avant `[DONE]`, erreur réseau ou timeout (TTFT / global).

Écoute par défaut : `http://127.0.0.1:18100`.

> ## PROTECTION COMBO/CHEAP — NE PAS TOUCHER — DO NOT TOUCH
>
> **`combo/cheap` DOIT rester TOUJOURS DISPONIBLE et FONCTIONNELLE.**
> Directive JP du 2026-09-19.
>
> - Ne JAMAIS supprimer ni vider `chains["combo/cheap"]` dans
>   `inferhub-failover.json`.
> - Ne JAMAIS retirer le modèle par défaut `'combo/cheap'` ni la restauration
>   automatique (`COMBO_CHEAP_DEFAULT` + garde `chainFor`) dans
>   `inferhub-failover.mjs`.
> - Chaîne fonctionnelle de référence : modèles directs pas chers PUIS le
>   routeur `combo/cheap` en dernier recours (règle ROUTER-LAST).
> - La chaîne est exposée sur `GET /` et `GET /healthz` (champ `comboCheap`).

## Fichiers

| Fichier | Rôle |
|---|---|
| `inferhub-failover.mjs` | la passerelle (Node ≥ 18, zéro dépendance) |
| `inferhub-failover.json` | chaînes, secours, providers, limites, garde-fous |
| `inferhub-failover-launch.ps1` | lanceur Windows : détachement propre, log en append + rotation |
| `inferhub-failover-start.ps1` / `.vbs` | démarrage idempotent (logon, raccourci) |
| `inferhub-failover-restart.ps1` | redémarrage |
| `inferhub-failover-watchdog.ps1` | surveillance et relance |
| `inferhub-failover-selftest.mjs` | autotest de la passerelle |
| `inferhub-failover-retry-muse-*.mjs` | sondes d'intégration provider muse/meta |
| `test/router-failover.test.mjs` | « une API down ne down pas tout » |
| `test/combo-cheap.test.mjs` | verrou combo/cheap (restauration + défaut + config) |
| `test/router-provider-timeout.test.mjs` | timeouts provider (TTFT / global) |

## Configuration

`inferhub-failover.json` — clés principales :

- `port` (18100), `upstream`, `keyEnv` (`INFERHUB_API_KEY`),
  `forwardHeaders`, `retryBodyPattern`, `attemptsPerModel` ;
- `circuit` : disjoncteur par modèle (`fails`, `windowMs`, `coolMs`, seuils de
  lenteur `slowTps` / `slowCoolMs` / `slowMinTokens`) ;
- `chains` : ordre de repli par modèle demandé ;
- `secondChance`, `defaultBackups`, `escalation`, `speedPriority` ;
- `providers` : amonts secondaires (base, `keyEnv`, modèles, timeouts,
  réécritures de corps).

**Aucun secret dans ce dépôt.** Les clés sont lues dans l'environnement
(`INFERHUB_API_KEY`, `META_API_KEY`, `AGNES_API_KEY`, ...) ; sous Windows la
variable utilisateur fait foi (rotation), sinon `process.env`.

Surcharges par variables d'environnement : `FAILOVER_CONFIG` (autre fichier de
config), `FAILOVER_UPSTREAM` (amont de test / faux fournisseur).

## Lancer

```powershell
# Windows — démarrage détaché, log en append + rotation
powershell -ExecutionPolicy Bypass -File .\inferhub-failover-launch.ps1
```

```bash
# Linux / macOS
INFERHUB_API_KEY=... node inferhub-failover.mjs
```

Vérification : `GET /healthz` (200 même si l'amont est mort) et `GET /`.

## Tests

```bash
node --test test/router-failover.test.mjs test/combo-cheap.test.mjs test/router-provider-timeout.test.mjs
```

Les tests démarrent la passerelle sur un port libre avec un faux amont local :
aucun appel réel au réseau, aucun impact sur le routeur en service.

## Chaîne combo/cheap (fonctionnelle)

```json
"combo/cheap": [
  "cbcn/deepseek-v4.1-flash",
  "cx/gpt-5.6-luna",
  "combo/cheap"
]
```

Si la config oublie cette chaîne, la passerelle la restaure au démarrage
(`COMBO_CHEAP_DEFAULT`, log `[combo/cheap] chaine absente/vide...`) et
`chainFor('combo/cheap')` ne rend jamais une chaîne vide.
