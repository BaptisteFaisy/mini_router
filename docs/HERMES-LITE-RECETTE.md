# Recette de bout en bout du gate hermes-lite (2026-10-07)

PR témoin du runbook `~/Bureau/hermes-port/RUNBOOK-PORTAGE.md` § 3.6.

- Née volontairement **1 commit derrière `main`**, sans conflit.
- Attendu : le gate (`hermes-lite.yml` + `.github/hermes/gate.py`) détecte le
  retard → rebase **serveur** (`PUT /pulls/N/update-branch`, rebase) →
  l'événement `synchronize` re-déclenche le gate → PR fraîche + aucun check
  requis (`HERMES_REQUIRED_CHECKS` vide) → **merge automatique (rebase)**.
- Aucune action humaine attendue entre l'ouverture et le merge.
- Référence : BaptisteFaisy/duello#841 ([HERMES-REFRESH]).

Bump post-fix [HERMES-LITE-SETTLE-20261007] : relance la recette avec le settle dans le run du gate.
