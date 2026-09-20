# Demarre la passerelle inferhub-failover si elle n'est pas deja vivante.
# Idempotent : utilisable au logon (dossier Demarrage) et a la main.
#
# [DELEGATION 20260919] Ce script dupliquait la logique du lanceur : garde de
# sante, rotation du log, detachement `cmd /c`. La duplication avait deja
# diverge et produit un bug : la garde testait /v1/models, endpoint PROXIFIE
# vers l'amont, donc une panne d'Inferhub la faisait conclure a tort que la
# passerelle etait morte (routeur vivant, amont mort : /v1/models = 502,
# /healthz = 200). Une seule source de verite desormais :
# inferhub-failover-launch.ps1 (detachement propre + log en append + rotation).
$ErrorActionPreference = 'Stop'

& "$PSScriptRoot\inferhub-failover-launch.ps1"
exit $LASTEXITCODE
