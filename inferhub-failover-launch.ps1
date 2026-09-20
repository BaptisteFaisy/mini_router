# inferhub-failover-launch.ps1
# Lance la passerelle inferhub-failover si elle n'est pas deja vivante.
# Idempotent : utilisable par le watchdog, par le restart, ou a la main.
#
# [DETACH 20260919] POURQUOI CE FICHIER EXISTE
# L'ancienne methode
#     Start-Process -WindowStyle Hidden node -ArgumentList "...mjs" -RedirectStandardError $log
# faisait heriter au processus node le stdout du parent, c'est-a-dire le pipe du shell
# appelant (Start-Process avec une seule redirection passe en UseShellExecute=false et
# laisse les flux non rediriges herites). Comme node tourne sans fin, le pipe ne se
# fermait jamais : l'appelant attendait jusqu'a son timeout. C'est ce qui a coupe la
# session du 19/09 au moment precis de la verification finale, en laissant la passerelle
# arretee alors que l'ancien processus avait deja ete tue.
# On passe desormais par `cmd /c ... 2>> log` : aucun handle n'est herite.
#
# Le log est en APPEND (2>>) et non plus ecrase a chaque relance : la cause des morts
# silencieuses est preservee. Rotation au-dela de 5 Mo, 5 archives conservees.

$ErrorActionPreference = 'Stop'

$base       = 'C:\Users\jeanp\Documents\Switch-PrepApp'
$scriptPath = "$base\scripts\inferhub-failover.mjs"
$log        = "$base\data\logs\inferhub-failover.log"
$port       = 18100
$maxLog     = 5MB

function Test-RouterUp {
    # [HEALTHZ 20260919] On teste /healthz, endpoint LOCAL du routeur, et non
    # /v1/models qui est PROXIFIE vers l'amont. Avec /v1/models, une panne
    # d'Inferhub faisait repondre 502 alors que la passerelle tournait
    # parfaitement : le lanceur croyait la passerelle morte, lancait un
    # doublon qui mourait sur EADDRINUSE, et renvoyait 'ECHEC' a tort
    # (constate le 19/09 : routeur vivant sur 18400, amont mort, /v1/models
    # = 502, /healthz = 200). La garde d'idempotence doit dependre de la
    # passerelle, jamais de la disponibilite de l'amont.
    try {
        Invoke-WebRequest -Uri "http://127.0.0.1:$port/healthz" -TimeoutSec 4 -UseBasicParsing -ErrorAction Stop | Out-Null
        return $true
    } catch {
        return $false
    }
}

if (Test-RouterUp) {
    Write-Host 'inferhub-failover deja vivante'
    exit 0
}

New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null

if (Test-Path $log) {
    if ((Get-Item $log).Length -gt $maxLog) {
        $bak = "$log.bak-rotation-" + (Get-Date -Format 'yyyyMMdd-HHmmss')
        Move-Item $log $bak -Force
        Get-ChildItem "$log.bak-rotation-*" -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending |
            Select-Object -Skip 5 |
            Remove-Item -Force -ErrorAction SilentlyContinue
    }
}

"--- demarrage $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ---" | Out-File $log -Append -Encoding utf8

$nodeExe = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $nodeExe) { $nodeExe = 'node' }

Start-Process -WindowStyle Hidden cmd -ArgumentList "/c `"`"$nodeExe`" `"$scriptPath`" 2>> `"$log`"`""

for ($i = 1; $i -le 30; $i++) {
    Start-Sleep -Seconds 1
    if (Test-RouterUp) {
        Write-Host "inferhub-failover demarree en $i s"
        exit 0
    }
}

Write-Host 'inferhub-failover-start-ECHEC'
exit 1
