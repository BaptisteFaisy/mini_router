# inferhub-failover-restart.ps1
# Relance complete de la passerelle : arret, relance detachee, watchdog.
# Rend la main immediatement : aucun processus enfant n'herite des handles du parent.
#
# [DETACH 20260919] L'ancienne version terminait par
#     Start-Process -WindowStyle Hidden node -ArgumentList "...mjs" -RedirectStandardError $log
# node heritait alors le stdout du parent (le pipe du shell appelant) et, comme il tourne
# sans fin, l'appelant ne reprenait jamais la main : coupure au timeout, alors que l'ancien
# processus avait deja ete tue. La passerelle restait donc arretee. La relance passe
# maintenant par inferhub-failover-launch.ps1, qui detache proprement via `cmd /c`.
#
# Ordre volontaire : watchdog d'abord (sinon il relance pendant l'arret), puis passerelle.

$ErrorActionPreference = 'Stop'
$sw = [System.Diagnostics.Stopwatch]::StartNew()

$base      = 'C:\Users\jeanp\Documents\Switch-PrepApp'
$scripts   = "$base\scripts"
$launcher  = "$scripts\inferhub-failover-launch.ps1"
$watchFile = "$scripts\inferhub-failover-watchdog.ps1"
$port      = 18100
$psExe     = 'C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe'

function Get-RouterProcesses {
    @(Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*inferhub-failover.mjs*' })
}

function Get-WatchdogProcesses {
    @(Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -like '*inferhub-failover-watchdog*' })
}

function Test-PortBusy {
    [bool](Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
}

# 1) Arret
foreach ($p in (Get-WatchdogProcesses)) {
    Write-Host ("watchdog arrete PID " + $p.ProcessId)
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
foreach ($p in (Get-RouterProcesses)) {
    Write-Host ("passerelle arretee PID " + $p.ProcessId)
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}

# 2) Attente de la liberation du port (le bind survit quelques centaines de ms)
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep -Milliseconds 500
    if (-not (Test-PortBusy)) { break }
}
if (Test-PortBusy) { Write-Host 'port 18100 encore occupe - poursuite quand meme' }

# 3) Relance detachee via le lanceur commun (processus enfant : isolation totale)
$out = & $psExe -NoProfile -ExecutionPolicy Bypass -File $launcher 2>&1
Write-Host ("lanceur: " + (($out | Out-String).Trim()))

# 4) Watchdog (processus detache, console propre)
Start-Process -WindowStyle Hidden $psExe -ArgumentList @(
    '-NoProfile', '-ExecutionPolicy', 'Bypass', '-WindowStyle', 'Hidden', '-File', "`"$watchFile`""
)
Start-Sleep -Seconds 2

# 5) Etat final
$routerPid = (Get-RouterProcesses).ProcessId
$wdPid     = (Get-WatchdogProcesses).ProcessId
Write-Host ("passerelle PID: " + (($routerPid -join ',') -replace '^$', 'AUCUNE'))
Write-Host ("watchdog   PID: " + (($wdPid -join ',') -replace '^$', 'AUCUN'))

if (-not $routerPid) {
    Write-Host ("restart-ECHEC apres " + [int]$sw.Elapsed.TotalSeconds + " s")
    exit 1
}

Write-Host ("restart OK en " + [int]$sw.Elapsed.TotalSeconds + " s")
