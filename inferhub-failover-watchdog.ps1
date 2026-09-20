# inferhub-failover-watchdog.ps1
# Surveille la passerelle inferhub-failover et la relance si elle meurt.
# Fichier autonome : le restart ne le regenere plus (plus de here-string dupliquee).
#
# [DETACH 20260919] La relance passe par inferhub-failover-launch.ps1, qui detache
# proprement (`cmd /c`) au lieu de laisser node heriter des handles du parent.
# L'ancienne version ecrasait aussi le log a chaque relance, ce qui effacait la cause
# de la mort : le lanceur est en append + rotation.
#
# Garde anti-doublon : si le port repond, la passerelle est vivante meme si la detection
# par ligne de commande a echoue. Evite le zombie qui meurt sur EADDRINUSE.

$ErrorActionPreference = 'SilentlyContinue'

$base     = 'C:\Users\jeanp\Documents\Switch-PrepApp'
$launcher = "$base\scripts\inferhub-failover-launch.ps1"
$port     = 18100
$wdLog    = "$base\data\logs\inferhub-watchdog.log"
$psExe    = 'C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe'

while ($true) {
    # [HEALTHZ 20260919] La verite est le PORT 18100 (endpoint LOCAL /healthz).
    # L'ancienne detection par ligne de commande etait doublement fausse :
    #  1. FAUX POSITIF : toute instance portant 'inferhub-failover.mjs' dans sa
    #     ligne de commande la faisait conclure "vivante", y compris une
    #     instance de TEST laissee par une autre session sur un autre port
    #     (constate le 19/09 : PID 41864 sur 18102, commande relative
    #     'node inferhub-failover.mjs'). Si la production mourait pendant ce
    #     temps, le watchdog ne la relancait PAS.
    #  2. FAUX NEGATIF : via /v1/models (proxifie vers l'amont), une panne
    #     d'Inferhub faisait croire a une passerelle morte alors qu'elle
    #     servait (routeur vivant sur 18400, amont mort : /v1/models = 502,
    #     /healthz = 200).
    $up = $false
    try {
        Invoke-WebRequest -Uri "http://127.0.0.1:$port/healthz" -TimeoutSec 4 -UseBasicParsing -ErrorAction Stop | Out-Null
        $up = $true
    } catch { }

    if ($up) { Start-Sleep -Seconds 20; continue }

    # Le port ne repond pas : on confirme par la table TCP (un processus peut
    # mettre du temps a liberer le socket pendant son arret).
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        Start-Sleep -Seconds 20
        continue
    }

    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') passerelle morte -> relance" |
        Out-File $wdLog -Append -Encoding utf8

    & $psExe -NoProfile -ExecutionPolicy Bypass -File $launcher
    Start-Sleep -Seconds 10
}
