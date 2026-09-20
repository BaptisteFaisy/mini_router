' inferhub-failover : lance la passerelle sans fenetre au demarrage de session
CreateObject("Wscript.Shell").Run _
  "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\Users\jeanp\Documents\Switch-PrepApp\scripts\inferhub-failover-start.ps1""", _
  0, False
