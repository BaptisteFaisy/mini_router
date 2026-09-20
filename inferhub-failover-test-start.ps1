$env:FAILOVER_PORT = '18101'
$env:FAILOVER_TEST_ANY = '1'
Start-Process -WindowStyle Hidden node -ArgumentList 'C:\Users\jeanp\Documents\Switch-PrepApp\scripts\inferhub-failover.mjs' -RedirectStandardError 'C:\Users\jeanp\Documents\Switch-PrepApp\data\logs\inferhub-failover-test.log'
