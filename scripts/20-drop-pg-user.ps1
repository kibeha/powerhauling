Import-Module SimplySql

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$PgDBACredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.DBA.UserName, (ConvertTo-SecureString $ConConfig.Postgres.DBA.Password -AsPlainText -Force))

Open-PostGreConnection -ConnectionName "PGDBA" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgDBACredential -TrustSSL

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("drop user " + $ConConfig.Postgres.User.UserName)

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("drop schema " + $ConConfig.Postgres.User.UserName + " cascade")

Close-SqlConnection -ConnectionName "PGDBA"
