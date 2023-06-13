Import-Module SimplySql

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$PgDBACredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.DBA.UserName, (ConvertTo-SecureString $ConConfig.Postgres.DBA.Password -AsPlainText -Force))

Open-PostGreConnection -ConnectionName "PGDBA" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgDBACredential -TrustSSL

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("create schema " + $ConConfig.Postgres.User.UserName)

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("create user " + $ConConfig.Postgres.User.UserName + " with password '" + $ConConfig.Postgres.User.Password + "'")

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("grant connect on database " + $ConConfig.Postgres.Database + " to " + $ConConfig.Postgres.User.UserName)

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("grant usage on schema " + $ConConfig.Postgres.User.UserName + " to " + $ConConfig.Postgres.User.UserName)

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("grant all on schema " + $ConConfig.Postgres.User.UserName + " to " + $ConConfig.Postgres.User.UserName)

<#
Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ()

Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ()
#>

Close-SqlConnection -ConnectionName "PGDBA"
