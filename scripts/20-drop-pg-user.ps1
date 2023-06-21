<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    20-drop-pg-user.ps1

    Dropping the POWERHAUL schema in the PostgreSQL database

    This currently requires Powershell 5 due to use of SimplySql
#>

# Import the SimplySql module
Import-Module SimplySql

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as postgres
$PgDBACredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.DBA.UserName, (ConvertTo-SecureString $ConConfig.Postgres.DBA.Password -AsPlainText -Force))

# Connect to PG
Open-PostGreConnection -ConnectionName "PGDBA" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgDBACredential -TrustSSL

# Drop user POWERHAUL
Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("drop user " + $ConConfig.Postgres.User.UserName)

# Drop schema POWERHAUL
Invoke-SqlUpdate -ConnectionName "PGDBA" -Query ("drop schema " + $ConConfig.Postgres.User.UserName + " cascade")

# Close and dispose
Close-SqlConnection -ConnectionName "PGDBA"

# --- End script ---