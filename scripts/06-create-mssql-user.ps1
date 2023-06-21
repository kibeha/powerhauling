<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    06-create-mssql-user.ps1

    Creating the POWERHAUL schema in the Microsoft SQL Server database

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace System.Data.SqlClient

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as SA
$SqlDBAPsw = ConvertTo-SecureString $ConConfig.MSSql.DBA.Password -AsPlainText -Force
$SqlDBAPsw.MakeReadOnly()
$SqlDBACredential = New-Object SqlCredential($ConConfig.MSSql.DBA.UserName, $SqlDBAPsw)

# Connect to MSSQL
$SqlCon = New-Object SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlDBACredential)
$SqlCon.Open()

# Create a Command object to be used for executing the DML statements
$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

# Execute CREATE SCHEMA
$SqlCmd.CommandText = (
    "create schema " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Execute CREATE LOGIN
$SqlCmd.CommandText = (
    "create login " + $ConConfig.MSSql.User.UserName +
    " with password = '" + $ConConfig.MSSql.User.Password + "'"
)
$SqlCmd.ExecuteNonQuery()

# Execute CREATE USER
$SqlCmd.CommandText = (
    "create user " + $ConConfig.MSSql.User.UserName +
    " for login " + $ConConfig.MSSql.User.UserName +
    " with default_schema = " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Execute granting of privileges
$SqlCmd.CommandText = (
    "grant connect to " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "grant create table to " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "grant control on schema::" + $ConConfig.MSSql.User.UserName + " to " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Disposing of objects
$SqlCmd.Dispose()
$SqlCon.Dispose()

# --- End script ---