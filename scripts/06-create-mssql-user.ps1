using namespace System.Data.SqlClient

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$SqlDBAPsw = ConvertTo-SecureString $ConConfig.MSSql.DBA.Password -AsPlainText -Force
$SqlDBAPsw.MakeReadOnly()
$SqlDBACredential = New-Object SqlCredential($ConConfig.MSSql.DBA.UserName, $SqlDBAPsw)

$SqlCon = New-Object SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlDBACredential)
$SqlCon.Open()

$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

$SqlCmd.CommandText = (
    "create schema " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "create login " + $ConConfig.MSSql.User.UserName +
    " with password = '" + $ConConfig.MSSql.User.Password + "'"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "create user " + $ConConfig.MSSql.User.UserName +
    " for login " + $ConConfig.MSSql.User.UserName +
    " with default_schema = " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

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

$SqlCmd.Dispose()
$SqlCon.Dispose()
