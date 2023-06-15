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
    "drop user " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop login " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop table " + $ConConfig.MSSql.User.UserName + ".[Canterbury_corpus]"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop table " + $ConConfig.MSSql.User.UserName + ".[Car_sales_data]"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop table " + $ConConfig.MSSql.User.UserName + ".[DEPT]"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop table " + $ConConfig.MSSql.User.UserName + ".[DULUX]"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop table " + $ConConfig.MSSql.User.UserName + ".[EMP]"
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.CommandText = (
    "drop schema " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

$SqlCmd.Dispose()
$SqlCon.Dispose()
