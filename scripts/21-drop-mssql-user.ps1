<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    21-drop-mssql-user.ps1

    Dropping the POWERHAUL schema in the Microsoft SQL Server database

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

# Execute drop user
$SqlCmd.CommandText = (
    "drop user " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Execute drop login
$SqlCmd.CommandText = (
    "drop login " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Execute drop tables
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

# Execute drop schema
$SqlCmd.CommandText = (
    "drop schema " + $ConConfig.MSSql.User.UserName
)
$SqlCmd.ExecuteNonQuery()

# Disposing of objects
$SqlCmd.Dispose()
$SqlCon.Dispose()

# --- End script ---