<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    22-drop-oracle-user.ps1

    Dropping the POWERHAUL schema in the Oracle database

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace Oracle.ManagedDataAccess.Client

# Manually adding the Oracle library
Add-Type -Path lib/Oracle.ManagedDataAccess.dll

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as SYS
$OraDBAPsw = ConvertTo-SecureString $ConConfig.Oracle.DBA.Password -AsPlainText -Force
$OraDBAPsw.MakeReadOnly()
$OraDBACredential = New-Object OracleCredential($ConConfig.Oracle.DBA.UserName, $OraDBAPsw, [OracleDBAPrivilege]::SYSDBA)

# Connect to Oracle
$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraDBACredential)
$OraCon.Open()

# Create a Command object to be used for executing the DML statements
$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

# Execute drop user cascade
$OraCmd.CommandText = (
    "drop user " + $ConConfig.Oracle.User.UserName + " cascade"
)
$OraCmd.ExecuteNonQuery()

# Disposing of objects
$OraCmd.Dispose()
$OraCon.Dispose()

# --- End script ---