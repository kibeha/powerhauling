using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraDBAPsw = ConvertTo-SecureString $ConConfig.Oracle.DBA.Password -AsPlainText -Force
$OraDBAPsw.MakeReadOnly()
$OraDBACredential = New-Object OracleCredential($ConConfig.Oracle.DBA.UserName, $OraDBAPsw, [OracleDBAPrivilege]::SYSDBA)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraDBACredential)
$OraCon.Open()

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

$OraCmd.CommandText = (
    "drop user " + $ConConfig.Oracle.User.UserName + " cascade"
)
$OraCmd.ExecuteNonQuery()

$OraCmd.Dispose()
$OraCon.Dispose()
