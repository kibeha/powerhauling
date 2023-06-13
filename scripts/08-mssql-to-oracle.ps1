using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()


$SqlCon = New-Object System.Data.SqlClient.SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlUserCredential)
$SqlCon.Open()

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

$OraCmd.CommandText = ("create table car_sales_data_ms (
   id          integer
 , salesdate   date
 , salesperson varchar2(255)
 , cust_name   varchar2(255)
 , car_make    varchar2(255)
 , car_model   varchar2(255)
 , car_year    integer
 , sale_price  integer
 , comm_rate   number
 , comm_earned number
 )")
$OraCmd.ExecuteNonQuery()

$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

$SqlCmd.CommandText = "select
   [ID]   
 , [Date]
 , [Salesperson]
 , [Customer Name]
 , [Car Make]
 , [Car Model]
 , [Car Year]
 , [Sale Price]
 , cast([Commission Rate]   as numeric(32,16)) [Commission Rate]
 , cast([Commission Earned] as numeric(32,16)) [Commission Earned]
from [Car_sales_data]"
$SqlReader = $SqlCmd.ExecuteReader()

$SqlReader.GetSchemaTable().Rows

$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "CAR_SALES_DATA_MS"

Write-Output "Begin BulkCopy"
Get-Date
$OraBulkCopy.WriteToServer($SqlReader)
Get-Date
Write-Output "End BulkCopy"

$OraBulkCopy.Dispose()

$SqlReader.Dispose()
$SqlCmd.Dispose()
$OraCmd.Dispose()

$SqlCon.Dispose()
$OraCon.Dispose()
