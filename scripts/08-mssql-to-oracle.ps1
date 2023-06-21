<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    08-mssql-to-oracle.ps1

    Example of copying data from a table in MSSQL to Oracle

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace Oracle.ManagedDataAccess.Client

# Manually adding the Oracle library
Add-Type -Path lib/Oracle.ManagedDataAccess.dll

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as POWERHAUL in Oracle
$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

# Setup a credential as POWERHAUL in MSSQL
$SqlUserPsw = ConvertTo-SecureString $ConConfig.MSSql.User.Password -AsPlainText -Force
$SqlUserPsw.MakeReadOnly()
$SqlUserCredential = New-Object SqlCredential($ConConfig.MSSql.User.UserName, $SqlUserPsw)

# Connect to Oracle
$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

# Create a Command object to be used for executing DML statement in Oracle
$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

# Execute CREATE TABLE for the target table in Oracle
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

# Connect to MSSQL
$SqlCon = New-Object System.Data.SqlClient.SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlUserCredential)
$SqlCon.Open()

# Create a Command object to be used for querying the source MSSQL
$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

# Create a DataReader for querying the source MSSQL table
# Note the two places using:  cast(... as numeric(32,16))
# In the original table in Oracle that was created from Access (see file 05-oledb-to-oracle.ps1), these columns were NUMBER with no precision/scale
# When that was copied to MSSQL (see file 07-oracle-to-mssql.ps1), they became numeric(38,32)
# Too high precision/scale in the MSSQL source DataReader will not be mapped properly to NUMBER, therefore we cast it to a smaller precision/scale in the query
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

# Just output for demo the column metadata of the DataReader
$SqlReader.GetSchemaTable().Rows

# Create an OracleBulkCopy object
$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

# Set parameters for the BulkCopy
$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "CAR_SALES_DATA_MS"

Write-Output "Begin BulkCopy"
Get-Date

# Execute the actual bulk copy of data
# This will perform bulk insert in BatchSize chunks with direct-path insert
# Could be any DataReader, not just MSSQL
$OraBulkCopy.WriteToServer($SqlReader)

Get-Date
Write-Output "End BulkCopy"

# Disposing of objects
$OraBulkCopy.Dispose()

$SqlReader.Dispose()
$SqlCmd.Dispose()
$OraCmd.Dispose()

$SqlCon.Dispose()
$OraCon.Dispose()

# --- End script ---