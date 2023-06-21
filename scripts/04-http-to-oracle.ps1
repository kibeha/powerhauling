<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    04-http-to-oracle.ps1

    Example of copying data from JSON file retrieved from HTTP to Oracle

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace Oracle.ManagedDataAccess.Client

# Manually adding the Oracle library
Add-Type -Path lib/Oracle.ManagedDataAccess.dll

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as POWERHAUL
$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

# Connect to Oracle
$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

# Create a Command object to be used for executing the DML statement (not needed if table already exists)
$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

# Create target table deliberately not quite identical to the JSON file
# Column names and column order are slightly different
$OraCmd.CommandText = ("create table dulux (
    id                  integer
  , name                varchar2(100)
  , code                varchar2(100)
  , light_reflectance   number
  , light_text          number(1,0)
  , red                 integer
  , green               integer
  , blue                integer
)")
$OraCmd.ExecuteNonQuery()

# Retrieve the JSON file using HTTP
$HttpClient = New-Object System.Net.Http.HttpClient

$GetJson = $HttpClient.GetStringAsync("https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json")

# Deserialize the JSON string into a DataTable object
$DuluxDataTable = [Newtonsoft.Json.JsonConvert]::DeserializeObject($GetJson.Result, [System.Data.DataTable])

# A DataReader to read the DataTable could be created like this:
#   $DuluxDataTableReader = New-Object System.Data.DataTableReader($DuluxDataTable)
# But because OracleBulkCopy supports directly using a DataTable, we do not need the DataReader

# Output how many rows is in the DataTable
$DuluxDataTable.Rows.Count

# Create an OracleBulkCopy object
$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

# Set parameters for the BulkCopy
$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "DULUX"

# Because column order and names are slightly different
#   we map source columns to target columns by adding to the objects ColumnMapping collection
# Here we map using column names, it is also possible to map using column ordinals
$OraBulkCopy.ColumnMappings.Add("name", "name")
$OraBulkCopy.ColumnMappings.Add("code", "code")
$OraBulkCopy.ColumnMappings.Add("lrv", "light_reflectance")
$OraBulkCopy.ColumnMappings.Add("id", "id")
$OraBulkCopy.ColumnMappings.Add("lightText", "light_text")
$OraBulkCopy.ColumnMappings.Add("r", "red")
$OraBulkCopy.ColumnMappings.Add("g", "green")
$OraBulkCopy.ColumnMappings.Add("b", "blue")

# Execute the actual bulk copy of data
# This will perform bulk insert in BatchSize chunks with direct-path insert
# Works with any DataTable
$OraBulkCopy.WriteToServer($DuluxDataTable)

# Disposing of objects
$OraBulkCopy.Dispose()

$DuluxDataTable.Dispose()
$GetJson.Dispose()
$HttpClient.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()

# --- End script ---