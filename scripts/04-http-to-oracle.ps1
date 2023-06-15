using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

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


$HttpClient = New-Object System.Net.Http.HttpClient

$GetJson = $HttpClient.GetStringAsync("https://raw.githubusercontent.com/dariusk/corpora/master/data/colors/dulux.json")

$DuluxDataTable = [Newtonsoft.Json.JsonConvert]::DeserializeObject($GetJson.Result, [System.Data.DataTable])
#$DuluxDataTableReader = New-Object System.Data.DataTableReader($DuluxDataTable)

#$DuluxDataTableReader.GetSchemaTable()
$DuluxDataTableReader.FieldCount
$DuluxDataTable.Rows.Count

$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "DULUX"

$OraBulkCopy.ColumnMappings.Add("name", "name")
$OraBulkCopy.ColumnMappings.Add("code", "code")
$OraBulkCopy.ColumnMappings.Add("lrv", "light_reflectance")
$OraBulkCopy.ColumnMappings.Add("id", "id")
$OraBulkCopy.ColumnMappings.Add("lightText", "light_text")
$OraBulkCopy.ColumnMappings.Add("r", "red")
$OraBulkCopy.ColumnMappings.Add("g", "green")
$OraBulkCopy.ColumnMappings.Add("b", "blue")

$OraBulkCopy.WriteToServer($DuluxDataTable)

$OraBulkCopy.Dispose()

$DuluxDataTable.Dispose()
$GetJson.Dispose()
$HttpClient.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()
