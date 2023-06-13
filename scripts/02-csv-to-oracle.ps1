using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll
Add-Type -Path lib/CsvHelper.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

$OraCmd.CommandText = ("create table dept(   
    deptno     number(2,0),   
    dname      varchar2(14),   
    loc        varchar2(13),   
    constraint pk_dept primary key (deptno)   
 )")
$OraCmd.ExecuteNonQuery()

$CsvFile = New-Object System.IO.StreamReader("data/dept.csv")
$CsvRead = New-Object CsvHelper.CsvReader($CsvFile, [CultureInfo]::InvariantCulture)
$CsvDataReader = New-Object CsvHelper.CsvDataReader($CsvRead)

$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "DEPT"

$OraBulkCopy.WriteToServer($CsvDataReader)

$OraBulkCopy.Dispose()

$CsvDataReader.Dispose()
$CsvRead.Dispose()
$CsvFile.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()
