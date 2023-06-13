using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()


$ExcelCon = New-Object System.Data.Odbc.OdbcConnection("Driver={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=data/emp.xlsx;")
$ExcelCon.Open()
$ExcelCmd = $ExcelCon.CreateCommand()
$ExcelCmd.CommandText = "select * from [emp$]"
$ExcelReader = $ExcelCmd.ExecuteReader()

#$ExcelReader.GetSchemaTable()

$SqlCreate = "create table emp (`r`n"

foreach ($ExcelCol in $ExcelReader.GetSchemaTable().Rows) {
    if ($ExcelCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }
    
    $ExcelCol.DataType.Name

    switch ($ExcelCol.DataType.Name) {
        "Double"    { $SqlDataType = "number"; break; }
        "String"    { $SqlDataType = "varchar2("+$ExcelCol.ColumnSize+")"; break; }
        "DateTime"  { $SqlDataType = "date"; break; }
        default     { $SqlDataType = "## NOT SUPPORTED ##" }
    }

    $SqlCreate += $Prefix + $ExcelCol.ColumnName + " " + $SqlDataType + "`r`n"

}

$SqlCreate += ")"

#$SqlCreate

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 0

$OraCmd.CommandText = $SqlCreate
$OraCmd.ExecuteNonQuery()


$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "EMP"

$OraBulkCopy.WriteToServer($ExcelReader)

$OraBulkCopy.Dispose()


$ExcelReader.Dispose()
$ExcelCmd.Dispose()
$ExcelCon.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()
