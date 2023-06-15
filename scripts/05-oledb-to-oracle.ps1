using namespace Oracle.ManagedDataAccess.Client

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

$OraCon = New-Object OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

$AccessCon = New-Object System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.16.0;Data Source=data/powerhaul.accdb;")
$AccessCon.Open()

$TableRestrictions = New-Object -TypeName string[] -ArgumentList 4
$TableRestrictions[3] = 'TABLE'

#$ColumnRestrictions = New-Object -TypeName string[] -ArgumentList 4

$AccessCon.GetSchema("Tables", $TableRestrictions) | ForEach-Object -Process {

    $OraTableName = $_.TABLE_NAME
    Write-Output $OraTableName
    
    $AccessCmd = $AccessCon.CreateCommand()
    $AccessCmd.CommandText = "select * from [" + $OraTableName + "]"
    $AccessReader = $AccessCmd.ExecuteReader()

    #$AccessReader.GetSchemaTable()

    $OraCreate = "create table """ + $OraTableName + """ (`r`n"

    foreach ($AccessCol in $AccessReader.GetSchemaTable().Rows) {
        if ($AccessCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }
        
        Write-Output $AccessCol.DataType.Name

        switch ($AccessCol.DataType.Name) {
            "Int32"     { $OraDataType = "integer"; break; }
            "Double"    { $OraDataType = "number"; break; }
            "String"    { $OraDataType = ( $AccessCol.ColumnSize -gt 4000 ? "clob" : "varchar2("+$AccessCol.ColumnSize+")"); break; }
            "DateTime"  { $OraDataType = "date"; break; }
            default     { $OraDataType = "## NOT SUPPORTED ##" }
        }

        $OraCreate += $Prefix + """" + $AccessCol.ColumnName + """ " + $OraDataType + "`r`n"

    }

    $OraCreate += ")"

    Write-Output $OraCreate

    $OraCmd = $OraCon.CreateCommand()
    $OraCmd.CommandTimeout = 0

    $OraCmd.CommandText = $OraCreate
    $OraCmd.ExecuteNonQuery()


    $OraBulkCopy = New-Object OracleBulkCopy($OraCon)

    $OraBulkCopy.BatchSize = 10000 #rows
    $OraBulkCopy.BulkCopyTimeout = 600 #seconds
    $OraBulkCopy.DestinationTableName = """" + $OraTableName + """"

    Write-Output "Begin BulkCopy"
    Get-Date
    $OraBulkCopy.WriteToServer($AccessReader)
    Get-Date
    Write-Output "End BulkCopy"

    $OraBulkCopy.Dispose()

    $AccessReader.Dispose()
    $AccessCmd.Dispose()
    $OraCmd.Dispose()
}

$AccessCon.Dispose()
$OraCon.Dispose()
