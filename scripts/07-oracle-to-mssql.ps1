using namespace System.Data.SqlClient

Add-Type -Path lib/Oracle.ManagedDataAccess.dll

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

$SqlUserPsw = ConvertTo-SecureString $ConConfig.MSSql.User.Password -AsPlainText -Force
$SqlUserPsw.MakeReadOnly()
$SqlUserCredential = New-Object SqlCredential($ConConfig.MSSql.User.UserName, $SqlUserPsw)


$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object Oracle.ManagedDataAccess.Client.OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)


$SqlCon = New-Object SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlUserCredential)
$SqlCon.Open()

$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

$SqlBulkCopy = New-Object SqlBulkCopy($SqlCon,"KeepIdentity,KeepNulls,TableLock",$null)
$SqlBulkCopy.BatchSize = 10000
$SqlBulkCopy.EnableStreaming = $true
$SqlBulkCopy.BulkCopyTimeout = 0

$OraCon = New-Object Oracle.ManagedDataAccess.Client.OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

$TableRestrictions = New-Object -TypeName string[] -ArgumentList 2
$TableRestrictions[0] = $ConConfig.Oracle.User.UserName

$OraCon.GetSchema("Tables", $TableRestrictions) | ForEach-Object -Process {

    $SqlTableName = $_.TABLE_NAME
    Write-Output $SqlTableName

    $SqlCreate = "create table [" + $SqlTableName + "] (`r`n"

    $OraSql = "select * from """ + $SqlTableName + """"
    #Write-Output $OraSql

    $OraCmd.CommandText = $OraSql
    $OraReader = $OraCmd.ExecuteReader()

    foreach ($OraCol in $OraReader.GetSchemaTable().Rows) {
        $OraType = [Oracle.ManagedDataAccess.Client.OracleDbType]$OraCol.ProviderType

        Write-Output ($OraCol.ColumnName + " : " + $OraType)
        <# #>
        if ($OraCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }
        
        switch ($OraType) {
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int16			) { $SqlDataType = "smallint"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int32			) { $SqlDataType = "int"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int64			) { $SqlDataType = "bigint"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Decimal		) { $SqlDataType = "numeric("+$OraCol.NumericPrecision+","+([math]::Min($OraCol.NumericScale, 32))+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Single			) { $SqlDataType = "numeric("+$OraCol.NumericPrecision+","+([math]::Min($OraCol.NumericScale, 32))+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Double			) { $SqlDataType = "numeric("+$OraCol.NumericPrecision+","+([math]::Min($OraCol.NumericScale, 32))+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::BinaryFloat	) { $SqlDataType = "float"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::BinaryDouble	) { $SqlDataType = "float"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Date			) { $SqlDataType = "datetime"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStamp		) { $SqlDataType = "datetime2"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStampLTZ	) { $SqlDataType = "datetimeoffset"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStampTZ	) { $SqlDataType = "datetimeoffset"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Char			) { $SqlDataType = "char("+$OraCol.ColumnSize+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Varchar2		) { $SqlDataType = "varchar("+$OraCol.ColumnSize+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Clob			) { $SqlDataType = "varchar(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Long			) { $SqlDataType = "varchar(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::NChar			) { $SqlDataType = "nchar("+$OraCol.ColumnSize+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::NVarchar2		) { $SqlDataType = "nvarchar("+$OraCol.ColumnSize+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::NClob			) { $SqlDataType = "nvarchar(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Byte			) { $SqlDataType = "binary(1)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Raw			) { $SqlDataType = "varbinary("+$OraCol.ColumnSize+")"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Blob			) { $SqlDataType = "varbinary(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::LongRaw		) { $SqlDataType = "varbinary(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::XmlType		) { $SqlDataType = "xml"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Boolean		) { $SqlDataType = "bit"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Json			) { $SqlDataType = "varchar(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::BFile			) { $SqlDataType = "varbinary(max)"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Array			) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::IntervalDS		) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::IntervalYM		) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Ref			) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::RefCursor		) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Object			) { $SqlDataType = "## NOT SUPPORTED ##"; break; }
        }
        
        $SqlCreate += $Prefix + "[" + $OraCol.ColumnName + "] " + $SqlDataType + "`r`n"
        <# #>
    }
    <# #>
    $SqlCreate += ")"
    Write-Output $SqlCreate

    $SqlCmd.CommandText = $SqlCreate
    $SqlCmd.ExecuteNonQuery()
    
    # Set destination for the BulkCopy object

    $SqlBulkCopy.DestinationTableName = $SqlTableName
    
    # Execute the actual bulk copying of data
    # This will stream data from the OraReader to the destination table
    
    Write-Output "Begin BulkCopy"
    Get-Date
    $SqlBulkCopy.WriteToServer($OraReader)
    Get-Date
    Write-Output "End BulkCopy"
    <# #>
    # Close the reader of the source table
    
    $OraReader.close()
}

$OraCmd.Dispose()
$OraCon.Dispose()

$SqlBulkCopy.Dispose()
$SqlCmd.Dispose()
$SqlCon.Dispose()
