<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    07-oracle-to-mssql.ps1

    Example of copying data from all tables in an Oracle schema to MSSQL

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace System.Data.SqlClient

# Manually adding the Oracle library
Add-Type -Path lib/Oracle.ManagedDataAccess.dll

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Setup a credential as POWERHAUL in MSSQL
$SqlUserPsw = ConvertTo-SecureString $ConConfig.MSSql.User.Password -AsPlainText -Force
$SqlUserPsw.MakeReadOnly()
$SqlUserCredential = New-Object SqlCredential($ConConfig.MSSql.User.UserName, $SqlUserPsw)

# Setup a credential as POWERHAUL in Oracle
$OraUserPsw = ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force
$OraUserPsw.MakeReadOnly()
$OraUserCredential = New-Object Oracle.ManagedDataAccess.Client.OracleCredential($ConConfig.Oracle.User.UserName, $OraUserPsw)

# Connect to MSSQL
$SqlCon = New-Object SqlConnection(("Server=" + $ConConfig.MSSql.Server + ";" + "Initial Catalog=" + $ConConfig.MSSql.Catalog + ";"), $SqlUserCredential)
$SqlCon.Open()

# Create a Command object to be used for executing DML statements in MSSQL
$SqlCmd = $SqlCon.CreateCommand()
$SqlCmd.CommandTimeout = 300

# Create a SqlBulkCopy object
# Note the parameters:
#   KeepIdentity means identity columns keep their imported values instead of reassigned new values
#   KeepNulls means to keep imported null values instead of reassigning column defaults
#   TableLock means entire table will be locked during import to improve performance
$SqlBulkCopy = New-Object SqlBulkCopy($SqlCon,"KeepIdentity,KeepNulls,TableLock",$null)
$SqlBulkCopy.BatchSize = 10000
$SqlBulkCopy.EnableStreaming = $true    # If source DataReader supports streaming, data will stream from source via BulkCopy to target
$SqlBulkCopy.BulkCopyTimeout = 0

# Connect to Oracle
$OraCon = New-Object Oracle.ManagedDataAccess.Client.OracleConnection(("data source=" + $ConConfig.Oracle.DataSource), $OraUserCredential)
$OraCon.Open()

# Create a Command object to be used for querying the source Oracle
$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 300

# Particularly important settings if many LOBs are to be copied over high latency connection
# Using defaults would fetch each individual LOB in separate roundtrips
# -1 for InitialLOBFetchSize makes LOBs be prefetched as much as will fit in the FetchSize
$OraCmd.FetchSize = 67108864 # 64 MB
$OraCmd.InitialLOBFetchSize = -1
$OraCmd.InitialLONGFetchSize = -1

# We use GetSchema() of the connection to discover tables in source Oracle
# See comments in file 05-oledb-to-oracle.ps1 how TableRestrictions work
# In this case, an array of 2 strings is needed
$TableRestrictions = New-Object -TypeName string[] -ArgumentList 2
# First value in the array puts restriction (predicate) on Schema name in Oracle
$TableRestrictions[0] = $ConConfig.Oracle.User.UserName

# With this restriction, GetSchema("Tables") will return all tables in schema POWERHAUL
# Piping the output to ForEach-Object allows us to process the block of code in {} for each table
$OraCon.GetSchema("Tables", $TableRestrictions) | ForEach-Object -Process {

    # Set a variable to contain the table name
    $SqlTableName = $_.TABLE_NAME
    Write-Output $SqlTableName

    # Create a DataReader to read the current-in-the-loop Oracle table
    $OraSql = "select * from """ + $SqlTableName + """"
    $OraCmd.CommandText = $OraSql
    $OraReader = $OraCmd.ExecuteReader()

    # The start of building a CREATE TABLE statement that will contain the columns of the Oracle table
    $SqlCreate = "create table [" + $SqlTableName + "] (`r`n"

    # Loop over the columns of the Oracle table
    foreach ($OraCol in $OraReader.GetSchemaTable().Rows) {

        # First column will have ordinal zero, so other columns need a comma separating it from previous column
        if ($OraCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }

        # Retrieve the datatype
        $OraType = [Oracle.ManagedDataAccess.Client.OracleDbType]$OraCol.ProviderType
        Write-Output ($OraCol.ColumnName + " : " + $OraType)
        
        # Decide what MSSQL datatypes should be used for each Oracle datatype
        switch ($OraType) {
            # Oracle datatype NUMBER(x,0) will appear as Int** depending on precision
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int16			) { $SqlDataType = "smallint"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int32			) { $SqlDataType = "int"; break; }
            ([Oracle.ManagedDataAccess.Client.OracleDbType]::Int64			) { $SqlDataType = "bigint"; break; }
            # Non-integer NUMBER datatypes will appear as Decimal, Single or Double depending on precision/scale
            # Note that NUMBER without precision/scale will give too large scale for MSSQL - therefore we use scale or 32, whichever is smallest
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
        
        # Add the column to the CREATE TABLE statement
        $SqlCreate += $Prefix + "[" + $OraCol.ColumnName + "] " + $SqlDataType + "`r`n"
    }

    # Close the CREATE TABLE statement
    $SqlCreate += ")"

    # Just output the CREATE TABLE statement that's been built to be able to check it
    Write-Output $SqlCreate

    # Create the target table using the generated CREATE TABLE statement
    $SqlCmd.CommandText = $SqlCreate
    $SqlCmd.ExecuteNonQuery()
    
    # Set destination for the BulkCopy object
    $SqlBulkCopy.DestinationTableName = $SqlTableName
        
    Write-Output "Begin BulkCopy"
    Get-Date

    # Execute the actual bulk copying of data
    # This will stream data from the OraReader to the destination table
    $SqlBulkCopy.WriteToServer($OraReader)
    
    Get-Date
    Write-Output "End BulkCopy"
 
    # Close the reader of the source table
    $OraReader.close()
}

# Disposing of objects
$OraCmd.Dispose()
$OraCon.Dispose()

$SqlBulkCopy.Dispose()
$SqlCmd.Dispose()
$SqlCon.Dispose()

# --- End script ---