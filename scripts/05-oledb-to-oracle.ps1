<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhaul by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    05-oledb-to-oracle.ps1

    Example of copying data from all tables in an Access file to Oracle

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

# Build an OLE-DB connection to the Access file
$AccessCon = New-Object System.Data.OleDb.OleDbConnection("Provider=Microsoft.ACE.OLEDB.16.0;Data Source=data/powerhaul.accdb;")
$AccessCon.Open()

# An OLE-DB connection (and many others) has method GetSchema to retrieve metadata
# Calling it without any arguments returns info on which metadata collections can be queried:
#   $AccessCon.GetSchema()
# In our case, we want the collection called "Tables", which can be retrieved like this:
#   $AccessCon.GetSchema("Tables")
# But it returns not only our 2 user tables, but also Access system tables

# Therefore we need to create an array of strings for TableRestrictions (aka predicates)
# Which restrictions are possible depends on which collection and which OLE-DB driver
# For Access the "Tables" collection support 4 restrictions, the last of which is Table Type
# So we create an array with 4 elements and put "TABLE" in the last element
$TableRestrictions = New-Object -TypeName string[] -ArgumentList 4
$TableRestrictions[3] = 'TABLE'

# With this restriction, GetSchema("Tables") will return only our 2 user tables in the Access file
# Piping the output to ForEach-Object allows us to process the block of code in {} for each Access table
$AccessCon.GetSchema("Tables", $TableRestrictions) | ForEach-Object -Process {

    # Set a variable to contain the table name
    $OraTableName = $_.TABLE_NAME
    Write-Output $OraTableName
    
    # Create a DataReader to read the current-in-the-loop Access table
    $AccessCmd = $AccessCon.CreateCommand()
    $AccessCmd.CommandText = "select * from [" + $OraTableName + "]"
    $AccessReader = $AccessCmd.ExecuteReader()

    # You can get the metadata of the columns of a DataReader by calling GetSchemaTable like this:
    #   $AccessReader.GetSchemaTable()

    # The start of building a CREATE TABLE statement that will contain the columns of the Access table
    # Double quoting keeps table uppercase/lowercase like Access
    $OraCreate = "create table """ + $OraTableName + """ (`r`n"

    # Loop over the columns of the Access table
    foreach ($AccessCol in $AccessReader.GetSchemaTable().Rows) {

        # First column will have ordinal zero, so other columns need a comma separating it from previous column
        if ($AccessCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }
        
        # Decide what Oracle datatypes should be used for each Access datatype
        # Here just a subset of datatypes are shown - those needed for the sample Access file
        switch ($AccessCol.DataType.Name) {
            "Int32"     { $OraDataType = "integer"; break; }
            "Double"    { $OraDataType = "number"; break; }
            # Access "short text" and "long text" both map to OLE-DB "String"
            # Therefore we decide on using CLOB or VARCHAR2 based on column length
            "String"    { $OraDataType = ( $AccessCol.ColumnSize -gt 4000 ? "clob" : "varchar2("+$AccessCol.ColumnSize+")"); break; }
            "DateTime"  { $OraDataType = "date"; break; }
            default     { $OraDataType = "## NOT SUPPORTED ##" }
        }

        # Add the column to the CREATE TABLE statement
        # Double quoting keeps column uppercase/lowercase like Access
        $OraCreate += $Prefix + """" + $AccessCol.ColumnName + """ " + $OraDataType + "`r`n"

    }

    # Close the CREATE TABLE statement (could add tablespace and such here)
    $OraCreate += ")"

    # Just output the CREATE TABLE statement that's been built to be able to check it
    Write-Output $OraCreate

    # Create a Command object to be used for executing the DML statement
    $OraCmd = $OraCon.CreateCommand()
    $OraCmd.CommandTimeout = 0

    # Create the target table using the generated CREATE TABLE statement
    $OraCmd.CommandText = $OraCreate
    $OraCmd.ExecuteNonQuery()

    # Create an OracleBulkCopy object
    $OraBulkCopy = New-Object OracleBulkCopy($OraCon)

    # Set parameters for the BulkCopy
    $OraBulkCopy.BatchSize = 10000 #rows
    $OraBulkCopy.BulkCopyTimeout = 600 #seconds
    $OraBulkCopy.DestinationTableName = """" + $OraTableName + """" # Note double-quoted table name

    Write-Output "Begin BulkCopy"
    Get-Date
    
    # Execute the actual bulk copy of data
    # This will perform bulk insert in BatchSize chunks with direct-path insert
    # Could be any OLE-DB DataReader, not just Access
    $OraBulkCopy.WriteToServer($AccessReader)

    Get-Date
    Write-Output "End BulkCopy"

    # Dispose of the objects created inside the loop
    # (If many tables, consider moving object creation and disposing out of
    #   loop, reusing the objects every iteration of the loop
    #   See file 07-oracle-to-mssql for example of doing it that way)
    $OraBulkCopy.Dispose()
    $AccessReader.Dispose()
    $AccessCmd.Dispose()
    $OraCmd.Dispose()
}

# Disposing of objects
$AccessCon.Dispose()
$OraCon.Dispose()

# --- End script ---