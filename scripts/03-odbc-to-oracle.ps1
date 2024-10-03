<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    03-odbc-to-oracle.ps1

    Example of copying data from Excel file to Oracle via ODBC

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

# Build an ODBC connection to the Excel file
$ExcelCon = New-Object System.Data.Odbc.OdbcConnection("Driver={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};DBQ=data/emp.xlsx;")
$ExcelCon.Open()

# Build a DataReader to query the "emp" sheet within the Excel file
$ExcelCmd = $ExcelCon.CreateCommand()
$ExcelCmd.CommandText = "select * from [emp$]"
$ExcelReader = $ExcelCmd.ExecuteReader()

# You can get the metadata of the columns of a DataReader by calling GetSchemaTable like this:
#   $ExcelReader.GetSchemaTable()

# The start of building a CREATE TABLE statement that will contain the columns of the Excel sheet
$SqlCreate = "create table emp (`r`n"

# Loop over the columns of the Excel sheet
foreach ($ExcelCol in $ExcelReader.GetSchemaTable().Rows) {
    
    # First column will have ordinal zero, so other columns need a comma separating it from previous column
    if ($ExcelCol.ColumnOrdinal -eq 0) { $Prefix = "   " } else { $Prefix = " , " }
    
    # Decide what Oracle datatypes should be used for each Excel datatype
    # Here just a subset of datatypes are shown - those needed for the sample Excel sheet
    switch ($ExcelCol.DataType.Name) {
        "Double"    { $SqlDataType = "number"; break; }
        "String"    { $SqlDataType = "varchar2("+$ExcelCol.ColumnSize+")"; break; }
        "DateTime"  { $SqlDataType = "date"; break; }
        default     { $SqlDataType = "## NOT SUPPORTED ##" }
    }

    # Add the column to the CREATE TABLE statement
    $SqlCreate += $Prefix + $ExcelCol.ColumnName + " " + $SqlDataType + "`r`n"

}

# Close the CREATE TABLE statement (could add tablespace and such here)
$SqlCreate += ")"

# Just output the CREATE TABLE statement that's been built to be able to check it
$SqlCreate

# Create a Command object to be used for executing the DML statement
$OraCmd = $OraCon.CreateCommand()
$OraCmd.CommandTimeout = 0

# Create the target table using the generated CREATE TABLE statement
$OraCmd.CommandText = $SqlCreate
$OraCmd.ExecuteNonQuery()

# Create an OracleBulkCopy object
$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

# Set parameters for the BulkCopy
$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "EMP"

# Execute the actual bulk copy of data
# This will perform bulk insert in BatchSize chunks with direct-path insert
# Could be any ODBC DataReader, not just Excel
$OraBulkCopy.WriteToServer($ExcelReader)

# Disposing of objects
$OraBulkCopy.Dispose()

$ExcelReader.Dispose()
$ExcelCmd.Dispose()
$ExcelCon.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()

# --- End script ---