<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    02-csv-to-oracle.ps1

    Example of copying data from CSV file to Oracle utilizing CsvHelper class

    This is tested in Powershell 7 using .NET Core classes
#>

# To avoid entering namespace in front of all classes
using namespace Oracle.ManagedDataAccess.Client

# Manually adding libraries
Add-Type -Path lib/Oracle.ManagedDataAccess.dll
Add-Type -Path lib/CsvHelper.dll

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

# Create target table to copy the data into (could be existing already - for demo it's created here)
$OraCmd.CommandText = ("create table dept(   
    deptno     number(2,0),   
    dname      varchar2(14),   
    loc        varchar2(13),   
    constraint pk_dept primary key (deptno)   
 )")
$OraCmd.ExecuteNonQuery()

# Build a CsvDataReader object to read the CSV file
# As this CSV file is completely standard, no special config is needed, just using InvariantCulture defaults
$CsvFile = New-Object System.IO.StreamReader("data/dept.csv")
$CsvRead = New-Object CsvHelper.CsvReader($CsvFile, [CultureInfo]::InvariantCulture)
$CsvDataReader = New-Object CsvHelper.CsvDataReader($CsvRead)

# Create an OracleBulkCopy object
$OraBulkCopy = New-Object OracleBulkCopy($OraCon)

# Set parameters for the BulkCopy
$OraBulkCopy.BatchSize = 10000 #rows
$OraBulkCopy.BulkCopyTimeout = 600 #seconds
$OraBulkCopy.DestinationTableName = "DEPT"

# Execute the actual bulk copy of data
# This will perform bulk insert in BatchSize chunks with direct-path insert
# Works with any *DataReader object that implements System.Data.iDataReader interface
$OraBulkCopy.WriteToServer($CsvDataReader)

# Disposing of objects
$OraBulkCopy.Dispose()

$CsvDataReader.Dispose()
$CsvRead.Dispose()
$CsvFile.Dispose()

$OraCmd.Dispose()
$OraCon.Dispose()

# --- End script ---