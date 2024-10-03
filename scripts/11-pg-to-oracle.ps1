<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    11-pg-to-oracle.ps1

    Example of copying a table in PG to Oracle

    This is tested in Powershell 7 using SimplySql 2.0.4.75

    NOTES!

    - If you've run any of the scripts that do Add-Type of the Oracle ddl, this script will raise an error
      The solution is to run this script in another Powershell window
#>

# Import the SimplySql module
Import-Module SimplySql

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Do "nice" handling of errors with a "try" block (other scripts should do this too)
try
{
    # Setup a credential as POWERHAUL in Oracle
    $OraUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Oracle.User.UserName, (ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force))

    # Setup a credential as POWERHAUL in PG
    $PgUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.User.UserName, (ConvertTo-SecureString $ConConfig.Postgres.User.Password -AsPlainText -Force))

    # Connect to Oracle - name the connection "Target"
    Open-OracleConnection -ConnectionName "Target"  -DataSource $ConConfig.Oracle.Host -Port $ConConfig.Oracle.Port -ServiceName $ConConfig.Oracle.ServiceName -Credential $OraUserCredential

    # Connect to PG - name the connection "Source"
    Open-PostGreConnection -ConnectionName "Source" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgUserCredential

    # Create target table to copy the data into (could be existing already - for demo it's created here)
    $CreateQuery = "
    CREATE TABLE POWERHAUL.CAR_SALES_DATA_PG
    (
    SALESDATE DATE,
    SALESPERSON VARCHAR2(255),
    CUST_NAME VARCHAR2(255), 
    CAR_MAKE VARCHAR2(255), 
    CAR_MODEL VARCHAR2(255), 
    CAR_YEAR NUMBER(*,0)
    )
    "
    Invoke-SqlUpdate -ConnectionName "Target" -Query $CreateQuery

    Get-Date

    # Bulk copy the table from PG to Oracle
    Invoke-SqlBulkCopy -SourceConnectionName "Source" -DestinationConnectionName "Target" -SourceTable "ORA_CARSALES_MS" -DestinationTable "CAR_SALES_DATA_PG" -BatchSize 10000 -BatchTimeout 300 -Notify

    Get-Date

}
finally
{
    # Close and dispose both connections
    Close-SqlConnection -ConnectionName "Target"
    Close-SqlConnection -ConnectionName "Source"
}

# --- End script ---