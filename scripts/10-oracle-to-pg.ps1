<#
    https://github.com/kibeha/powerhauling
    Companion repository for presentation "Powerhauling Data with Powershell"
    https://bit.ly/powerhauling by Kim Berg Hansen, https://www.kibeha.dk/
    If you are inspired to use this code, the responsibility is your own

    10-oracle-to-pg.ps1

    Example of copying data from selected tables in an Oracle schema to PG

    This is tested in Powershell 7 using SimplySql 2.0.4.75

    NOTES!

    - If you've run any of the scripts that do Add-Type of the Oracle ddl, this script will raise an error
      The solution is to run this script in another Powershell window

    - There seems currently in SimplySql to be a bug (I think) in Invoke-SqlBulkCopy that causes it to fail for column/table names
      containing spaces, even if they are quoted. There's no problem in Invoke-SqlQuery or Invoke-SqlUpdate.
      Therefore Canterbury_corpus and Car_sales_data fail to BulkCopy.
#>

# Import the SimplySql module
Import-Module SimplySql

# Retrieve connection configuration
$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json

# Retrieve table/columns/rows configuration
$TabConfig = Get-Content -Raw config/tables.json | ConvertFrom-Json

# Do "nice" handling of errors with a "try" block (other scripts should do this too)
try
{
    # Setup a credential as POWERHAUL in Oracle
    $OraUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Oracle.User.UserName, (ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force))

    # Setup a credential as POWERHAUL in PG
    $PgUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.User.UserName, (ConvertTo-SecureString $ConConfig.Postgres.User.Password -AsPlainText -Force))

    # Connect to Oracle - name the connection "Source"
    Open-OracleConnection -ConnectionName "Source"  -DataSource $ConConfig.Oracle.Host -Port $ConConfig.Oracle.Port -ServiceName $ConConfig.Oracle.ServiceName -Credential $OraUserCredential

    # Connect to PG - name the connection "Target"
    Open-PostGreConnection -ConnectionName "Target" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgUserCredential

    # For each table in the config tablelist, process the block
    $TabConfig.TableList | ForEach-Object -process {
        if ($_.TableName) {
            Write-Output $_.TableName
            
            # If config specifies destination table, use it, otherwise default to source table name
            if ($_.DestinationTableName) {
                $DestinationTable = $_.DestinationTableName.ToLower();
            } else {
                $DestinationTable = $_.TableName.ToLower();
            }

            # Initialize SQL strings for creating target table and querying source
            $CreateQuery = "create table " + $DestinationTable;
            $SourceQuery = "select "
            $ColumnCounter = 0;

            # Retrieve metadata from source Oracle about the columns, datatypes and expressions to query

            $ColumnsQuery = "select
            column_name
          , '""' || lower(column_name) || '""' as column_alias
          , case
               when data_type = 'SDO_GEOMETRY'
                  /* querying well-known-binary format can be inserted in PG */
                  then 'sdo_util.to_wkbgeometry(""' || column_name || '"")'
               when data_type = 'JSON'
                  /* serialize to text */
                  then 'json_serialize(""' || column_name || '"")'
               when data_type = 'XMLTYPE'
                  /* serialize to text */
                  then 'xmlserialize(content ""' || column_name || '"")'
               when data_type = 'NUMBER'
                  /* in query, remove precision/scale to allow mapping to PG */
                  then 'cast(""' || column_name || '"" as number)'
               else
                  /* all else just query the column itself */
                  '""' || column_name || '""'
            end as column_expression
              , case
               when data_type = 'NUMBER'
                  /* build suitable PG numeric datatype for column creation */
                  then 'numeric' || case when data_precision is not null then '(' || data_precision || case when data_scale is not null then ',' || data_scale end || ')' end
               when data_type = 'FLOAT'
                  then 'numeric'
               when data_type = 'BINARY_FLOAT'
                  then 'real'
               when data_type = 'BINARY_DOUBLE'
                  then 'double precision'
               when data_type = 'DATE'
                  then 'timestamp'
               when data_type like 'TIMESTAMP%'
                  then 'timestamp' || case when data_type like '%WITH TIME ZONE' then ' with time zone' end
               when data_type in ('CHAR','NCHAR')
                  then 'char(' || char_length || ')'
               when data_type in ('VARCHAR2','NVARCHAR2')
                  then 'char(' || char_length || ')'
               when data_type in ('CLOB','NCLOB')
                  then 'text'
               when data_type in ('RAW','BLOB')
                  then 'bytea'
               when data_type = 'XMLTYPE'
                  then 'text' --'xml'
               when data_type = 'JSON'
                  then 'text'--'jsonb'
               when data_type = 'SDO_GEOMETRY'
                  then 'geometry'
               else
                  '**NOT**SUPPORTED**'
            end as dest_data_type
         from all_tab_columns c
         where c.owner = :owner
         and c.table_name = :tabname
         order by column_id"

            # Retrieve a DataTable from source Oracle with the results of the above metadata query
            # Notice use of owner and tabname bind variables
            $TabCols = Invoke-SqlQuery -ConnectionName "Source" -Query $ColumnsQuery -Parameters @{owner = $ConConfig.Oracle.User.UserName; tabname = $_.TableName} -AsDataTable

            # Loop over each column retrieved from the metadata
            foreach ($Col in $TabCols) {
                # Add the column if config ColumnList is either empty or contains the column
                if ( (-Not $_.ColumnList) -Or ($_.ColumnList -contains $Col.COLUMN_NAME) ) {

                    # Build SQL for create and query
                    $ColumnCounter += 1;
                    
                    # Handle first column separately
                    if ($ColumnCounter -eq 1) {
                        $CreateQuery += " (";
                        $SourceQuery += " "
                    } else {
                        $CreateQuery += " ,";
                        $SourceQuery += " ,"
                    }

                    # Add column in the CREATE TABLE for the PG target
                    $CreateQuery += $Col.COLUMN_ALIAS + " " + $Col.DEST_DATA_TYPE;
                    # Add column expression in the SELECT query for the Oracle source
                    $SourceQuery += $Col.COLUMN_EXPRESSION + " as " + $Col.COLUMN_ALIAS;
                }
            }

            # End the SQL for create and query
            $CreateQuery += ")";
            $SourceQuery += " from " + $ConConfig.Oracle.User.UserName + ".""" + $_.TableName + """";
            # If the config contains a predicate, add the WHERE clause to the query
            if ($_.Predicate) { $SourceQuery += " where " + $_.Predicate }

            # Output the SQL to check on it
            Write-Output $CreateQuery
            Write-Output $SourceQuery

            # Execute CREATE TABLE in the target PG database
			   Invoke-SqlUpdate -ConnectionName "Target" -Query $CreateQuery | Out-Null

			   Get-Date

            # Execute the actual copying of data
            # (Progress per 1000 inserts - final output = number of rows copied)
            Invoke-SqlBulkCopy -SourceConnectionName "Source" -DestinationConnectionName "Target" -SourceQuery $SourceQuery -DestinationTable $DestinationTable -BatchSize 1000 -BatchTimeout 300 -Notify
            # Because target in this case is PostgreSQL, SimplySql will perform regular inserts
            # If target had been Oracle or MSSQL, SimplySql will perform BulkCopy

            Get-Date
		}
	}
}
finally
{
    # Close and dispose both connections
    Close-SqlConnection -ConnectionName "Target"
    Close-SqlConnection -ConnectionName "Source"
}

# --- End script ---