Import-Module SimplySql

$ConConfig = Get-Content -Raw config/connections.json | ConvertFrom-Json
$TabConfig = Get-Content -Raw config/tables.json | ConvertFrom-Json

try
{
	# Open connections
    $OraUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Oracle.User.UserName, (ConvertTo-SecureString $ConConfig.Oracle.User.Password -AsPlainText -Force))

    $PgUserCredential = New-Object System.Management.Automation.PSCredential($ConConfig.Postgres.User.UserName, (ConvertTo-SecureString $ConConfig.Postgres.User.Password -AsPlainText -Force))

    Open-OracleConnection -ConnectionName "Source"  -DataSource $ConConfig.Oracle.Host -Port $ConConfig.Oracle.Port -ServiceName $ConConfig.Oracle.ServiceName -Credential $OraUserCredential

    Open-PostGreConnection -ConnectionName "Target" -Server $ConConfig.Postgres.Server -Database $ConConfig.Postgres.Database -Credential $PgUserCredential -TrustSSL

    # For each table in the config tablelist, copy it from the source to the target

    $TabConfig.TableList | ForEach-Object -process {
        if ($_.TableName) {
            Write-Output $_.TableName
            
            if ($_.DestinationTableName) {
                $DestinationTable = $_.DestinationTableName.ToLower();
            } else {
                $DestinationTable = $_.TableName.ToLower();
            }

            # Initialize SQL for creating target table and querying source
            $CreateQuery = "create table " + $DestinationTable;
            $SourceQuery = "select "
            $ColumnCounter = 0;

            # Retrieve metadata from source about the columns, datatypes and expressions to query

            $ColumnsQuery = "select
            column_name
          , '""' || lower(column_name) || '""' as column_alias
          , case
               when data_type = 'SDO_GEOMETRY'
                  then 'sdo_util.to_wkbgeometry(""' || column_name || '"")'
               when data_type = 'JSON'
                  then 'json_serialize(""' || column_name || '"")'
               when data_type = 'XMLTYPE'
                  then 'xmlserialize(content ""' || column_name || '"")'
               when data_type = 'NUMBER'
                  then 'cast(""' || column_name || '"" as number)'
               else
                  '""' || column_name || '""'
            end as column_expression
              , case
               when data_type = 'NUMBER'
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

            $TabCols = Invoke-SqlQuery -ConnectionName "Source" -Query $ColumnsQuery -Parameters @{owner = $ConConfig.Oracle.User.UserName; tabname = $_.TableName} -AsDataTable

            foreach ($Col in $TabCols) {
                # Add the column if config ColumnList is either empty or contains the column
                if ( (-Not $_.ColumnList) -Or ($_.ColumnList -contains $Col.COLUMN_NAME) ) {

                    # Build SQL for create and query
                    $ColumnCounter += 1;
                    
                    if ($ColumnCounter -eq 1) {
                        $CreateQuery += " (";
                        $SourceQuery += " "
                    } else {
                        $CreateQuery += " ,";
                        $SourceQuery += " ,"
                    }

                    #$CreateQuery += $Col.COLUMN_NAME.ToLower() + " " + $Col.DEST_DATA_TYPE;
                    $CreateQuery += $Col.COLUMN_ALIAS + " " + $Col.DEST_DATA_TYPE;
                    $SourceQuery += $Col.COLUMN_EXPRESSION + " as " + $Col.COLUMN_ALIAS;
                }
            }

            # End the SQL for create and query
            $CreateQuery += ")";
            $SourceQuery += " from " + $ConConfig.Oracle.User.UserName + ".""" + $_.TableName + """";
            if ($_.Predicate) { $SourceQuery += " where " + $_.Predicate }

            Write-Output $CreateQuery
            Write-Output $SourceQuery

            # Execute CREATE TABLE in the target database

			Invoke-SqlUpdate -ConnectionName "Target" -Query $CreateQuery | Out-Null

            # Execute the actual copying of data
            # (Progress per 1000 inserts - final output = number of rows copied)
			Get-Date
            Invoke-SqlBulkCopy -SourceConnectionName "Source" -DestinationConnectionName "Target" -SourceQuery $SourceQuery -DestinationTable $DestinationTable -BatchSize 1000 -BatchTimeout 300 -Notify
         Get-Date
		}
	}

#----


}
finally
{
	# Close connections
    Close-SqlConnection -ConnectionName "Target"
    Close-SqlConnection -ConnectionName "Source"
}