Import-Module "sqlps" -DisableNameChecking

# Load SQL Server SMO Assembly
Add-Type -AssemblyName "Microsoft.SqlServer.Smo"


# Define variables
$serverName = "KalromDb03"  # Replace with your SQL Server instance
$databaseName = "IberiagroupsLiveBaseImageDB_4020"  # Replace with your database name
$tablesToScript = @("dbo.Table1","HR.Table2","Sales.Table3","DWH.TableN")  # Add the table names you want to script
$outputFolder = "D:\DataScripts"  # Folder to save the scripts


# Ensure the output folder exists
if (!(Test-Path -Path $outputFolder)) {
    New-Item -ItemType Directory -Path $outputFolder
}

# Create a server object
$server = New-Object Microsoft.SqlServer.Management.Smo.Server $serverName

# Check if the database exists
if ($server.Databases[$databaseName] -eq $null) {
    Write-Output "Database $databaseName does not exist on server $serverName."
    exit
}

# Get the database object
$database = $server.Databases[$databaseName]

# Initialize a counter for the prefix
$counter = 1

# Loop through each schema-qualified table name
foreach ($fullTableName in $tablesToScript) {
    # Split the schema and table name
    $schema, $tableName = $fullTableName -split '\.'

    # Check if the schema and table exist
    $table = $database.Tables | Where-Object { $_.Schema -eq $schema -and $_.Name -eq $tableName }

    if ($table -ne $null) {
        # Prefix for file naming
        $prefix = "{0:D3}" -f $counter
        
        # Create the Scripter object for schema
        $scripterSchema = New-Object Microsoft.SqlServer.Management.Smo.Scripter($server)
        $scripterSchema.Options.IncludeIfNotExists = $true
        $scripterSchema.Options.ToFileOnly = $false  # We will manage file output manually

        # -------- Script the Schema -------- #
        if ($scripterSchema.Options.ScriptSchema -eq $true) {
            $schemaFile = "$outputFolder\$prefix`_$schema.$tableName`_schema.sql"
            
            $scripterSchema.Options.ScriptSchema = $true  # Script only schema
            $scripterSchema.Options.ScriptData = $true   # No data in schema script

            # Script the schema
            $scriptResultSchema = $scripterSchema.EnumScript($table)

            # Write script results to file (only if schema is generated)
            if ($scriptResultSchema.Count -gt 0) {
                [System.IO.File]::WriteAllLines($schemaFile, $scriptResultSchema)
                Write-Output "Scripted schema of $fullTableName to $schemaFile"
            } else {
                Write-Output "No schema script generated for $fullTableName. File might be empty."
            }
        } else {
            Write-Output "Skipping schema script for $fullTableName as ScriptSchema is set to false."
        }

        # -------- Script the Data -------- #
        if ($scripterSchema.Options.ScriptData -eq $true) {
            $dataFile = "$outputFolder\$prefix`_$schema.$tableName`_data.sql"
            $hasIdentityColumn = $table.Columns | Where-Object { $_.Identity -eq $true }

            $insertStatements = @()

            # If table has an identity column, include IDENTITY_INSERT ON
            if ($hasIdentityColumn -ne $null) {
                $insertStatements += "SET IDENTITY_INSERT [$schema].[$tableName] ON;"
            }

            # Get data using a SQL query
            $query = "SELECT * FROM [$schema].[$tableName];"
            $command = $server.Databases[$databaseName].ExecuteWithResults($query)
            
            foreach ($row in $command.Tables[0].Rows) {
                $columns = $command.Tables[0].Columns | ForEach-Object { $_.ColumnName }
                $values = $columns | ForEach-Object {
                    $value = $row[$_].ToString()
                    # Escape single quotes and handle empty values
                    $value = $value -replace "'", "''"
                    if ($value -eq "") { "NULL" } else { "'$value'" }
                }
                $insertStatements += "INSERT INTO [$schema].[$tableName] (" + [System.String]::Join(", ", $columns) + ") VALUES (" + [System.String]::Join(", ", $values) + ");"
            }

            # If table has an identity column, include IDENTITY_INSERT OFF after the inserts
            if ($hasIdentityColumn -ne $null) {
                $insertStatements += "SET IDENTITY_INSERT [$schema].[$tableName] OFF;"
            }

            # Write insert statements to the file
            [System.IO.File]::WriteAllLines($dataFile, $insertStatements)
            Write-Output "Scripted data of $fullTableName to $dataFile"
        } else {
            Write-Output "Skipping data script for $fullTableName as ScriptData is set to false."
        }

        # Increment the counter for the next file
        $counter++
    } else {
        Write-Output "Table $fullTableName does not exist in the database $databaseName."
    }
}

