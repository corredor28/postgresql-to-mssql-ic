using System.Data;
using System.Data.SqlClient;
using Application.Helpers;
using Application.Providers.Interfaces;
using Application.Services.Interfaces;
using Application.Validators.Interfaces;
using Dapper;
using Spectre.Console;

namespace Application;

internal class Service : IService
{
    private readonly IProvider _provider;
    private readonly IValidator _validator;

    public Service(
        IProvider provider,
        IValidator validator
        )
    {
        _provider = provider;
        _validator = validator;
    }

    public void Migrate()
    {
        var errors = new List<string>();

        try
        {
            SpectreConsoleHelper.WriteHeader("postgresql to mssql", Color.Blue);

            _validator.ValidateProviders();

            SpectreConsoleHelper.Log("Initializing...");
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Arrow3)
                .SpinnerStyle(Style.Parse("green"))
                .Start("Starting the migration...", ctx =>
                {
                    using var postgresConnection = _provider.GetPostgresqlConnection();
                    using var sqlServerConnection = _provider.GetSqlServerConnection();

                    postgresConnection.Open();
                    sqlServerConnection.Open();

                    ctx.Status("Fetching postgresql schemas");
                    ctx.Spinner(Spinner.Known.BouncingBall);
                    var getSchemasQuery = "SELECT schema_name FROM information_schema.schemata";
                    var schemas = postgresConnection.Query<string>(getSchemasQuery).ToList();
                    SpectreConsoleHelper.Log("Fetched schemas from postgresql...");

                    RemoveUnnecessarySchemas(schemas);

                    ctx.Status("Looping through available schemas...");
                    
                    foreach (var sourceSchema in schemas)
                    {
                        string destinationSchema = $"{sourceSchema}_new";

                        ctx.Status($"Creating {destinationSchema} schema in sql server...");
                        var createDestinationSchemaQuery = $"CREATE SCHEMA [{destinationSchema}];";
                        sqlServerConnection.Execute(createDestinationSchemaQuery);
                        SpectreConsoleHelper.Log($"Created {destinationSchema} schema in sql server...");
                    }
                    
                    foreach (var sourceSchema in schemas)
                    {
                        string destinationSchema = $"{sourceSchema}_new";

                        ctx.Status($"Fetching available tables from {sourceSchema} schema...");
                        var getTablesQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
                        var tables = postgresConnection.Query<string>(getTablesQuery).ToList();
                        SpectreConsoleHelper.Log($"Fetched tables of {sourceSchema} schema from postgres");

                        ctx.Status($"Looping through all tables of {sourceSchema} schema...");
                        foreach (var table in tables)
                        {
                            ctx.Status($"Fetching column definition for {table} table...");
                            var getColumnsQuery = $@"SELECT c.column_name, c.data_type, c.is_nullable, c.is_identity, c.identity_start, c.identity_increment, ccu.constraint_name
                                                        FROM information_schema.columns c
                                                        LEFT JOIN information_schema.table_constraints tc
                                                            ON tc.constraint_schema = c.table_schema AND tc.table_name = c.table_name AND constraint_type = 'PRIMARY KEY'
                                                        LEFT JOIN information_schema.constraint_column_usage ccu
                                                            ON tc.constraint_schema = ccu.table_schema AND tc.table_name = ccu.table_name AND c.column_name = ccu.column_name AND tc.constraint_name = ccu.constraint_name
                                                        WHERE c.table_name = '{table}' AND c.table_schema = '{sourceSchema}'";
                            var columns = postgresConnection.Query(getColumnsQuery);
                            SpectreConsoleHelper.Log($"Fetched column definition for {table} table...");

                            ctx.Status($"Creating table {destinationSchema}.{table} in sql server...");
                            var createTableQuery = $"CREATE TABLE {destinationSchema}.{table} (";
                            createTableQuery += string.Join(", ", columns.Select(column => $@"
                                                                    [{column.column_name}] 
                                                                    {ConvertPostgreSqlToSqlServerDataType(column.data_type)} 
                                                                    {(column.is_nullable == "YES" ? "NULL" : "NOT NULL")} 
                                                                    {(column.constraint_name != null ? "PRIMARY KEY" : string.Empty)} 
                                                                    {(column.is_identity == "YES" ? $"IDENTITY({column.identity_start}, {column.identity_increment})" : string.Empty)} 
                                                                    "));
                            createTableQuery += ")";
                            sqlServerConnection.Execute(createTableQuery);
                            SpectreConsoleHelper.Log($"Created table {destinationSchema}.{table} in sql server...");
                        }

                        ctx.Status($"Adding foreign keys...");
                        foreach (var table in tables)
                        {
                            // Add foreign keys
                            ctx.Status($"Fetching foreign keys for {table} table...");
                            var getForeignKeysQuery = $@"SELECT
                                                            tc.constraint_name,
                                                            kcu.column_name,
                                                            ccu.table_name AS foreign_table_name,
                                                            ccu.column_name AS foreign_column_name
                                                        FROM
                                                            information_schema.table_constraints AS tc
                                                            JOIN information_schema.key_column_usage AS kcu
                                                            ON tc.constraint_name = kcu.constraint_name
                                                            JOIN information_schema.constraint_column_usage AS ccu
                                                            ON ccu.constraint_name = tc.constraint_name
                                                        WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name='{table}' AND tc.table_schema = '{sourceSchema}';";
                            var foreignKeys = postgresConnection.Query(getForeignKeysQuery);
                            foreach (var fk in foreignKeys)
                            {
                                var addForeignKeyQuery = $@"ALTER TABLE {destinationSchema}.{table}
                                                            ADD CONSTRAINT {fk.constraint_name}
                                                            FOREIGN KEY ([{fk.column_name}])
                                                            REFERENCES {destinationSchema}.{fk.foreign_table_name}([{fk.foreign_column_name}])";
                                sqlServerConnection.Execute(addForeignKeyQuery);
                                SpectreConsoleHelper.Log($"Added foreign key {fk.constraint_name} to {destinationSchema}.{table} in sql server...");
                            }
                        }

                        foreach (var table in tables)
                        {
                            // Insert data
                            IDataReader data;
                            try
                            {
                                ctx.Status($"Fetching data from {sourceSchema}.{table} from postgresql...");
                                data = postgresConnection.ExecuteReader($"SELECT * FROM {sourceSchema}.{table}");
                                SpectreConsoleHelper.Log($"Fetched data from {sourceSchema}.{table} table of postgresql...");

                                ctx.Status("Coverting the data into proper shape before migrating to sql server...");
                                var dataTable = new DataTable();
                                dataTable.Load(data);
                                SpectreConsoleHelper.Log("Converted data into proper shape...");

                                ctx.Status($"Transferring data from [blue]{sourceSchema}.{table}[/] to [green]{destinationSchema}.{table}[/]");
                                using var bulkCopy = new SqlBulkCopy(sqlServerConnection);
                                bulkCopy.DestinationTableName = $"{destinationSchema}.{table}";
                                bulkCopy.BulkCopyTimeout = 300;
                                bulkCopy.WriteToServer(dataTable);
                                SpectreConsoleHelper.Success($"Successfully transferred data from {sourceSchema}.{table} to {destinationSchema}.{table}");
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"{sourceSchema}~{table}");
                                AnsiConsole.WriteException(ex);
                            }
                        }
                    }
                });
            SpectreConsoleHelper.WriteHeader("Success!", Color.Green);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
        finally
        {
            if (errors.Any())
            {
                var table = new Table();
                table.Title("List of failed migration table/views");

                table.AddColumn("SourceSchema");
                table.AddColumn("SourceTable");

                foreach (var error in errors)
                {
                    var errorDetails = error.Split("~");
                    table.AddRow(errorDetails[0], errorDetails[1]);
                }

                table.Border(TableBorder.Rounded);
                AnsiConsole.Write(table);
            }
        }
    }

    #region Private methods

    private static void RemoveUnnecessarySchemas(List<string> schemas)
    {
        if (schemas.Contains("information_schema"))
        {
            schemas.Remove("information_schema");
        }
        if (schemas.Contains("pg_catalog"))
        {
            schemas.Remove("pg_catalog");
        }
        if (schemas.Contains("pg_toast"))
        {
            schemas.Remove("pg_toast");
        }
        if (schemas.Contains("pg_temp_1"))
        {
            schemas.Remove("pg_temp_1");
        }
        if (schemas.Contains("pg_toast_temp_1"))
        {
            schemas.Remove("pg_toast_temp_1");
        }
    }

    private static string ConvertPostgreSqlToSqlServerDataType(string postgresDataType)
    {
        var map = new Dictionary<string, string>
        {
            { "bigint", "bigint" },
            { "boolean", "bit" },
            { "character", "char" },
            { "character varying", "nvarchar(max)" },
            { "date", "date" },
            { "double precision", "float" },
            { "integer", "int" },
            { "interval", "time" },
            { "numeric", "decimal" },
            { "real", "real" },
            { "smallint", "smallint" },
            { "text", "nvarchar(max)" },
            { "time", "time" },
            { "timestamp", "datetime2" },
            { "timestamptz", "datetimeoffset" },
            { "uuid", "uniqueidentifier" },
            { "bytea", "varbinary(max)" },
            { "bit", "bit" },
            { "bit varying", "varbinary(max)" },
            { "money", "money" },
            { "json", "nvarchar(max)" },
            { "jsonb", "nvarchar(max)" },
            { "cidr", "nvarchar(max)" },
            { "inet", "nvarchar(max)" },
            { "macaddr", "nvarchar(max)" },
            { "tsvector", "nvarchar(max)" },
            { "tsquery", "nvarchar(max)" },
            { "array", "nvarchar(max)" },
            { "domain", "nvarchar(max)" },
            { "timestamp with time zone", "datetimeoffset" },
            { "timestamp without time zone", "datetime2" },
        };

        return map.TryGetValue(postgresDataType.ToLower(), out string? value) ? value.ToUpper() : "nvarchar(max)".ToUpper();
    }

    #endregion
}
