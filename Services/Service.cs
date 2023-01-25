﻿using System.Data;
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
        try
        {
            SpectreConsoleHelper.WriteHeader("postgresql to mssql", Color.Blue);
            SpectreConsoleHelper.Log("LOG: Initializing...");
            AnsiConsole.Status()
                .Spinner(Spinner.Known.Aesthetic)
                .SpinnerStyle(Style.Parse("green"))
                .Start("Starting the migration...", ctx =>
                {
                    ctx.Status("Connecting sql server and postgresql...");
                    _validator.ValidateProviders();

                    using var postgresConnection = _provider.GetPostgresqlConnection();
                    using var sqlServerConnection = _provider.GetSqlServerConnection();

                    ctx.Status("Fetching postgresql schemas");
                    ctx.Spinner(Spinner.Known.BouncingBall);
                    var getSchemasQuery = "SELECT schema_name FROM information_schema.schemata";
                    var schemas = postgresConnection.Query<string>(getSchemasQuery).ToList();
                    SpectreConsoleHelper.Log("LOG: Fetched schemas from postgresql...");

                    schemas.Remove("information_schema");
                    schemas.Remove("pg_catalog");
                    schemas.Remove("pg_toast");

                    ctx.Status("Looping through available schemas...");
                    foreach (var sourceSchema in schemas)
                    {
                        string destinationSchema = $"{sourceSchema}_new";

                        ctx.Status($"Creating [{destinationSchema}] schema in sql server...");
                        var createDestinationSchemaQuery = $"CREATE SCHEMA [{destinationSchema}];";
                        sqlServerConnection.Execute(createDestinationSchemaQuery);
                        SpectreConsoleHelper.Log($"LOG: Created [{destinationSchema}] schema in sql server...");

                        ctx.Status($"Fetching available tables from [{sourceSchema}] schema...");
                        var getTablesQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
                        var tables = postgresConnection.Query<string>(getTablesQuery).ToList();
                        SpectreConsoleHelper.Log($"LOG: Fetched tables of [{sourceSchema}] schema from postgres");

                        ctx.Status($"Looping through all tables of [{sourceSchema}] schema...");
                        foreach (var table in tables)
                        {
                            ctx.Status($"Getting column definition for [{table}] table...");
                            var getColumnsQuery = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = '{sourceSchema}'";
                            var columns = postgresConnection.Query(getColumnsQuery);
                            SpectreConsoleHelper.Log($"LOG: Got column definition for [{table}] table...");

                            ctx.Status($"Creating table [{destinationSchema}].[{table}] in sql server...");
                            var createTableQuery = $"CREATE TABLE {destinationSchema}.{table} (";
                            createTableQuery += string.Join(", ", columns.Select(c => $"{c.column_name} {MapPostgresToSqlServerType(c.data_type)}"));
                            createTableQuery += ")";
                            sqlServerConnection.Execute(createTableQuery);
                            SpectreConsoleHelper.Log($"LOG: Created table [{destinationSchema}].[{table}] in sql server...");

                            ctx.Status($"Fetching data from [{sourceSchema}].[{table}] from postgresql...");
                            var data = postgresConnection.Query<dynamic>($"SELECT * FROM {sourceSchema}.{table}").ToList();
                            SpectreConsoleHelper.Log($"LOG: Fetched data from [{sourceSchema}].[{table}] table of postgresql...");

                            ctx.Status("Coverting the data into proper shape before migrating to sql server...");
                            var dataTable = ToDataTable(data);
                            SpectreConsoleHelper.Log("LOG: Converted data into proper shape...");

                            ctx.Status($"Transferring data from [blue][{sourceSchema}].[{table}][/] to [green][{destinationSchema}].[{table}][/]");
                            using var bulkCopy = new SqlBulkCopy(sqlServerConnection);
                            bulkCopy.DestinationTableName = $"[{destinationSchema}].[{table}]";
                            bulkCopy.WriteToServer(dataTable);
                            SpectreConsoleHelper.Log($"LOG: [green]Successfully transferred data from [{sourceSchema}].[{table}] to [{destinationSchema}].[{table}][/]");
                        }
                    }
                });
            SpectreConsoleHelper.WriteHeader("Success!", Color.Green);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);
        }
    }

    #region Private methods

    private static string MapPostgresToSqlServerType(string postgresType)
    {
        var typeMapping = new Dictionary<string, string>
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
        };

        return typeMapping.TryGetValue(postgresType.ToLower(), out string value) ? value : "nvarchar(max)";
    }

    private static DataTable ToDataTable(List<dynamic> items)
    {
        var dataTable = new DataTable("DynamicObject");

        foreach (dynamic item in items)
        {
            if (dataTable.Columns.Count == 0)
            {
                foreach (var property in item)
                {
                    dataTable.Columns.Add(property.Key);
                }
            }
            var values = new object[dataTable.Columns.Count];
            var i = 0;
            foreach (var property in item)
            {
                values[i++] = property.Value;
            }
            dataTable.Rows.Add(values);
        }

        return dataTable;
    }

    #endregion
}
