﻿using System.Data;
using Dapper;

namespace Application;

internal class Service : IService
{
    private readonly IProvider _provider;

    public Service(IProvider provider)
    {
        _provider = provider;
    }

    public void Migrate()
    {
        using var postgresConnection = _provider.GetPostgresqlConnection();
        using var sqlServerConnection = _provider.GetSqlServerConnection();

        postgresConnection.Open();
        sqlServerConnection.Open();

        // get list of schemas
        var schemaListQuery = "SELECT schema_name FROM information_schema.schemata";
        var schemaList = postgresConnection.Query<string>(schemaListQuery).ToList();

        foreach (var sourceSchema in schemaList)
        {
            // modify unsupported schemas
            string destinationSchema = sourceSchema;
            if (sourceSchema == "public")
            {
                destinationSchema = "public_new";
            }

            // create schema
            var createDestinationSchemaQuery = $"CREATE SCHEMA [{destinationSchema}];";
            sqlServerConnection.Execute(createDestinationSchemaQuery);

            // get list of tables
            var tableListQuery = $"SELECT table_name FROM information_schema.tables WHERE table_schema = '{sourceSchema}'";
            var tableList = postgresConnection.Query<string>(tableListQuery).ToList();

            foreach (var tableName in tableList)
            {
                // get the table column's definition
                var columnListQuery = $"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tableName}' AND table_schema = '{sourceSchema}'";
                var columns = postgresConnection.Query(columnListQuery);

                // create the table in sql server
                var createTableQuery = $"CREATE TABLE {destinationSchema}.{tableName} (";
                createTableQuery += string.Join(", ", columns.Select(c => $"{c.column_name} {MapPostgresToSqlServerType(c.data_type)}"));
                createTableQuery += ")";
                sqlServerConnection.Execute(createTableQuery);

                // fetch data from postgres
                var data = postgresConnection.Query<object>($"SELECT * FROM {sourceSchema}.{tableName}").ToList();

                // insert into sql server
                foreach (var item in data)
                {
                    // TODO : insert logic
                    sqlServerConnection.Execute($"INSERT INTO {destinationSchema}.{tableName} VALUES(@col1, @col2, ...)", item);
                }
            }
        }
    }

    public void ValidateProviders()
    {
    sqlServerStart:
        using (var sqlServerConnection = _provider.GetSqlServerConnection())
        {
            if (!IsServerConnected(sqlServerConnection))
            {
                Console.WriteLine("Invalid SQL Server Connection!");

                Console.WriteLine("Provide the valid SQL Server Connection String...");
                var connectionString = Console.ReadLine();
                EnvironmentVariable.Set("SqlServerConnectionString", connectionString);

                Console.WriteLine("SqlServerConnectionString Set Successfully!");

                if (!IsServerConnected(sqlServerConnection)) goto sqlServerStart;
            }
        }

    postgreSqlStart:
        using (var postgreSqlConnection = _provider.GetPostgresqlConnection())
        {
            if (!IsServerConnected(postgreSqlConnection))
            {
                Console.WriteLine("Invalid PostgreSQL Connection!");

                Console.WriteLine("Provide the valid PostgreSQL Connection String...");
                var connectionString = Console.ReadLine();
                EnvironmentVariable.Set("PostgresqlConnectionString", connectionString);

                Console.WriteLine("PostgresqlConnectionString Set Successfully!");

                if (!IsServerConnected(postgreSqlConnection)) goto postgreSqlStart;
            }
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

    private static bool IsServerConnected(IDbConnection connection)
    {
        try
        {
            connection.Open();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    #endregion
}
