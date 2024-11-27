using MongoDB.Bson;
using System.Data.SqlClient;
using System.Text;

namespace MongoDbToSqlServerMigration
{
    public static class SqlServerHelper
    {
        public static bool DoesTableExist(SqlConnection sqlConnection, string tableName)
        {
            using (var command = sqlConnection.CreateCommand())
            {
                command.CommandText = $@"
                    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}')
                    SELECT 1 ELSE SELECT 0";
                return (int)command.ExecuteScalar() == 1;
            }
        }

        public static void CreateTable(SqlConnection sqlConnection, string tableName, BsonDocument sampleDocument)
        {
            using (var command = sqlConnection.CreateCommand())
            {
                var createTableCommand = new StringBuilder($"CREATE TABLE {tableName} (");

                foreach (var element in sampleDocument.Elements)
                {
                    createTableCommand.Append($"[{element.Name}] {MongoDbHelper.GetSqlType(element.Value)}, ");
                }

                createTableCommand.Length -= 2; // Remove the last comma and space
                createTableCommand.Append(");");

                command.CommandText = createTableCommand.ToString();
                command.ExecuteNonQuery();
            }
        }

        public static void AddMissingColumns(SqlConnection sqlConnection, string tableName, HashSet<string> columns)
        {
            using (var command = sqlConnection.CreateCommand())
            {
                var existingColumns = GetExistingColumns(sqlConnection, tableName);
                var addColumnCommand = new StringBuilder();
                var sqlTypes = new List<string>();

                foreach (var column in columns)
                {
                    if (!existingColumns.Contains(column))
                    {
                        var sampleValue = GetSampleValueForColumn(tableName, column); // You may need a method to get the sample value's type
                        var sqlType = MongoDbHelper.GetSqlType(sampleValue);
                        addColumnCommand.Append($"ALTER TABLE {tableName} ADD [{column}] {sqlType} NULL; ");
                        sqlTypes.Add($"{column}: {sqlType}");
                    }
                }

                if (addColumnCommand.Length > 0)
                {
                    command.CommandText = addColumnCommand.ToString();
                    command.ExecuteNonQuery();
                    Console.WriteLine($"Added new columns: {string.Join(", ", sqlTypes)}");
                }
                else
                {
                    Console.WriteLine("No new columns to add.");
                }
            }
        }

        private static BsonValue GetSampleValueForColumn(string tableName, string columnName)
        {
            // Implement logic to get a sample BsonValue for the column.
            // This might involve querying MongoDB to get an example document.
            // For simplicity, here we just return a default value:
            return new BsonString("sample");
        }

        //public static void AddMissingColumns(SqlConnection sqlConnection, string tableName, BsonDocument document)
        //{
        //    using (var command = sqlConnection.CreateCommand())
        //    {
        //        var existingColumns = GetExistingColumns(sqlConnection, tableName);
        //        var addColumnCommand = new StringBuilder();

        //        foreach (var element in document.Elements)
        //        {
        //            if (!existingColumns.Contains(element.Name))
        //            {
        //                var sqlType = MongoDbHelper.GetSqlType(element.Value);
        //                addColumnCommand.Append($"ALTER TABLE {tableName} ADD [{element.Name}] {sqlType}; ");
        //            }
        //        }

        //        if (addColumnCommand.Length > 0)
        //        {
        //            command.CommandText = addColumnCommand.ToString();
        //            command.ExecuteNonQuery();
        //        }
        //    }
        //}

        private static HashSet<string> GetExistingColumns(SqlConnection sqlConnection, string tableName)
        {
            var columns = new HashSet<string>();

            using (var command = sqlConnection.CreateCommand())
            {
                command.CommandText = $@"
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{tableName}'";

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(reader.GetString(0));
                    }
                }
            }

            return columns;
        }

        public static string GenerateInsertStatement(string tableName, BsonDocument document)
        {
            var columns = new StringBuilder();
            var values = new StringBuilder();

            foreach (var element in document.Elements)
            {
                columns.Append($"[{element.Name}], ");
                values.Append($"{MongoDbHelper.ConvertMongoTypeToSqlType(element.Value)}, ");
            }

            columns.Length -= 2; // Remove the last comma and space
            values.Length -= 2;  // Remove the last comma and space

            return $"INSERT INTO {tableName} ({columns}) VALUES ({values});";
        }
    }
}