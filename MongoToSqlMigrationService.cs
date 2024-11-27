using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace MongoDbToSqlServerMigration
{
    public class MongoToSqlMigrationService
    {
        private readonly MongoClient _mongoClient;
        private readonly SqlConnection _sqlConnection;
        private readonly string _databaseName;

        public MongoToSqlMigrationService(string mongoConnectionString, string sqlConnectionString, string databaseName)
        {
            _mongoClient = new MongoClient(mongoConnectionString);
            _sqlConnection = new SqlConnection(sqlConnectionString);
            _databaseName = databaseName;
        }

        public void Migrate()
        {
            var database = _mongoClient.GetDatabase(_databaseName);
            var collections = database.ListCollectionNames().ToList();

            foreach (var collectionName in collections)
            {
                var collection = database.GetCollection<BsonDocument>(collectionName);
                var documents = collection.Find(new BsonDocument()).ToList();

                if (documents.Count == 0)
                {
                    Console.WriteLine($"Skipping empty collection: {collectionName}");
                    continue;
                }

                Console.WriteLine($"Migrating collection: {collectionName}");
                MigrateCollection(collectionName, documents);
            }
        }

        private void MigrateCollection(string collectionName, List<BsonDocument> documents)
        {
            using (var command = _sqlConnection.CreateCommand())
            {
                _sqlConnection.Open();

                // Check if the table exists, and create it if it doesn't
                if (!SqlServerHelper.DoesTableExist(_sqlConnection, collectionName))
                {
                    Console.WriteLine($"Creating table: {collectionName}");
                    SqlServerHelper.CreateTable(_sqlConnection, collectionName, documents.First());
                }

                // Identify all unique columns from the BSON documents
                var allColumns = new HashSet<string>();
                foreach (var document in documents)
                {
                    foreach (var element in document.Elements)
                    {
                        allColumns.Add(element.Name);
                    }
                }

                // Ensure the table schema is updated to match MongoDB documents
                Console.WriteLine($"Updating table schema: {collectionName}");
                SqlServerHelper.AddMissingColumns(_sqlConnection, collectionName, allColumns);

                // Now insert the data
                foreach (var document in documents)
                {
                    var sql = SqlServerHelper.GenerateInsertStatement(collectionName, document);
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }

                _sqlConnection.Close();
            }
        }

        //private void MigrateCollection(string collectionName, List<BsonDocument> documents)
        //{
        //    using (var command = _sqlConnection.CreateCommand())
        //    {
        //        _sqlConnection.Open();

        //        // Check if the table exists, and create it if it doesn't
        //        if (!SqlServerHelper.DoesTableExist(_sqlConnection, collectionName))
        //        {
        //            Console.WriteLine($"Creating table: {collectionName}");
        //            SqlServerHelper.CreateTable(_sqlConnection, collectionName, documents.First());
        //        }
        //        else
        //        {
        //            // Ensure the table schema is updated to match MongoDB documents
        //            Console.WriteLine($"Updating table schema: {collectionName}");
        //            SqlServerHelper.AddMissingColumns(_sqlConnection, collectionName, documents.First());
        //        }

        //        foreach (var document in documents)
        //        {
        //            var sql = SqlServerHelper.GenerateInsertStatement(collectionName, document);
        //            command.CommandText = sql;
        //            command.ExecuteNonQuery();
        //        }

        //        _sqlConnection.Close();
        //    }
        //}

    }
}
