using System;

namespace MongoDbToSqlServerMigration
{
    class Program
    {
        static void Main(string[] args)
        {
            string mongoConnectionString = "mongodb+srv://admingayan:admin_gayan_2024@escrap-dbcluster.rauxwor.mongodb.net/";
            string sqlConnectionString = "Integrated Security=SSPI;Persist Security Info=False;Initial Catalog=escrapConvert;Data Source=MICROSERVICE11;TrustServerCertificate=True";
            string databaseName = "escrap";

            var migrationService = new MongoToSqlMigrationService(mongoConnectionString, sqlConnectionString, databaseName);
            migrationService.Migrate();

            Console.WriteLine("Migration Completed Successfully!");
        }
    }
}
