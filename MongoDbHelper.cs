using MongoDB.Bson;

namespace MongoDbToSqlServerMigration
{
    public static class MongoDbHelper
    {
        public static string ConvertMongoTypeToSqlType(BsonValue value)
        {
            if (value.IsObjectId)
            {
                return $"'{DataTypeConverter.ObjectIdToGuid(value.AsObjectId)}'";
            }
            if (value.IsString)
            {
                return $"N'{value.AsString.Replace("'", "''")}'";
            }
            if (value.IsInt32)
            {
                return value.AsInt32.ToString();
            }
            if (value.IsBoolean)
            {
                return value.AsBoolean ? "1" : "0";
            }
            if (value.IsDouble)
            {
                return value.AsDouble.ToString("F2");
            }
            if (value.IsBsonDateTime)
            {
                // Format datetime as 'yyyy-MM-ddTHH:mm:ss.fffffff'
                var sqlDateTime = DataTypeConverter.BsonDateTimeToSqlDateTime(value.AsBsonDateTime).ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                return $"'{sqlDateTime}'";
            }
            return "NULL";
        }



        public static string GetSqlType(BsonValue value)
        {
            if (value.IsObjectId) return "UNIQUEIDENTIFIER";
            if (value.IsString) return "NVARCHAR(MAX)";
            if (value.IsInt32) return "INT";
            if (value.IsBoolean) return "BIT";
            if (value.IsDouble) return "DECIMAL(18, 2)";
            if (value.IsBsonDateTime) return "DATETIME2(7)";
            return "NVARCHAR(MAX)";
        }
    }
}
