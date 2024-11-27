using MongoDB.Bson;
using System;

namespace MongoDbToSqlServerMigration
{
    public static class DataTypeConverter
    {
        public static Guid ObjectIdToGuid(ObjectId objectId)
        {
            string hex = objectId.ToString();

            // Pad the hex string to 32 characters to make it valid for a GUID
            string paddedHex = hex.PadRight(32, '0');

            return Guid.ParseExact(paddedHex, "N");
        }

        public static DateTime BsonDateTimeToSqlDateTime(BsonDateTime bsonDateTime)
        {
            return bsonDateTime.ToUniversalTime();
        }

    }
}
