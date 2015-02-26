using System.Data.SqlClient;
using BulkDemo.Test.Properties;

namespace BulkDemo.Test
{
    public static class Db
    {
        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns></returns>
        public static SqlConnection GetConnection()
        {
            var connString = Settings.Default.BulkDemoConnection;
            var conn = new SqlConnection(connString);
            conn.Open();
            return conn;
        }
    }
}
