using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkDemo.Test
{
    public class DataAdapterLoader
    {
        public DataAdapterLoader()
        {
            BatchSize = 10000;
        }

        public int BatchSize { get; set; }
        #region SqlDataAdapter

        public void BulkInsert(SqlConnection connection, DataTable table)
        {
            using (var insertCommand = new SqlCommand("INSERT BulkTable(ID, Name, EMail) VALUES (@ID, @Name, @EMail)", connection))
            {
                insertCommand.Parameters.Add("@ID", SqlDbType.Int, 4, "ID");
                insertCommand.Parameters.Add("@Name", SqlDbType.VarChar, 200, "Name");
                insertCommand.Parameters.Add("@EMail", SqlDbType.VarChar, 200, "EMail");

                insertCommand.UpdatedRowSource = UpdateRowSource.None;
                using (var insertAdapter = new SqlDataAdapter())
                {
                    insertAdapter.InsertCommand = insertCommand;
                    insertAdapter.UpdateBatchSize = BatchSize;
                    insertAdapter.Update(table);
                }
            }
        }

        #endregion

    }
}
