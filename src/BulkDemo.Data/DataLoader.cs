using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Fasterflect;

namespace BulkDemo.Data
{
    public class DataLoader
    {
        public DataLoader()
        {
            BatchSize = 10000;
        }

        public int BatchSize { get; set; }

        #region DBUtils

        public void TruncateTable(SqlConnection connection, string tableName)
        {
            using (var cmd = new SqlCommand(string.Format("truncate table {0};", tableName), connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        #endregion

        #region SqlDataAdapter

        public void BulkInsertWithSqlDataAdapter(SqlConnection connection, DataTable table)
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

        #region SqlBulkCopy

        public void BulkInsertWithSqlBulkCopy(SqlConnection connection, string tableName, DataTable table)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.WriteToServer(table);
                bulkCopy.Close();
            }
        }

        #endregion

        #region SqlBulkCopyTableLock

        public void BulkInsertWithSqlBulkCopyAndTableLock(SqlConnection connection, string tableName, DataTable table)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.WriteToServer(table);
                bulkCopy.Close();
            }
        }

        #endregion

        #region BulkInsert POCO

        public void BulkInsert<T>(SqlConnection connection, string tableName, IList<T> list)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTableFasterflect(list);
                bulkCopy.WriteToServer(table);
            }
        }

        public void BulkInsert<T>(string connection, string tableName, IList<T> list)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTableFasterflect(list);
                bulkCopy.WriteToServer(table);
            }
        }

        #endregion

        #region BulkInsert POCO TableLock

        public static void BulkInsertWithTableLock<T>(SqlConnection connection, string tableName, IList<T> list)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.BatchSize = list.Count;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTableFasterflect(list);
                bulkCopy.WriteToServer(table);
            }
        }

        public static void BulkInsertWithTableLock<T>(string connection, string tableName, IList<T> list)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock))
            {
                bulkCopy.BatchSize = list.Count;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTableFasterflect(list);
                bulkCopy.WriteToServer(table);
            }
        }

        #endregion

        #region List<POCO> to DataTable

        public static DataTable ListToDataTable<T>(IList<T> list)
        {
            var table = new DataTable();
            var props = TypeDescriptor.GetProperties(typeof(T))
                //Dirty hack to make sure we only have system data types
                //i.e. filter out the relationships/collections
              .Cast<PropertyDescriptor>()
              .Where(propertyInfo => propertyInfo.PropertyType.Namespace.Equals("System"))
              .ToArray();

            foreach (var propertyInfo in props)
            {
                table.Columns.Add(propertyInfo.Name,
                                  Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType);
            }

            var values = new object[props.Length];
            foreach (var item in list)
            {
                for (var i = 0; i < values.Length; i++)
                    values[i] = props[i].GetValue(item);

                table.Rows.Add(values);
            }
            return table;
        }

        public static DataTable ListToDataTableFasterflect<T>(IList<T> list)
        {
            var table = new DataTable();
            var props = TypeDescriptor.GetProperties(typeof(T))
                //Dirty hack to make sure we only have system data types
                //i.e. filter out the relationships/collections
              .Cast<PropertyDescriptor>()
              .Where(propertyInfo => propertyInfo.PropertyType.Namespace.Equals("System"))
              .ToArray();

            foreach (var propertyInfo in props)
            {
                table.Columns.Add(propertyInfo.Name,
                                  Nullable.GetUnderlyingType(propertyInfo.PropertyType) ?? propertyInfo.PropertyType);
            }

            var values = new object[props.Length];
            foreach (var item in list)
            {
                for (var i = 0; i < values.Length; i++)
                    values[i] = item.GetPropertyValue(props[i].Name);

                table.Rows.Add(values);
            }
            return table;
        }

        #endregion
    }
}
