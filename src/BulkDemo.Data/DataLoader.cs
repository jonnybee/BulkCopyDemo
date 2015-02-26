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



        #region SqlBulkInsert DataTable

        public void BulkInsert(SqlConnection connection, string tableName, DataTable table)
        {
            BulkInsert(connection, tableName, table, SqlBulkCopyOptions.Default);
        }


        public void BulkInsert(SqlConnection connection, string tableName, DataTable table, SqlBulkCopyOptions options)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, options, null))
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
            BulkInsert<T>(connection, tableName, list, SqlBulkCopyOptions.Default);
        }


        public void BulkInsert<T>(SqlConnection connection, string tableName, IList<T> list, SqlBulkCopyOptions options)
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
            BulkInsert<T>(connection, tableName, list, SqlBulkCopyOptions.Default);
        }

        public void BulkInsert<T>(string connection, string tableName, IList<T> list, SqlBulkCopyOptions options)
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
