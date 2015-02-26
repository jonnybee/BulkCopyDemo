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
    /// <summary>
    /// This is the wrapper class for performing efficient BulkInserts
    /// </summary>
    public class DataLoader
    {
        public DataLoader()
        {
            BatchSize = 10000;
        }

        /// <summary>
        /// Gets or sets the size of the batch.
        /// </summary>
        /// <value>
        /// The size of the batch size, default is 10000.
        /// </value>
        public int BatchSize { get; set; }

        #region DBUtils

        /// <summary>
        /// Truncates the given table.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="tableName">Name of the table.</param>
        public void TruncateTable(SqlConnection connection, string tableName)
        {
            // check tablename for SqlInjection
            if (tableName.Contains(";") || tableName.Contains("--")) throw new ArgumentException();

            using (var cmd = new SqlCommand(string.Format("truncate table {0}", tableName), connection))
            {
                cmd.ExecuteNonQuery();
            }
        }

        #endregion



        #region SqlBulkInsert DataTable

        /// <summary>
        /// Does the BulkInsert of data in DataTable.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="dataTable">Tha data in a DataTable</param>
        /// <param name="options">The SqlBulkCopyOptions such as f.ex TableLock.</param>
        public void BulkInsert(SqlConnection connection, string tableName, DataTable dataTable, SqlBulkCopyOptions options = SqlBulkCopyOptions.Default)
        {
            using (var bulkCopy = new SqlBulkCopy(connection, options, null))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.WriteToServer(dataTable);
                bulkCopy.Close();
            }
        }

        #endregion

        #region BulkInsert POCO

        /// <summary>
        /// Does the BulkInsert of data in DataTable.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="list">The data in an IEnumerable of poco class.</param>
        /// <param name="options">The SqlBulkCopyOptions such as f.ex. TableLock.</param>
        public void BulkInsert<T>(SqlConnection connection, string tableName, IEnumerable<T> list, SqlBulkCopyOptions options = SqlBulkCopyOptions.Default)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTable(list);
                bulkCopy.WriteToServer(table);
            }
        }

        /// <summary>
        /// Does the BulkInsert of data in DataTable.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="list">The data in an IEnumerable of poco class.</param>
        /// <param name="options">The SqlBulkCopyOptions such as f.ex. TableLock.</param>
        public void BulkInsert<T>(string connectionString, string tableName, IEnumerable<T> list, SqlBulkCopyOptions options = SqlBulkCopyOptions.Default)
        {
            using (var bulkCopy = new SqlBulkCopy(connectionString))
            {
                bulkCopy.BatchSize = BatchSize;
                bulkCopy.DestinationTableName = tableName;
                var table = ListToDataTable(list);
                bulkCopy.WriteToServer(table);
            }
        }

        #endregion

        #region List<POCO> to DataTable

        /// <summary>
        /// Transforms a list of poco objects to a DataTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list">The list.</param>
        /// <returns></returns>
        public static DataTable ListToDataTable<T>(IEnumerable<T> list)
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

        /// <summary>
        /// Transforms a list of poco objects to a DataTable
        /// Uses FasterFlect to get the property values. 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list">The list.</param>
        /// <returns></returns>
        public static DataTable ListToDataTableFasterflect<T>(IEnumerable<T> list)
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
