using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using BulkDemo.Data;
using Dapper;
using NUnit.Framework;

namespace BulkDemo.Test
{
    public class DataLoaderTest
    {
        [TestFixture]
        public class BulkInsertWithSqlBulkCopy
        {
            private SqlConnection connection;
            private DataTable data;


            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
                data = new Generator().GetDataTableData();
            }

            [Test]
            public void Insert100000RowsFromDataTable()
            {
                Utils.TimedAction("Using SqlBulkCopyFromDataTable: {0} ms",
                                  () => new DataLoader().BulkInsert(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertWithSqlBulkCopyAndTableLock
        {
            private SqlConnection connection;
            private DataTable data;


            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
                data = new Generator().GetDataTableData();
            }

            [Test]
            public void Insert100000RowsFromDataTableWithTableLock()
            {
                Utils.TimedAction("Using SqlBulkCopyFromDataTableWithTableLock: {0} ms",
                                  () =>  new DataLoader().BulkInsert(connection, "BulkTable", data, SqlBulkCopyOptions.TableLock));
            }
        }

        [TestFixture]
        public class BulkInsertPocoList
        {
            private SqlConnection connection;
            private List<TestData> data;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
                data = new Generator().GetTestData();
            }

            [Test]
            public void Insert100000RowsFromPocoList()
            {
                Utils.TimedAction("Using SqlBulkCopyFromPocoList: {0} ms",
                                  () => new DataLoader().BulkInsert(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertPocoListUsingTableLock
        {
            private SqlConnection connection;
            private List<TestData> data;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
                data = new Generator().GetTestData();
            }

            [Test]
            public void Insert100000RowsFromPocoListWithTableLock()
            {
                Utils.TimedAction("Using SqlBulkCopyFromPocoListWithTableLock: {0} ms",
                                  () => new DataLoader().BulkInsert(connection, "BulkTable", data, SqlBulkCopyOptions.TableLock));
            }
        }

        [TestFixture]
        public class BulkInsertWithSqlDataAdapter
        {
            private SqlConnection connection;
            private DataTable data;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
                data = new Generator().GetDataTableData();
            }

            [Test]
            public void Insert1000000RowsWithSqlDataAdapter()
            {
                Utils.TimedAction("Using SqlDataAdapterFromDataTable: {0} ms",
                                  () => new DataAdapterLoader().BulkInsert(connection, data));
            }
        }

        [TestFixture]
        public class ToDataTable
        {
            [Test]
            public void PocoListToDataTableWithReflection()
            {
                var data = new Generator().GetTestData();
                DataTable table = null;

                Utils.TimedAction("PocoListToDataTableWithReflection: {0} ms",
                                  () => table =  DataLoader.ListToDataTable(data));

                Assert.IsNotNull(table);
                Assert.AreEqual(100000, data.Count);
                Assert.AreEqual(100000, table.Rows.Count);
            }

            [Test]
            public void PocoListToDataTableWithFasterflect()
            {
                var data = new Generator().GetTestData();
                DataTable table = null;

                Utils.TimedAction("PocoListToToDataTableWithFasterFlect: {0} ms",
                                  () => table = DataLoader.ListToDataTableFasterflect(data));

                Assert.IsNotNull(table);
                Assert.AreEqual(100000, data.Count);
                Assert.AreEqual(100000, table.Rows.Count);
            }
        }

        [TestFixture]
        public class LoadDataByDapper
        {
            private SqlConnection connection;

            [SetUp]
            public void InitConnectionAndData()
            {
                connection = Db.GetConnection();
                var data = new Generator().GetTestData();
                new DataLoader().TruncateTable(connection, "BulkTable");
                new DataLoader().BulkInsert(connection, "BulkTable", data);
            }

            [Test]
            public void Load100000RowsFromDbWithDapper()
            {
                int rowCount = 0;
                Utils.TimedAction("Load data using Dapper: {0} ms",
                  () => rowCount = connection.Query<TestData>("select * from BulkTable").ToList().Count);
                Assert.AreEqual(100000, rowCount);
            }
        }
    }
}
