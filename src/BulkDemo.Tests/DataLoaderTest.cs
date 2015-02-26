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


            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
            }

            [Test]
            public void Insert100000RowsFromDataTable()
            {
                var data = new Generator().GetDataTableData();

                Utils.TimedAction("Using SqlBulkCopy: {0} ms",
                                  () => new DataLoader().BulkInsertWithSqlBulkCopy(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertWithSqlBulkCopyAndTableLock
        {
            private SqlConnection connection;


            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
            }

            [Test]
            public void Insert100000RowsFromDataTableWithTableLock()
            {
                var data = new Generator().GetDataTableData();

                Utils.TimedAction("Using SqlBulkCopyAndTableLock: {0} ms",
                                  () => new DataLoader().BulkInsertWithSqlBulkCopyAndTableLock(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertPocoList
        {
            private SqlConnection connection;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
            }

            [Test]
            public void Insert100000RowsFromPocoList()
            {
                var data = new Generator().GetTestData();

                Utils.TimedAction("Using BulkInsert: {0} ms",
                                  () => new DataLoader().BulkInsert(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertPocoListUsingTableLock
        {
            private SqlConnection connection;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
            }

            [Test]
            public void Insert100000RowsFromPocoListWithTableLock()
            {
                var data = new Generator().GetTestData();

                Utils.TimedAction("Using BulkInsertWithTableLock: {0} ms",
                                  () => DataLoader.BulkInsertWithTableLock(connection, "BulkTable", data));
            }
        }

        [TestFixture]
        public class BulkInsertWithSqlDataAdapter
        {
            private SqlConnection connection;

            [SetUp]
            public void TruncateTable()
            {
                connection = Db.GetConnection();
                new DataLoader().TruncateTable(connection, "BulkTable");
            }

            [Test]
            public void Insert1000000RowsWithSqlDataAdapter()
            {
                var data = new Generator().GetDataTableData();

                Utils.TimedAction("Using SqlDatAdapter: {0} ms",
                                  () => new DataLoader().BulkInsertWithSqlDataAdapter(connection, data));
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

                Utils.TimedAction("ToDataTable: {0} ms",
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

                Utils.TimedAction("ToDataTableFF: {0} ms",
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
