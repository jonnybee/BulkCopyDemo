using System.Collections.Generic;
using System.Data;

namespace BulkDemo.Test
{
  public class Generator
  {
    #region DataTable

    public DataTable GetDataTableData()
    {
      var table = CreateTable();
      FillDataTable(ref table);
      return table;
    }

    private static DataTable CreateTable()
    {
      var table = new DataTable();
      table.Columns.Add("ID", typeof(int));
      table.Columns.Add("Name", typeof(string));
      table.Columns.Add("EMail", typeof(string));
      return table;
    }

    private static void FillDataTable(ref DataTable table)
    {

      var values = new object[3];
      for (int i = 1; i < 100001; i++)
      {
        values[0] = i;
        values[1] = i % 5 == 0 ? "Jonny" : i % 4 == 0 ? "Frode" : i % 3 == 0 ? "Roar" : i % 2 == 0 ? "Stein" : "Bente" + " " + i;
        values[2] = System.Guid.NewGuid().ToString("N") + "@gmail.com";
        table.Rows.Add(values);
      }
    }

    #endregion

    #region TestData

    public List<TestData> GetTestData()
    {
      var list = new List<TestData>();

      for (int i = 1; i < 100001; i++)
      {
        list.Add(new TestData()
                   {
                     ID = i,
                     Name = i % 5 == 0 ? "Jonny" : i % 4 == 0 ? "Frode" : i % 3 == 0 ? "Roar" : i % 2 == 0 ? "Stein" : "Bente" + " " + i,
                     EMail = System.Guid.NewGuid().ToString("N") + "@gmail.com",
                   });
      }

      return list;
    }

    #endregion
  }

  public class TestData
  {
    public int ID { get; set; }
    public string Name { get; set; }
    public string EMail { get; set; }
  }
}
