# BulkCopyDemo
Shows 3 different implementations for BulkCopy with SqlServer and the takeway from this code is the DataLoader class that simplifies doing bulk inserts into table either directly from a poco class or from a DataTable that match the database table. 

Rather than specifying mapping I prefer to structure poco classes in my code that match tha database and then populate the list and do the database update as fast as possible to avoid locks/downtime.

Note: The DataLoader class when supplied with a poco list will implicitly transform the poco list into a datatable before it call SqlBulkCopy. This is basically the time difference when looking at elapsed time. 

Figures below is measured on my dev computer with database running on the same computer. Timing is for insert of 100000 rows into empty table.  

    Using SqlBulkCopyFromPocoList:                 646 ms
    Using SqlBulkCopyFromPocoListWithTableLock:    535 ms
    Using SqlBulkCopyFromDataTable:                318 ms
    Using SqlBulkCopyFromDataTableWithTableLock:   441 ms
    Using SqlDataAdapterFromDataTable:           10180 ms
    PocoListToDataTableWithFasterFlect:            189 ms
    PocoListToDataTableWithReflection:             216 ms      
  
# Getting started #
This project uses a SqlServer database with a simple table that has 3 columns and one primary key. 

You must create the database in SqlServer Studio or Visual Studio and verify that the connection string is correct in test project app.config. The default configuration is: 

    <connectionStrings>
        <add name="BulkDemo.Test.Properties.Settings.BulkDemoConnection"
            connectionString="Data Source=(localdb)\mssqllocaldb;Initial Catalog=BulkDemo;Integrated Security=SSPI" />
    </connectionStrings>

The test project uses FluentMigrator to create the actual table on first run. 

# Different ways to do Bulk Insert #

There is 2 basic variants for builk insert in X# / .NET: SqlBulkCopy and SqlDataAdapter

SqlDataAdapter is remarkably slow. On my dev computer it takes about 10seconds to bulk insert 100000 rows into a table with 3 colums.  So whenever possible I would avoid using SqlDataAdapter. On the positive side, SqlDataAdapter supportes both Insert/Update/Delete on rows in a data table so you are not limited to just Insert

SqlBulkCopy is similar to **bcp**  command in SqlServer and will accept a DataTable or IDataReader as input. 

In my code the focus is on using SqlBulkCopy in an as simple way as possible with typically a list of poco object or from a datatable that is build in your own C# code and internally transform the poco list to a DataTable and do BulkInsert on the items. 

So assuming that you have a data list og datatable in memory you just call: 

    new DataLoader().BulkInsert(connection, "BulkTable", data);


or you may also choose the overload that allows you to specify SqlBulkCopyOptions like TableLock (default value is RowLock)

    new DataLoader().BulkInsert(connection, "BulkTable", data, SqlBulkCopyOptions.TableLock)	