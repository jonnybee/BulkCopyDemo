using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentMigrator;
using FluentMigrator.Expressions;

namespace BulkDemo.Database.Migrations
{
    [Migration(20150622112000, "Create BulkTable")]
    public class CreateDb : Migration
    {
        public override void Up()
        {
            Create.Table("BulkTable")
                .WithColumn("ID").AsInt64().PrimaryKey()
                .WithColumn("Name").AsString(255)
                .WithColumn("EMail").AsString(255);
        }

        public override void Down()
        {
            Delete.Table("BulkTable");
        }
    }
}
