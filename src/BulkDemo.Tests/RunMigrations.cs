using System;
using BulkDemo.Test.Properties;
using FluentMigrator.Runner;
using FluentMigrator.Runner.Announcers;
using FluentMigrator.Runner.Initialization;

namespace BulkDemo
{
    using NUnit.Framework;


    [SetUpFixture]
    public class RunMigrations
    {
        [SetUp]
        public void ExecuteMigrations()
        {
            IAnnouncer announcer = new TextWriterAnnouncer(Console.Out);
            IRunnerContext migrationContext = new RunnerContext(announcer)
            {
                Connection = Settings.Default.BulkDemoConnection,
                Database = "sqlserver",
                Target = "BulkDemo.Test"
            };

            TaskExecutor executor = new TaskExecutor(migrationContext);
            executor.Execute();
        }
    }
}

