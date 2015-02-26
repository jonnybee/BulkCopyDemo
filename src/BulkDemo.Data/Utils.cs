using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace BulkDemo.Data
{
    public static class Utils
    {
        public static void TimedAction(string message, Action action)
        {
            var sw = new Stopwatch();
            try
            {
                sw.Start();
                action.Invoke();
            }
            finally
            {
                sw.Stop();
                Trace.WriteLine(string.Format(message, sw.ElapsedMilliseconds));
            }
        }
    }
}
