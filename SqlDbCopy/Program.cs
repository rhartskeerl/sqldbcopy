using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDbCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("The current GC Server mode is: " + System.Runtime.GCSettings.IsServerGC);
            Logger log = new Logger();
            int maxdop = Environment.ProcessorCount * 2;

            if (args.Length < 3)
            {
                throw new ArgumentException("Not enough arguments provided.");
            }

            if (args.Length == 4)
            {
                Int32.TryParse(args[3], out maxdop);
                if (maxdop < 1)
                    maxdop = Environment.ProcessorCount * 2;
            }

            if (args.Length == 5)
            {

            }

            log.Write(String.Format("Starting database copy with {0} thread(s).", maxdop));
            Database database = new Database(args[0], args[1], log);

            if (args[2] == "*")
            {
                database.GetTablesFromSource();
            }
            else if (args[2].Contains('*'))
            {
                database.GetTablesFromSource(args[2]);
            }
            else if (args[2].ToLowerInvariant().EndsWith(".txt"))
            {
                database.Tables = new List<Table>();
                string[] tables = File.ReadAllLines(args[2]);
                for (int i = 0; i < tables.Length; i++)
                {
                    database.Tables.Add(new Table { Name = tables[i].ToQuotedName(), Query = "SELECT * FROM " + tables[i].ToQuotedName() });
                }
            }
            else if (args[2].ToLowerInvariant().EndsWith(".sql"))
            {
                database.Query = File.ReadAllText(args[2]);
                database.GetTablesFromSource();
            }

            Stopwatch s = Stopwatch.StartNew();
            database.Copy(maxdop);

            s.Stop();
            log.Write(String.Format("Finished in {0}ms.", s.ElapsedMilliseconds));
        }
    }
}
