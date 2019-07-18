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
            Logger log = new Logger();
            int maxdop = 1;

            if(args.Length < 3)
            {
                throw new ArgumentException("Not enough arguments provided.");
            }

            if(args.Length == 4)
            {
                Int32.TryParse(args[3], out maxdop);
                if (maxdop < 1)
                    maxdop = 1;
            }

            Database database = new Database(args[0], args[1], log);
            
            if(args[2] == "*")
            {
                database.GetTablesFromSource();
            } else if (args[2].Contains('*'))
            {
                database.GetTablesFromSource(args[2]);
            } else if (args[2].ToLowerInvariant().EndsWith(".txt"))
            {
                string[] tables = File.ReadAllLines(args[2]);
                for(int i = 0; i<tables.Length;i++)
                {
                    tables[i] = tables[i].ToQuotedName();
                }
                database.Tables = tables;
            }

            Stopwatch s = Stopwatch.StartNew();
            database.Copy(maxdop);

            s.Stop();
            log.Write(String.Format("Finished in {0}ms.", s.ElapsedMilliseconds));
        }
    }
}
