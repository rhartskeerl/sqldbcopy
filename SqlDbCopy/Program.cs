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
            bool async = true;

            if(args.Length < 3)
            {
                throw new ArgumentException("Not enough arguments provided.");
            }

            if(args.Length == 4)
            {
                if (args[3].ToLowerInvariant() == "true" || args[3] == "1")
                    async = false;
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
                database.Tables = File.ReadAllLines(args[2]);
            }

            Stopwatch s = Stopwatch.StartNew();
            if (async)
                database.CopyAsync().Wait();
            else
                database.Copy();
            s.Stop();
            log.Write(String.Format("Finished in {0}ms.", s.ElapsedMilliseconds));
        }
    }
}
