using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDbCopy
{
    public enum LogLevel { Basic, Debug, Verbose}
    public class Logger
    {
        public string File { get; set; }

        public LogLevel Detail { get; set; } = LogLevel.Basic;

        public void Write(string message, LogLevel detail = LogLevel.Basic)
        {
            if(detail == Detail)
            {
                Console.WriteLine("{0:O}\t{1}", DateTime.UtcNow, message );
            }
        }
    }
}
