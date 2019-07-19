using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ThreadState = System.Threading.ThreadState;

namespace SqlDbCopy
{
    class Database
    {
        const string SYS_TABLES = "select s.name + '.' + t.name from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id order by s.name, t.name";
        private int _currentThreadCount = 0;
        public Database() { }

        public Database(string source, string destination)
        {
            Source = source;
            Destination = destination;
        }

        public Database(string source, string destination, Logger log)
        {
            Source = source;
            Destination = destination;
            Log = log;
        }
        public string Source { get; set; }
        public string Destination { get; set; }

        public Logger Log { get; set; }
        public string[] Tables { get; set; }

        public void Copy(int maxdop)
        {
            if (Destination.Contains("Data Source"))
                TruncateSource();

            Thread[] threads = new Thread[Tables.Length];
            _currentThreadCount = 0;

            for(int i = 0;i < Tables.Length;i++)
            {
                _currentThreadCount++;
                threads[i] = new Thread(CopyTable);
                threads[i].Start(Tables[i]);
                while(_currentThreadCount == maxdop)
                {
                    Thread.Sleep(500);
                }
            }
            for (int i = 0; i < threads.Length;i++)
            {
                if (threads[i].ThreadState == ThreadState.Running)
                    threads[i].Join();
            }
        }

        private void CopyTable(object o)
        {
            string table = (string)o;
            if(!String.IsNullOrEmpty(table))
            {
                long rows = 0;
                SqlConnection connectionSource = new SqlConnection(Source);
                try
                {
                    connectionSource.Open();
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    Log.Write(String.Format("Starting copy of {0}.", table));

                    SqlCommand source = new SqlCommand("SELECT * FROM " + table, connectionSource);
                    SqlDataReader reader = source.ExecuteReader();

                    if(Destination.ToLowerInvariant().Contains("data source"))
                    {
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(Destination, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls)
                        {
                            DestinationTableName = table,
                            EnableStreaming = true,
                            BatchSize = 100000
                        };

                        bulkCopy.WriteToServer(reader);
                        rows = bulkCopy.RowsAffected();
                    }
                    else
                    {
                        if (!Directory.Exists(Destination + "\\"))
                            Directory.CreateDirectory(Destination + "\\");

                        StreamWriter streamWriter = new StreamWriter(Destination + "\\" + table.Replace("[","").Replace("]","") + ".csv");
                        while(reader.Read())
                        {
                            streamWriter.WriteLine(reader.ToCsv());
                            rows++;
                        }
                        streamWriter.Close();
                    }
                    reader.Close();
                    connectionSource.Close();
                    stopwatch.Stop();
                    Log.Write(string.Format("Finished copying {0} rows to {1} in {2}ms", rows, table, stopwatch.ElapsedMilliseconds));
                }
                catch (Exception ex)
                {
                    Log.Write(ex.Message);
                }
                finally
                {
                    _currentThreadCount--;
                }
            }

        }
        private void TruncateSource()
        {
            StringBuilder sb = new StringBuilder();
            foreach(string t in Tables)
            {
                if(!String.IsNullOrEmpty(t))
                    sb.AppendLine("TRUNCATE TABLE " + t);
            }

            using (SqlConnection connection = new SqlConnection(Destination))
            {
                using (SqlCommand command = new SqlCommand(sb.ToString(), connection))
                {
                    connection.Open();
                    command.ExecuteNonQuery();
                    connection.Close();
                }
            }
        }
        public void GetTablesFromSource()
        {
            GetTablesInternal("");
        }

        public void GetTablesFromSource(string filter)
        {
            GetTablesInternal(filter);
        }

        private void GetTablesInternal(string filter)
        {
            string sql = SYS_TABLES;

            if(!String.IsNullOrEmpty(filter))
            {
                sql = SYS_TABLES + " LIKE '" + filter.Replace('*', '%') + "'";
            }

            List<string> tableList = new List<string>();
            SqlConnection source = new SqlConnection(Source);
            source.Open();
            SqlCommand command = new SqlCommand(sql, source);
            SqlDataReader reader = command.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                tableList.Add(reader.GetString(0).ToQuotedName());
            }
            reader.Close();
            source.Close();
            Tables = tableList.ToArray();
        }

    }
}
