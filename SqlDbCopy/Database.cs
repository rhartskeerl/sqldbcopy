using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ThreadState = System.Threading.ThreadState;

namespace SqlDbCopy
{
    class Database
    {
        public string Query { get; set; }  = "select s.name + '.' + t.name, 'select * from ' + s.name + '.' + t.name  from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id order by s.name, t.name";
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
        public List<Table> Tables { get; set; }

        public void Copy(int maxdop)
        {
            if (Destination.ToLowerInvariant().Contains("data source") || Destination.ToLowerInvariant().Contains("server"))
                TruncateSource();

            Thread[] threads = new Thread[Tables.Count];
            _currentThreadCount = 0;

            for (int i = 0; i < Tables.Count; i++)
            {
                _currentThreadCount++;
                threads[i] = new Thread(CopyTable);
                threads[i].Start(Tables[i]);
                while (_currentThreadCount == maxdop)
                {
                    Thread.Sleep(500);
                }
            }
            for (int i = 0; i < threads.Length; i++)
            {
                if (threads[i].ThreadState == ThreadState.Running)
                    threads[i].Join();
            }
        }

        private void CopyTable(object o)
        {
            Table table = (Table)o;
            if (!String.IsNullOrEmpty(table.Name))
            {
                long rows = 0;
                SqlConnection connectionSource = new SqlConnection(Source);
                try
                {
                    connectionSource.Open();
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    Log.Write(String.Format("Starting copy of {0}.", table.Name));

                    SqlCommand source = new SqlCommand(table.Query, connectionSource);
                    source.CommandTimeout = 0;
                    SqlDataReader reader = source.ExecuteReader();

                    if (Destination.ToLowerInvariant().Contains("data source") || Destination.ToLowerInvariant().Contains("server"))
                    {
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(Destination, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls)
                        {
                            DestinationTableName = table.Name,
                            EnableStreaming = true,
                            BatchSize = 100000,
                            BulkCopyTimeout = 0
                        };

                        bulkCopy.WriteToServer(reader);
                        rows = bulkCopy.RowsAffected();
                    }
                    else if (Destination.ToLowerInvariant() == "null")
                    {
                        while (reader.Read())
                        {
                            rows++;
                        }
                    }
                    else
                    {
                        if (!Directory.Exists(Destination + "\\"))
                            Directory.CreateDirectory(Destination + "\\");

                        using (FileStream outFile = File.Create(Destination + "\\" + table.Name.Replace("[", "").Replace("]", "") + ".gz"))
                        using (GZipStream compress = new GZipStream(outFile, CompressionMode.Compress))
                        using (StreamWriter streamWriter = new StreamWriter(compress))
                        {
                            while (reader.Read())
                            {
                                streamWriter.WriteLine(reader.ToCsv());
                                rows++;
                            }
                            streamWriter.Close();
                        }
                    }
                    reader.Close();
                    connectionSource.Close();
                    stopwatch.Stop();
                    Log.Write(string.Format("Finished copying {0} rows to {1} in {2}ms", rows, table.Name, stopwatch.ElapsedMilliseconds));
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
            foreach (Table t in Tables)
            {
                if (!String.IsNullOrEmpty(t.Name))
                    sb.AppendLine("TRUNCATE TABLE " + t.Name);
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
            if (!String.IsNullOrEmpty(filter))
            {
                Query = Query + " LIKE '" + filter.Replace('*', '%') + "'";
            }

            Tables = new List<Table>();

            SqlConnection source = new SqlConnection(Source);
            source.Open();
            SqlCommand command = new SqlCommand(Query, source);
            SqlDataReader reader = command.ExecuteReader(CommandBehavior.CloseConnection);
            while (reader.Read())
            {
                Tables.Add(new Table { Name = reader.GetString(0).ToQuotedName(), Query = reader.GetString(1) });
            }
            reader.Close();
            source.Close();
        }

    }
}
