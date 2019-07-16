using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlDbCopy
{
    class Database
    {
        const string SYS_TABLES = "select '[' + s.name + '].[' + t.name + ']' from sys.tables t inner join sys.schemas s on t.schema_id = s.schema_id";
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

        public void Copy()
        {
            TruncateSource();
            foreach(string t in Tables)
            {
                CopyTableAsync(t).Wait();
            }
        }

        public async Task CopyAsync()
        {
            TruncateSource();
            await Task.WhenAll(Tables.Select(async t => await CopyTableAsync(t)));
        }

        private async Task CopyTableAsync(string table)
        {
            if(!String.IsNullOrEmpty(table))
            {
                SqlConnection connectionSource = new SqlConnection(Source);
                try
                {
                    connectionSource.Open();
                    Stopwatch stopwatch = Stopwatch.StartNew();
                    Log.Write(String.Format("Starting copy of {0}.", table));

                    SqlCommand source = new SqlCommand("SELECT * FROM " + table, connectionSource);

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(Destination, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.KeepNulls)
                    {
                        DestinationTableName = table,
                        EnableStreaming = true,
                        BatchSize = 100000
                    };
                    SqlDataReader reader = source.ExecuteReader();
                    await bulkCopy.WriteToServerAsync(reader);
                    reader.Close();
                    connectionSource.Close();
                    stopwatch.Stop();
                    Log.Write(string.Format("Finished copying {0} in {1}ms", table, stopwatch.ElapsedMilliseconds));
                }
                catch (Exception ex)
                {
                    Log.Write(ex.Message);
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
                tableList.Add(reader.GetString(0));
            }
            reader.Close();
            source.Close();
            Tables = tableList.ToArray();
        }
    }
}
