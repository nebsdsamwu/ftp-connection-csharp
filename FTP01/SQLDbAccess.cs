using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace FTP01
{
    public static class SQLDbAccess
    {
        private static readonly string akamaiLogConn = @"Server=localhost\SQLEXPRESS;Database=AkamaiLog;Trusted_Connection=Yes;";

        public static void TestSQL(string connectionString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlDataReader reader = null;
                SqlCommand myCommand =
                    new SqlCommand("SELECT TOP 1 * FROM [AkamaiLog].[dbo].[AkamaiLogs]", connection);

                try
                {
                    connection.Open();

                    //using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    //{
                    //    bulkCopy.DestinationTableName = "MyTable";
                    //    bulkCopy.ColumnMappings.Add("mytext", "mytext");
                    //    bulkCopy.ColumnMappings.Add("num", "num");
                    //    bulkCopy.WriteToServer(table);
                    //}
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                finally
                {
                    if (reader != null) reader.Close();
                }
            }
        }

        public static void BulkCopy(DataTable table)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(akamaiLogConn))
                {
                    connection.Open();
                    //Console.WriteLine(table.Rows[0].ItemArray[0]);
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = "AkamaiLogs";
                        bulkCopy.ColumnMappings.Add("ReqIP", "ReqIP");
                        bulkCopy.ColumnMappings.Add("ReqTimeUTC", "ReqTimeUTC");
                        bulkCopy.ColumnMappings.Add("ReqTimeLocal", "ReqTimeLocal");
                        bulkCopy.ColumnMappings.Add("ReqString", "ReqString");
                        bulkCopy.ColumnMappings.Add("ReqMethod", "ReqMethod");
                        bulkCopy.ColumnMappings.Add("ReqURI", "ReqURI");
                        bulkCopy.ColumnMappings.Add("Period", "Period");
                        bulkCopy.ColumnMappings.Add("UserAgentString", "UserAgentString");
                        bulkCopy.ColumnMappings.Add("OriginLog", "OriginLog");
                        bulkCopy.ColumnMappings.Add("LastEditDate", "LastEditDate");
                        bulkCopy.ColumnMappings.Add("LastEditUser", "LastEditUser");
                        bulkCopy.WriteToServer(table);
                    }
                }
            }
            catch (Exception ex)
            {
                string orglog = (string)table.Rows[0].ItemArray[9];
                Console.WriteLine(orglog);
                WriteToFile(orglog, "");
                Console.WriteLine(ex);
            }
        }

        public static void BulkCopyToSQL(DataTable tempTbl, string connectStr, string tableName)
        {
            try
            {
                using (SqlConnection conn = new SqlConnection(connectStr))
                {
                    conn.Open();

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                    {
                        bulkCopy.BatchSize = 500;
                        bulkCopy.BulkCopyTimeout = 120; // (sec)

                        bulkCopy.DestinationTableName = tableName;

                        foreach (DataColumn col in tempTbl.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                        }

                        bulkCopy.WriteToServer(tempTbl);
                    }

                    tempTbl.Clear();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public static int WriteToFile(string content, string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
                filePath = @"C:\AkaimaiLog\exlogs\exceptionFirstLine.txt";
                return -1;
            }

            try
            {
                using (StreamWriter writer = new StreamWriter(filePath, true))
                {
                    writer.WriteLine(content);
                }
                return 1;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return -1;
            }
        }
    }
}
