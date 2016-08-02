using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;

namespace FTP01
{
    class Program
    {
        static int Main(string[] args)
        {
            var teststart = DateTime.Now;
            Console.WriteLine("Start at: " + teststart);
            #region Get all log-file-name list in the folder of the FTP server.
            /* 01. Get all log-file-name list in the folder */
            //FTPConnection myftp = new FTPConnection("ftp://ftp01.newegg.com/", "nakamai", "SU#&pYU8Zd");
            //////myftp.DownloadFile(@"AkamaiNewegg_9241.esclf_S.201411170000-0100-1.gz", @"C:\AkaimaiLog\download\test-0100-5.gz");

            string localOutput = @"C:\AkaimaiLog\listOfDirDetails\logNameList.txt";

            //myftp.GetDirFileList(localOutput);
            #endregion

            #region Filter to get wanted files.
            ///* 02. Filter log to get target file names. */
            // string forEnormousGetReq = @"C:\AkaimaiLog\loglist.txt";
            string filePath = localOutput;          
            //List<string> targetLogs = LogFilter.FilterLogs(filePath);

            DateTime startDate = new DateTime(2015, 3, 28, 00, 0, 0);
            DateTime endDate = new DateTime(2015, 3, 30, 23, 0, 0);
            //int checkCnt = LogFilter.RetriveFiles(startDate, endDate);
            //Console.ReadKey();
            #endregion

            #region Analizing log
            string logIndexFile = "";// @"C:\AkaimaiLog\CartFailCss206\targetLogSnIndex.txt";

            int  processOption = 4;

            string processName = "";
            switch (processOption)
            {
                case 1: processName = "TallyIPs";
                    break;
                case 2: processName = "CheckHttp206_CountAllCSS";
                    break;
                case 3: processName = "CheckHttp206_Count206";
                    break;
                case 4: processName = "IndexURL";
                    break;
                case 5: processName = "....";
                    break;
                default: break;
            }

            Dictionary<string, int> countBook = new Dictionary<string, int>();

            if (processName.Equals("IndexURL", StringComparison.OrdinalIgnoreCase))
            {
                var urlBook = AkamaiLogAnalyzer.StartProcess(logIndexFile, processName);
                return 1;
            }
            else
            {
                countBook = AkamaiLogAnalyzer.StartProcess(logIndexFile, processName);
            }

            bool runtest = false;
            var testend = new DateTime();

            if (runtest)
            {
                testend = DateTime.Now;
                Console.WriteLine("Finish at: " + testend);
                Console.WriteLine("Spent: " + testend.Subtract(teststart).TotalSeconds + " (sec)");
                Console.ReadKey();

                return 1;
            }

            Stack<string> sortedKVP = new Stack<string>();

            foreach (KeyValuePair<string, int> kvp in countBook.OrderBy(key => key.Value))
            {
                string content = string.Format("{0} -- {1}", kvp.Key, kvp.Value);
                sortedKVP.Push(content);
                //WriteToFile(content, outfile);
            }

            int sn = 0;
            string outFile = "defualtOutput.txt";

            if (processName.Equals("TallyIPs"))
            {
                outFile = @"C:\AkaimaiLog\CartFailCss206\ipcounts.txt";
            }
            else if (processName.Equals("CheckHttp206_CountAllCSS"))
            {
                outFile = @"C:\AkaimaiLog\CartFailCss206\cssCountsAll_generaticUpdated.txt";
            }
            else if (processName.Equals("CheckHttp206_Count206"))
            {
                outFile = @"C:\AkaimaiLog\CartFailCss206\cssCounts206_MR.txt";
            }
            else
            {
            }

            while (sortedKVP.Count != 0)
            {
                sn += 1;
                string str = string.Format("{0} -- {1}", sn.ToString("0000"), sortedKVP.Pop());
                if (sn % 100000 == 0) Console.WriteLine(str);
                WriteToFile(str, outFile);
            }

            #endregion

            testend = DateTime.Now;
            Console.WriteLine("Finished at: " + testend);
            Console.WriteLine("Spent: " + testend.Subtract(teststart).TotalSeconds + " (sec)");
            Console.ReadKey();


            #region test
            /* test*/
            ////int fileSN = 0;
            ////foreach (string f in targetLogs)
            ////{
            ////    string fileIndex = string.Format("{0} : log{1}", f, fileSN.ToString("00000"));
            ////    WriteToFile(fileIndex, @"C:\AkaimaiLog\TargetPeriod\log0319Index.txt");
            ////    fileSN += 1;
            ////}
            #endregion

            #region Retrive files
            /* 02-1. Retrive target files */
            //FTPConnection myftp = new FTPConnection("ftp://ftp01.newegg.com/", "nakamai", "SU#&pYU8Zd");
            ////int sn = 0;
            ////foreach (string log in targetLogs)
            ////{
            ////    string localName = string.Format(@"C:\AkaimaiLog\TargetPeriod\gzfiles\log{0}.gz", sn.ToString("00000"));
            ////    myftp.DownloadFile(log, localName);
            ////    sn += 1;
            ////    //if (sn > 10) break;
            ////}
            #endregion

            #region Local files: Bulid dictionary to count
            /* 04. Bulid dictionary to count */
            //string metapath = @"C:\AkaimaiLog\TargetPeriod\output\ipcounts.txt";

            //Dictionary<string, int> ipCounts = LogFilter.TallyIPs();

            //foreach (var kvp in ipCounts)
            //{
            //    string line = string.Format("{0}|{1}", (string)kvp.Key, (int)kvp.Value);
            //    WriteToFile(line, metapath);
            //}
            #endregion

            #region Result output 
            /* Output result */
            //string sourceFile = @"C:\AkaimaiLog\TargetPeriod\output\ipcounts.txt";
            //LogFilter.SortByValue(sourceFile);
            #endregion

            #region retrive during a specific time period
            /* 05. Retrive IPs and counts from specific period. */
            /* 20150319的1:15 – 1:45 (PDT)*/
            DateTime start = new DateTime(2015, 3, 19, 1, 15, 0 );
            DateTime end = new DateTime(2015, 3, 19, 1, 45, 0);
            #endregion

            #region ipcount
            //Dictionary<string, int> ipPeriodCounts = LogFilter.TallyTargetLogs(start, end);

            //foreach (var kvp in ipPeriodCounts)
            //{
            //    string line = string.Format("{0}|{1}", (string)kvp.Key, (int)kvp.Value);
            //    WriteToFile(line, @"C:\AkaimaiLog\TargetPeriod\output\inPeriodIpCounts.txt");
            //}
            //DateTime runEnd = DateTime.Now;
            //Console.WriteLine("Finish at: " + end);
            //Console.WriteLine("Spent : " + runEnd.Subtract(runStart).TotalMinutes);

            /* Output result */
            //string sourceFile = @"C:\AkaimaiLog\TargetPeriod\output\inPeriodIpCounts.txt";
            //LogFilter.SortByValue(sourceFile);
            #endregion


            #region tests
            /* SQL db test */
            //LogFilter.SaveToSqlDb();
            //string testConnStr = @"Server=localhost\SQLEXPRESS;Database=AkamaiLog;Trusted_Connection=Yes;";
            //SQLDbAccess.TestSQL(testConnStr);

            //////var testend = DateTime.Now;
            //////Console.WriteLine("Finish at: " + testend);
            //////Console.WriteLine("Spent : " + end.Subtract(teststart).TotalSeconds);
            //////Console.ReadKey();

            //////var runStart = DateTime.Now;
            //////Console.WriteLine("Start at: " + runStart);

            ///////* Threads */
            //////Queue<string> idxQue = BuildIndexQueue(109); 
            //////int option = 0;
            //////string path = @"C:\AkaimaiLog\TargetPeriod\all_logs\";
            //////int threadCount = 1;
            //////StartProcess(option, idxQue, path, threadCount);
            #endregion

            //Console.ReadKey();
            return 1;
        }

        public static int StartProcess(int opt, Queue<string> idxQue, string path, int threadCnt)
        {
            int allCount = 0;


            while (idxQue.Count > 0)
            {
                string logIdx = System.IO.Path.Combine(path, idxQue.Dequeue());
                Console.WriteLine(logIdx);
                
                allCount += 1;
            }
            return allCount;
        }

        public static int Process(int opt, Queue<string> idxQue, string path)
        {
            int allCount = 0;

            while (idxQue.Count > 0)
            {
                string logIdx = System.IO.Path.Combine(path, idxQue.Dequeue());
                Console.WriteLine(logIdx);

                allCount += 1;
            }
            return allCount;
        }

        public static Queue<string> BuildIndexQueue(int logCount)
        {
            Queue<string> index = new Queue<string>();
            if (logCount <= 0) return index;

            for (int i = 0; i < logCount; i++)
            {
                index.Enqueue(string.Format("log{0}", i.ToString("00000")));
            }
            return index;
        }

        public static int WriteToFile(string content, string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
            {
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

    //public class FTPConnection
    //{
    //    private string host = null;
    //    private string user = null;
    //    private string pass = null;
    //    private FtpWebRequest ftpRequest = null;
    //    private FtpWebResponse ftpResponse = null;
    //    private Stream responseStream = null;
    //    private StreamReader reader = null;
    //    private FileStream fileStream = null;
    //    private int bufferSize = 2048;

    //    public FTPConnection(string hostIP, string userName, string password) 
    //    { 
    //        host = hostIP; 
    //        user = userName; 
    //        pass = password; 
    //    }

    //    public void DownloadFile(string remoteFile, string localFile)
    //    {
    //        try
    //        {
    //            ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host + "/" + remoteFile);
    //            ftpRequest.Credentials = new NetworkCredential(user, pass);
    //            /* When in doubt, use these options */
    //            //ftpRequest.UseBinary = true;
    //            //ftpRequest.UsePassive = true;
    //            //ftpRequest.KeepAlive = true;

    //            /* Check App.config for other modifications for proxy error */
    //            /* my edit to solve Bad Gateway error ["http://www.codeproject.com/Questions/332730/FTP-proxy-problem-in-Csharp-application"] */
    //            ftpRequest.Proxy = null;
    //            ftpRequest.Method = WebRequestMethods.Ftp.DownloadFile;
    //            ftpResponse = (FtpWebResponse)ftpRequest.GetResponse();
    //            responseStream = ftpResponse.GetResponseStream();
    //            FileStream localFileStream = new FileStream(localFile, FileMode.Create);
    //            byte[] byteBuffer = new byte[bufferSize];
    //            int bytesRead = responseStream.Read(byteBuffer, 0, bufferSize);
    //            try
    //            {
    //                while (bytesRead > 0)
    //                {
    //                    localFileStream.Write(byteBuffer, 0, bytesRead);
    //                    bytesRead = responseStream.Read(byteBuffer, 0, bufferSize);
    //                }
    //            }
    //            catch (Exception ex) { Console.WriteLine(ex.ToString()); }
    //            /* Resource Cleanup */
    //            finally { localFileStream.Close();}
    //        }
    //        catch (Exception ex) 
    //        { 
    //            Console.WriteLine(ex.ToString()); 
    //        }
    //        finally
    //        {
    //            responseStream.Close();
    //            ftpResponse.Close();
    //            ftpRequest = null;
    //        }
    //        return;
    //    }

    //    /* Get the list of filenames under the directory */
    //    public void GetDirFileList(string outputPath)
    //    {
    //        try
    //        {
    //            ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host);
    //            ftpRequest.Method = WebRequestMethods.Ftp.ListDirectoryDetails;

    //            ftpRequest.Credentials = new NetworkCredential(user, pass);

    //            /* Check App.config for other modifications for proxy error */
    //            /* my edit to solve Bad Gateway error ["http://www.codeproject.com/Questions/332730/FTP-proxy-problem-in-Csharp-application"] */
    //            ftpRequest.Proxy = null;

    //            ftpResponse = (FtpWebResponse)ftpRequest.GetResponse();
    //            responseStream = ftpResponse.GetResponseStream();

    //            // Write to file.
    //            //fileStream = File.Create(@"C:\AkaimaiLog\download\loglist.txt");
    //            fileStream = File.Create(outputPath);

    //            byte[] buffer = new byte[32 * 1024];
    //            int read;

    //            while ((read = responseStream.Read(buffer, 0, buffer.Length)) > 0)
    //            {
    //                fileStream.Write(buffer, 0, read);
    //            }       
    //        }
    //        catch (Exception ex)
    //        {
    //            Console.WriteLine(ex.ToString());
    //        }
    //        finally
    //        {
    //            if (reader != null) reader.Close();
    //            if (fileStream != null) fileStream.Close();
    //            if (responseStream != null) responseStream.Close();
    //            if (ftpResponse != null) ftpResponse.Close();
    //            if (ftpRequest != null) ftpRequest = null;
    //        }
    //    }
    //}
}
