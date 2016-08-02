using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;
using System.Globalization;

namespace FTP01
{
    public class LogFilter
    {
        private static readonly LogFilter instance = new LogFilter();

        static LogFilter() { }
        private LogFilter() { }

        public static LogFilter Instance
        {
            get
            {
                return instance;
            }
        }

        private static readonly object qLock = new object();
        private static readonly int totalCount = 109;
        private static readonly int batchCount = 1000;
        private static readonly int threadCount = 5;
        private static List<string> targetPeriodList = new List<string>();
        private static Queue<int> indexQueue = new Queue<int>();
        private static List<Task> busyList = new List<Task>();
        private static List<string> targetFileNameList = new List<string>();
        private static Dictionary<int, string> logIndexBook = new Dictionary<int, string>();

        private static string digitFormat = "000000";
        private static bool writeToFile = false;
        private static int checkCount = 0;

        #region Dispatch threads to get from FTP server
        public static int RetriveFiles(DateTime startDate, DateTime endDate)
        {
            if (true)
            {
                targetPeriodList = SeparateHourly(startDate, endDate);
            }

            if (targetPeriodList == null || targetPeriodList.Count <= 0)
                return 0;

            BuildIndices(targetPeriodList);

            string targetFileNamesLocal = @"C:\AkaimaiLog\CartFailCss206\targetFileNames.txt";
            if (writeToFile)
            {               
                foreach (var kvp in logIndexBook)
                {
                    targetFileNameList.Add(kvp.Value);
                    string content = string.Format("{0}:{1}", kvp.Key.ToString(digitFormat), kvp.Value); // "000000"
                    WriteToFile(content, @"C:\AkaimaiLog\CartFailCss206\targetLogSnIndex.txt");
                    WriteToFile(kvp.Value, targetFileNamesLocal);
                }
            }
            else
            {
                targetFileNameList = RetirveFromLocal(targetFileNamesLocal);
            }

            indexQueue = BuildIndexQueue(logIndexBook);
            if (indexQueue == null || indexQueue.Count <= 0)
                return 0;

            while (indexQueue.Count > 0)
            {
                if (busyList.Count < threadCount)
                {
                    int targetKey = -1;
                    lock (qLock)
                    {
                        targetKey = indexQueue.Dequeue();
                    }

                    Task task = Task.Factory.StartNew(() => 
                    {
                        lock (qLock)
                        {
                            //Console.WriteLine(targetKey);
                            //Console.WriteLine(checkCount);
                            GetFile(targetKey);
                        }
                    });

                    busyList.Add(task);
                }
                else
                {
                    int doneIdx = Task.WaitAny(busyList.ToArray());
                    busyList.RemoveAt(doneIdx);
                    checkCount += 1;
                }
            }
            return checkCount;
        }

        private static void GetFile(int targetKey) 
        {
            FTPConnection myftp = new FTPConnection("ftp://ftp01.newegg.com/", "nakamai", "SU#&pYU8Zd");
            string targetLog = "";

            if (logIndexBook.TryGetValue(targetKey, out targetLog))
            {
                string localName = string.Format(@"C:\AkaimaiLog\CartFailCss206\gzfiles\log{0}.gz", targetKey.ToString(digitFormat)); // "000000"
                myftp.DownloadFile(targetLog, localName);

                /*  For Test */
                //string localName = string.Format(@"C:\AkaimaiLog\CartFailCss206\gzfiles_OrgName\{0}", targetLog);
                //myftp.DownloadFile(targetLog, localName);               
            }
            else
            {
                Console.WriteLine("Invalid log index:" + targetKey);
            }
        }

        private static List<string> RetirveFromLocal(string localFile)
        {
            List<string> fileList = new List<string>();
            try
            {
                using (FileStream fs = File.Open(localFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";
                    while ((line = sr.ReadLine()) != null)
                    {
                        fileList.Add(line);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                fileList.Clear();
            }
            return fileList;
        }

        private static void BuildIndices(List<string> targetPeriodList)
        {
            string filePath = @"C:\AkaimaiLog\listOfDirDetails\logNameList.txt";
            List<string> targetLogs = new List<string>();
            try
            {
                using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";
                    string subStr = "";
                    string keyStr = @"AkamaiNewegg_9241.esclf_S.";
                    string filename = "";

                    int fileStartIdx = 0;
                    int fileEndIdx = 0;
                    int startIdx = 0;
                    int endIdx = 0;
                    int sn = 0;

                    while ((line = sr.ReadLine()) != null)
                    {
                        fileStartIdx = line.IndexOf(keyStr, StringComparison.OrdinalIgnoreCase);
                        if (fileStartIdx < 0) 
                            continue;

                        startIdx = fileStartIdx + keyStr.Length;
                        endIdx = startIdx + 12;
                        subStr = line.Substring(startIdx, endIdx - startIdx);
                        if (targetPeriodList.Contains(subStr.Substring(0, 10)))
                        {
                            fileEndIdx = line.LastIndexOf('.');
                            filename = line.Substring(fileStartIdx, fileEndIdx - fileStartIdx + 3);
                            logIndexBook.Add(sn, filename);
                            sn += 1;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static Queue<int> BuildIndexQueue(Dictionary<int, string> logIndices)
        {
            Queue<int> indexQ = new Queue<int>();
            if (logIndices.Count <= 0) return indexQ;

            foreach (var kvp in logIndices)
            {
                indexQ.Enqueue(kvp.Key);

                /* For Test */
                ////if (indexQ.Count >= 200) break;
            }
            return indexQ;
        }

        public static List<string> SeparateHourly(DateTime startDate, DateTime endDate)
        {
            List<Tuple<DateTime, DateTime>> datePairs = SeparateDateRange(startDate, endDate);

            List<string> hourList = new List<string>();
            string tmpStr = "";
            DateTime tmpDate = new DateTime();

            foreach (var pair in datePairs)
            {
                tmpDate = pair.Item1;
                while (tmpDate <= pair.Item2)
                {
                    tmpStr = tmpDate.ToString("yyyyMMddHH");
                    //Console.WriteLine(tmpStr);
                    hourList.Add(tmpStr);
                    tmpDate = tmpDate.AddHours(1);
                }
            }
            return hourList;
        }

        private static List<Tuple<DateTime, DateTime>> SeparateDateRange(DateTime startDate, DateTime endDate)
        {
            List<Tuple<DateTime, DateTime>> result = new List<Tuple<DateTime, DateTime>>();
            int daysCnt = (int)endDate.Subtract(startDate).TotalDays;
            if (daysCnt <= 1)
            {
                result.AddRange(CheckLEOneDay(startDate, endDate));
            }
            else
            {
                while (daysCnt > 0)
                {
                    result.Add(new Tuple<DateTime, DateTime>(startDate, startDate.Date.AddDays(1).AddMilliseconds(-1)));
                    startDate = startDate.Date.AddDays(1);
                    if (daysCnt <= 1)
                    {
                        result.AddRange(CheckLEOneDay(startDate, endDate));
                    }
                    daysCnt -= 1;
                }
            }
            return result;
        }

        private static List<Tuple<DateTime, DateTime>> CheckLEOneDay(DateTime startDate, DateTime endDate)
        {
            List<Tuple<DateTime, DateTime>> result = new List<Tuple<DateTime, DateTime>>();
            if (startDate.Date == endDate.Date)
            {
                result.Add(new Tuple<DateTime, DateTime>(startDate, endDate));
            }
            else
            {
                result.Add(new Tuple<DateTime, DateTime>(startDate, startDate.Date.AddDays(1).AddMilliseconds(-1)));
                result.Add(new Tuple<DateTime, DateTime>(startDate.Date.AddDays(1), endDate));
            }
            return result;
        }
        #endregion

        #region To SQL DB
        public static Dictionary<string, int> SaveToSqlDb()
        {
            Dictionary<string, int> ipCounts = new Dictionary<string, int>();
            int countAll = 0;

            for (int i = 0; i < totalCount; i++)
            {
                string targetfile = string.Format(@"C:\AkaimaiLog\all\log{0}", i.ToString("00000")); 
                try
                {
                    using (FileStream fs = File.Open(targetfile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (BufferedStream bs = new BufferedStream(fs))
                    using (StreamReader sr = new StreamReader(bs))
                    {
                        string line = "";
                        int batchCnt = batchCount;                            
                        List<string> logLines = new List<string>();

                        while ((line = sr.ReadLine()) != null)
                        {
                            logLines.Add(line);
                            countAll += 1;

                            if (logLines.Count == batchCnt)
                            {
                                BuildTable(logLines);
                                SQLDbAccess.BulkCopy(BuildTable(logLines));

                                logLines.Clear();
                            }

                            if (countAll % 100000 == 0)
                            {
                                Console.WriteLine("Inserted: " + countAll);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return null;
                }
            }

            Console.WriteLine(DateTime.Now);
            return ipCounts;
        }

        private static DataTable BuildTable(List<string> lines)
        {
            DataTable dt = new DataTable("AkamaiLogs");
            dt.Columns.Add("ReqIP", typeof(string));
            dt.Columns.Add("ReqTimeUTC", typeof(DateTimeOffset));
            dt.Columns.Add("ReqTimeLocal", typeof(DateTimeOffset));
            dt.Columns.Add("ReqTimeZ1", typeof(DateTimeOffset));
            dt.Columns.Add("ReqMethod", typeof(string));
            dt.Columns.Add("ReqURI", typeof(string));
            dt.Columns.Add("ReqString", typeof(string));
            dt.Columns.Add("Period", typeof(string));
            dt.Columns.Add("UserAgentString", typeof(string));
            dt.Columns.Add("OriginLog", typeof(string));
            dt.Columns.Add("LastEditDate", typeof(DateTimeOffset));
            dt.Columns.Add("LastEditUser", typeof(string));

            DateTimeOffset curTime = DateTimeOffset.Now;

            foreach (string line in lines)
            {
                DataRow row = dt.NewRow();
                int idx1 = 0, idx2 = 0;
                string restStr1 = ""; 
                string restStr2 = "";
                string ip = "";
                DateTimeOffset[] tmpTimes = new DateTimeOffset[3];

                idx1 = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
                ip = line.Substring(0, idx1 - 1);

                row["ReqIP"] = ip;
                
                restStr1 = line.Substring(idx1, line.Length - idx1);
                idx1 = restStr1.IndexOf("[", StringComparison.OrdinalIgnoreCase) + 1;
                idx2 = restStr1.IndexOf("]", StringComparison.OrdinalIgnoreCase);
                string tmpDate = restStr1.Substring(idx1, idx2 - idx1);
                tmpTimes = ConvertTime(tmpDate);

                row["ReqTimeUTC"] = tmpTimes[0];
                row["ReqTimeLocal"] = tmpTimes[1];
                row["ReqTimeZ1"] = tmpTimes[2];

                idx2 += 1;
                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                idx1 = restStr2.IndexOf("\"");
                idx1 += 1;
                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                idx2 = restStr1.IndexOf("\"");

                row["ReqString"] = restStr1.Substring(0, idx2);

                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                idx1 = restStr2.IndexOf("\"");
                idx1 += 1;                     
                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                idx2 = restStr1.IndexOf("\"");
                
                row["Period"] = restStr1.Substring(0, idx2).Trim() ;

                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);

                string splitStr1 = "\"-\" \"";
                string splitStr2 = "\" \"-\"";
                idx1 = restStr2.IndexOf(splitStr1) + splitStr1.Length;
                idx2 = restStr2.LastIndexOf(splitStr2) - splitStr2.Length;

                row["UserAgentString"] = restStr2.Substring(idx1, idx2);
                row["OriginLog"] = line;
                row["LastEditDate"] = curTime;
                row["LastEditUser"] = "sw9w";

                string reqStr = (string)row["ReqString"];
                idx1 = reqStr.IndexOf("/");

                row["ReqMethod"] = reqStr.Substring(0, idx1).Trim();
                idx1 += 1;
                row["ReqURI"] = reqStr.Substring(idx1, reqStr.LastIndexOf(" ") - idx1).Trim();

                dt.Rows.Add(row);
            }
            return dt;
        }

        private static DateTimeOffset[] ConvertTime(string dateString)
        {
            dateString = @"24/Mar/2015:18:55:05 +0000";
            DateTimeOffset utcOff = new DateTimeOffset();
            DateTimeOffset utc = new DateTimeOffset(),
                     pst = new DateTimeOffset(),
                     local = new DateTimeOffset();

            if (DateTimeOffset.TryParseExact(dateString, "dd/MMM/yyyy:HH:mm:ss zzzzz", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out utcOff))
            {
                utc = utcOff.UtcDateTime;
                local = utc.ToLocalTime();
                TimeZoneInfo pstZone = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
                pst = TimeZoneInfo.ConvertTimeFromUtc(utc.UtcDateTime, pstZone);
            }
            else
            {
                Console.WriteLine("Invalid datetime string: " + dateString);
            }

            return new DateTimeOffset[] { utc, local, pst };
        }
        #endregion

        #region To a DataTable
        private static DataTable BuildTableALL(List<string> lines)
        {
            DataTable dt = new DataTable("AkamaiLogs");
            dt.Columns.Add("ReqIP", typeof(string));
            dt.Columns.Add("ReqTimeUTC", typeof(DateTimeOffset));
            dt.Columns.Add("ReqTimeLocal", typeof(DateTimeOffset));
            dt.Columns.Add("ReqTimeZ1", typeof(DateTimeOffset));
            dt.Columns.Add("ReqMethod", typeof(string));
            dt.Columns.Add("ReqURI", typeof(string));
            dt.Columns.Add("ReqString", typeof(string));
            dt.Columns.Add("Period", typeof(string));
            dt.Columns.Add("UserAgentString", typeof(string));
            dt.Columns.Add("OriginLog", typeof(string));
            dt.Columns.Add("LastEditDate", typeof(DateTimeOffset));
            dt.Columns.Add("LastEditUser", typeof(string));

            DateTimeOffset curTime = DateTimeOffset.Now;
            int idx1 = 0, idx2 = 0;
            string restStr1 = "";
            string restStr2 = "";
            string ip = "";
            string tmpDateStr = "";
            DateTimeOffset[] tmpTimes = new DateTimeOffset[3];

            foreach (string line in lines)
            {
                DataRow row = dt.NewRow();

                idx1 = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
                ip = line.Substring(0, idx1 - 1);

                row["ReqIP"] = ip;

                restStr1 = line.Substring(idx1, line.Length - idx1);
                idx1 = restStr1.IndexOf("[", StringComparison.OrdinalIgnoreCase) + 1;
                idx2 = restStr1.IndexOf("]", StringComparison.OrdinalIgnoreCase);
                tmpDateStr = restStr1.Substring(idx1, idx2 - idx1);
                tmpTimes = GetDateTimes(tmpDateStr);

                row["ReqTimeUTC"] = tmpTimes[0];
                row["ReqTimeLocal"] = tmpTimes[1];
                row["ReqTimeZ1"] = tmpTimes[2];

                idx2 += 1;
                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                idx1 = restStr2.IndexOf("\"");
                idx1 += 1;
                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                idx2 = restStr1.IndexOf("\"");

                row["ReqString"] = restStr1.Substring(0, idx2);

                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                idx1 = restStr2.IndexOf("\"");
                idx1 += 1;
                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                idx2 = restStr1.IndexOf("\"");

                row["Period"] = restStr1.Substring(0, idx2).Trim();

                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);

                string splitStr1 = "\"-\" \"";
                string splitStr2 = "\" \"-\"";
                idx1 = restStr2.IndexOf(splitStr1) + splitStr1.Length;
                idx2 = restStr2.LastIndexOf(splitStr2) - splitStr2.Length;

                row["UserAgentString"] = restStr2.Substring(idx1, idx2);
                row["OriginLog"] = line;
                row["LastEditDate"] = curTime;
                row["LastEditUser"] = "sw9w";

                string reqStr = (string)row["ReqString"];
                idx1 = reqStr.IndexOf("/");

                row["ReqMethod"] = reqStr.Substring(0, idx1).Trim();
                idx1 += 1;
                row["ReqURI"] = reqStr.Substring(idx1, reqStr.LastIndexOf(" ") - idx1).Trim();

                dt.Rows.Add(row);
            }
            return dt;
        }
       
        ///<summary>
        ///return DateTimeOffset[] {UTC, Local, PST} 
        ///</summary>
        private static DateTimeOffset[] GetDateTimes(string dateString, TimeZoneInfo timeZone = null)
        {
            if (timeZone == null) 
                timeZone = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");

            DateTimeOffset utcOff = new DateTimeOffset(),
                           utc = new DateTimeOffset(),
                           local = new DateTimeOffset(),
                           askZone = new DateTimeOffset();

            if (DateTimeOffset.TryParseExact(dateString, "dd/MMM/yyyy:HH:mm:ss zzzzz", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out utcOff))
            {
                utc = utcOff.UtcDateTime;
                local = utc.ToLocalTime();
                askZone = TimeZoneInfo.ConvertTimeFromUtc(utc.UtcDateTime, timeZone);
            }
            else
            {
                Console.WriteLine("Invalid datetime string: " + dateString);
            }

            return new DateTimeOffset[] { utc, local, askZone };
        }

        public static Dictionary<string, int> TallyTargetLogs(DateTime start, DateTime end)
        {
            Dictionary<string, int> ipCounts = new Dictionary<string, int>();

            int totalCnt = totalCount; //109;
            int runCount = totalCnt; // number in this run
            int dealtCnt = 0;

            List<List<string>> logLines = new List<List<string>>();

            for (int i = 0; i < runCount; i++)
            {
                List<string> logFields = new List<string>();
                string targetfile = string.Format(@"C:\AkaimaiLog\TargetPeriod\all_logs\log{0}", i.ToString("00000"));

                try
                {
                    using (FileStream fs = File.Open(targetfile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (BufferedStream bs = new BufferedStream(fs))
                    using (StreamReader sr = new StreamReader(bs))
                    {
                        string line = "";
                        int idx1 = 0, idx2 = 0;
                        string restStr1 = "";
                        string restStr2 = "";
                        string ip = "";
                        string tmpDate = "";
                        DateTimeOffset[] tmpTimes = new DateTimeOffset[3];
                        string dateFormat = "MMM dd yyyy HH:mm:ss zzz";
                        

                        while ((line = sr.ReadLine()) != null)
                        {
                            dealtCnt += 1;
                            if (dealtCnt % 10000 == 0) Console.WriteLine(dealtCnt + "\n" + line);

                            idx1 = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
                            ip = line.Substring(0, idx1 - 1);

                            restStr1 = line.Substring(idx1, line.Length - idx1);
                            idx1 = restStr1.IndexOf("[", StringComparison.OrdinalIgnoreCase) + 1;
                            idx2 = restStr1.IndexOf("]", StringComparison.OrdinalIgnoreCase);
                            tmpDate = restStr1.Substring(idx1, idx2 - idx1);
                            tmpTimes = GetDateTimes(tmpDate);

                            if (tmpTimes[1] >= start && tmpTimes[1] <= end) // use PDT time to compare. 
                            {
                                logFields.Add(ip); // ip address
                                logFields.Add(tmpTimes[0].ToString(dateFormat, CultureInfo.InvariantCulture)); // utc
                                logFields.Add(tmpTimes[1].ToString(dateFormat, CultureInfo.InvariantCulture)); // local

                                idx2 += 1;
                                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                                idx1 = restStr2.IndexOf("\"");
                                idx1 += 1;
                                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                                idx2 = restStr1.IndexOf("\"");

                                logFields.Add(restStr1.Substring(0, idx2)); // request string

                                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                                idx1 = restStr2.IndexOf("\"");
                                idx1 += 1;
                                restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
                                idx2 = restStr1.IndexOf("\"");

                                logFields.Add(restStr1.Substring(0, idx2).Trim()); // response time?

                                restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
                                string splitStr1 = "\"-\" \"";
                                string splitStr2 = "\" \"-\"";
                                idx1 = restStr2.IndexOf(splitStr1) + splitStr1.Length;
                                idx2 = restStr2.LastIndexOf(splitStr2) - splitStr2.Length;

                                logFields.Add(restStr2.Substring(idx1, idx2)); // UserAgentString
                                logFields.Add(line); // OriginLog
                                logFields.Add(restStr1.Substring(0, idx2).Trim()); // response time?
                                logFields.Add(restStr1.Substring(0, idx2).Trim()); // response time?

                                logLines.Add(logFields);

                                if (!string.IsNullOrEmpty(ip))
                                {
                                    //Console.WriteLine(ip);
                                    int count = 0;
                                    if (ipCounts.TryGetValue(ip, out count))
                                    {
                                        count += 1;
                                        ipCounts[ip] = count;
                                    }
                                    else
                                    {
                                        ipCounts[ip] = 1;
                                    }
                                }

                                string logContent = "";
                                foreach (string f in logFields)
                                {
                                    if (string.IsNullOrEmpty(logContent))
                                    {
                                        logContent = f;
                                    }
                                    else
                                    {
                                        logContent = logContent + "||" + f;
                                    }
                                }
                                logContent.Trim();
                                WriteToFile(logContent, @"C:\AkaimaiLog\TargetPeriod\output\inPeriodLogs.txt");
                                logFields.Clear();
                            }
                            else continue;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return null;
                }
            }

            Console.WriteLine(DateTime.Now);
            return ipCounts;
        }
        #endregion

        /* Calculate counts per id */
        public static Dictionary<string, int> TallyIPs()
        {
            Dictionary<string, int> ipCounts = new Dictionary<string, int>();
            
            int totalCnt = totalCount; //541;
            int runCount = totalCnt;

            for (int i = 0; i < runCount; i++)
            {
                string targetfile = string.Format(@"C:\AkaimaiLog\TargetPeriod\all_logs\log{0}", i.ToString("00000"));
                try
                {
                    using (FileStream fs = File.Open(targetfile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (BufferedStream bs = new BufferedStream(fs))
                    using (StreamReader sr = new StreamReader(bs))
                    {
                        string line = "";
                        string ip = "";
                        int endIdx = 0;

                        while ((line = sr.ReadLine()) != null)
                        {
                            endIdx = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
                            ip = line.Substring(0, endIdx - 1);
                            if (!string.IsNullOrEmpty(ip))
                            {
                                Console.WriteLine(ip);
                                int count = -1;
                                if (ipCounts.TryGetValue(ip, out count))
                                {
                                    count += 1;
                                    ipCounts[ip] = count;
                                }
                                else
                                {
                                    ipCounts[ip] = 1;
                                }
                            }
                        }
                    }                 
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return null;
                }              
            }

            Console.WriteLine(DateTime.Now);
            return ipCounts;
        }

        /* Depleted */
        public static List<string> FilterLogs(string orgFileList, List<string> targetDates)
        {
            List<string> targetLogs = new List<string>();
            try
            {
                using (FileStream fs = File.Open(orgFileList, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";
                    string subStr = "";
                    string keyStr = @"AkamaiNewegg_9241.esclf_S.";
                    string targetYear = "2015";
                    ////List<string> targetDates = new List<string>()
                    ////{
                    ////    "031907",
                    ////    "031908",
                    ////    "031909",
                    ////    "031910"
                    ////};

                    string filename = "";

                    int fileStartIdx = 0;
                    int fileEndIdx = 0;
                    int startIdx = 0;
                    int endIdx = 0;

                    while ((line = sr.ReadLine()) != null)
                    {
                        //Console.WriteLine(line);
                        fileStartIdx = line.IndexOf(keyStr, StringComparison.OrdinalIgnoreCase);
                        startIdx = fileStartIdx + keyStr.Length;
                        endIdx = startIdx + 12;
                        subStr = line.Substring(startIdx, endIdx - startIdx);
                        //Console.WriteLine(subStr);
                        if (subStr.Substring(0, 4) == targetYear
                            && targetDates.Contains(subStr.Substring(4, 6)))
                        {
                            Console.WriteLine(line);
                            fileEndIdx = line.LastIndexOf('.');
                            filename = line.Substring(fileStartIdx, fileEndIdx - fileStartIdx + 3);
                            targetLogs.Add(filename);
                            Console.WriteLine(filename);
                        }
                    }
                }
                return targetLogs;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return new List<string>();
            }
        }

        public static void SortByValue(string file)
        {
            Dictionary<string, int> org = new Dictionary<string, int>();
            string[] spliter = { "|" };

            try
            {
                using (FileStream fs = File.Open(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";

                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] result = line.Split(spliter, StringSplitOptions.None);
                        org.Add((string)result[0], int.Parse(result[1]));
                    }
                }

                Stack<string> sortedKVP = new Stack<string>();
                string outfile = @"C:\AkaimaiLog\TargetPeriod\output\inPeriodSortedResult.txt";

                foreach (KeyValuePair<string, int> kvp in org.OrderBy(key => key.Value))
                {
                    string content = string.Format("{0} -- {1}", kvp.Key, kvp.Value);
                    sortedKVP.Push(content);
                    //WriteToFile(content, outfile);
                }

                int sn = 0;
                while (sortedKVP.Count != 0)
                {
                    sn += 1;
                    string str = string.Format("{0} -- {1}", sn.ToString("0000"), sortedKVP.Pop());
                    Console.WriteLine(str);
                    WriteToFile(str, outfile);

                    /* for test */
                    //if (sn >= 1000) break;
                }

                //Console.ReadLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
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
}
