using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;
using System.Globalization;

namespace FTP01
{
    public class AkamaiLogAnalyzer
    {
        private static readonly AkamaiLogAnalyzer instance = new AkamaiLogAnalyzer();

        static AkamaiLogAnalyzer() { }
        private AkamaiLogAnalyzer() { }

        public static AkamaiLogAnalyzer Instance
        {
            get
            {
                return instance;
            }
        }

        /* Get .css return 206 issue */
        private static Dictionary<string, int> cssCountAll = new Dictionary<string, int>();
        private static Dictionary<string, int> cssCountHttp206 = new Dictionary<string, int>();
        private static Dictionary<string, int> cssCountOthers = new Dictionary<string, int>();
        private static Dictionary<string, Tuple<string, int, Dictionary<string, int>>> css206UrlBook
            = new Dictionary<string, Tuple<string, int, Dictionary<string, int>>>(); // <url, <size, count, <(httpcode, count)...>>>
        private static List<AkamaiLogEntity> css206log = new List<AkamaiLogEntity>();
        private static readonly object logListLock = new object();

        /* IP counts */
        private static readonly object ipLock = new object();
        private static int checkCount = 0;
        private static Dictionary<string, int> ipCounts = new Dictionary<string, int>();

        /* For test */
        private static readonly bool runTestCount = true;
        private static readonly int testCount = 20;

        /* For basic operation */  
        private static readonly int threadCount = 2;
        private static readonly object qLock = new object();
        private static Dictionary<string, string> idxBook = new Dictionary<string, string>();
        private static Queue<string> idxQue = new Queue<string>();
        private static List<Task> busyList = new List<Task>();
        private static readonly object mrLock = new object();

        public static Dictionary<string, int> StartProcess(string source, string processName)
        {
            if (string.IsNullOrEmpty(source))
                source = @"C:\AkaimaiLog\CartFailCss206\targetLogSnIndex.txt";

            BuildIndexInfo(source);

            if (idxBook == null || idxBook.Count <= 0)
                return null;

            string sourceDir = @"C:\AkaimaiLog\CartFailCss206\gzfiles";
            List<int> resultCodes = new List<int>();

            if (processName.Equals("IndexURL", StringComparison.OrdinalIgnoreCase))
            {
                InitUrlBook();
            }

            while (idxQue.Count > 0)
            {
                string logPath = "";
                if (busyList.Count < threadCount)
                {
                    lock (qLock)
                    {
                        if (idxQue.Count > 0)
                            logPath = System.IO.Path.Combine(sourceDir, string.Format("log{0}.gz", idxQue.Dequeue()));
                    }

                    Task task = Task.Factory.StartNew(() =>
                    {
                        int d = Process(logPath, processName);
                        resultCodes.Add(d);
                        checkCount += 1;

                        //Console.WriteLine(checkCount);
                        //Console.WriteLine(logPath);
                    });

                    busyList.Add(task);
                }
                else
                {
                    int doneIdx = Task.WaitAny(busyList.ToArray());
                    busyList.RemoveAt(doneIdx);
                }
            }

            /* Wait untill all threads finish */
            if (busyList.Count > 0) Task.WaitAll(busyList.ToArray());

            /* Return result */
            if (processName.Equals("TallyIPs", StringComparison.OrdinalIgnoreCase))
            {
                return ipCounts;
            }
            else if (processName.Equals("CheckHttp206_CountAllCSS", StringComparison.OrdinalIgnoreCase))
            {
                return cssCountAll;
            }
            else if (processName.Equals("CheckHttp206_Count206", StringComparison.OrdinalIgnoreCase))
            {
                //foreach (AkamaiLogEntity log in css206log)
                //{
                //    WriteToFile(log.ToString(), @"C:\AkaimaiLog\CartFailCss206\cssCounts206_logs.txt");
                //}
                return cssCountHttp206;
            }
            else return new Dictionary<string, int>();
        }

        public static int Process(string path, string processName)
        {
            List<string> decmprsdFile = new List<string>();
            try
            {
                FileInfo fileToDecompress = new FileInfo(path);
                decmprsdFile = Decompress(fileToDecompress);

                /* For Test */
                bool writeFile = false;

                if (writeFile)
                    RenameWriteToSubFolder(decmprsdFile, path, "dcmprsdtxt", ".file.txt");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return -1;
            }

            /* Analysis work here */
            if (processName.Equals("TallyIPs", StringComparison.OrdinalIgnoreCase))
            {
                TallyIPs(decmprsdFile);
            }
            else if (processName.Equals("CheckHttp206_CountAllCSS", StringComparison.OrdinalIgnoreCase))
            {
                CheckCSS(path, decmprsdFile, processName);
            }
            else if (processName.Equals("CheckHttp206_Count206", StringComparison.OrdinalIgnoreCase))
            {
                CheckHttp206(path, decmprsdFile, processName);
            }
            else if (processName.Equals("IndexURL", StringComparison.OrdinalIgnoreCase))
            {
                IndexURL(path, decmprsdFile, processName);
            }
            return 1;
        }

        public static int IndexURL(string path, List<string> fileContent, string processName)
        {
            List<AkamaiLogEntity> logs = ParseToEntity(fileContent);

            foreach (AkamaiLogEntity log in logs)
            {
                var tmp = new Dictionary<string, int>(); // <http_code, Count>
                var tmpTuple = new Tuple<string, int, Dictionary<string, int>>("", 0, tmp); // <size, count, code_count>

                if (css206UrlBook.TryGetValue(log.URL, out tmpTuple))
                {
                    CheckHttpCode(ref tmpTuple, log.ResponseCode);
                    int count = tmpTuple.Item2;
                    var newTuple = new Tuple<string, int, Dictionary<string, int>>(log.Size, count + 1, tmpTuple.Item3);
                    css206UrlBook[log.URL] = newTuple;
                }
                else continue;
            }
            return 1;
        }

        public static void CheckHttpCode(ref Tuple<string, int, Dictionary<string, int>> tpl, string code)
        {
            Dictionary<string, int> codeInfo = tpl.Item3;
            int count = 0;
            if (codeInfo.TryGetValue(code, out count))
            {
                count += 1;
                codeInfo[code] = count;
            }
            else
            {
                codeInfo[code] = 1;
            }
        }

        public static int InitUrlBook()
        {
            string sourcePath = @"C:\AkaimaiLog\CartFailCss206\mrResult_20150403\cssCounts206_MR_work.txt";
            string[] spliters = { " -- " };
            try
            {
                using (FileStream fs = File.Open(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";
                    int chkCnt = 0;
                    while ((line = sr.ReadLine()) != null)
                    {
                        chkCnt += 1;
                        string[] items = line.Split(spliters, StringSplitOptions.None);
                        Dictionary<string, int> codeInfo = new Dictionary<string, int>();
                        var tmp = new Dictionary<string, int>();
                        var Tpl = new Tuple<string, int, Dictionary<string, int>>("", 0, tmp);
                        css206UrlBook.Add(items[1], Tpl);
                        //Console.WriteLine(chkCnt + "  " + items[1]);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -1;
            }
            return 1;
        }

        public static void CheckHttp206(string path, List<string> fileContent, string processName)
        {
            int chkCount = 0;
            List<AkamaiLogEntity> logs = ParseToEntity(fileContent);
            Dictionary<string, int> countBook = null;
            string target = "";

            if (processName.Equals("CheckHttp206_Count206", StringComparison.OrdinalIgnoreCase))
            {
                countBook = cssCountHttp206;
                target = "URL";
            }

            foreach (AkamaiLogEntity log in logs)
            {
                if (log.URL.EndsWith(".css") && log.ResponseCode == "206")
                {
                    chkCount += 1;
                    if (chkCount % 100 == 0)
                    {
                        Console.WriteLine("{0}, {1}, {2}, {3}", log.ResponseCode, log.IP, log.URL, log.LogTimeUTC);
                    }
                    //lock (logListLock)
                    //{
                    //    css206log.Add(log);
                    //}
                    MapReduce<string, int>(countBook, (string)log.GetType().GetProperty(target).GetValue(log, null));
                }
            }
        }

        public static void CheckCSS(string path, List<string> fileContent, string processName)
        {
            string keyExt = ".css";
            int chkCount = 0;
            List<AkamaiLogEntity> logs = ParseToEntity(fileContent);
            var countBook = cssCountAll;
            string target = "";

            if (processName.Equals("CheckHttp206_CountAllCSS", StringComparison.OrdinalIgnoreCase))
            {
                countBook = cssCountAll;
                target = "URL";
            }

            foreach (AkamaiLogEntity log in logs)
            {
                if (log.URL.EndsWith(keyExt))
                {
                    chkCount += 1;
                    if (chkCount % 100 == 0)
                    {
                        Console.WriteLine("{0}, {1}, {2}, {3}", log.ResponseCode, log.IP, log.URL, log.LogTimeUTC);
                    }
                    MapReduce<string, int>(countBook, (string)log.GetType().GetProperty(target).GetValue(log, null));
                }
            }
        }

        public static void CheckHttpError_Bak(string path, List<string> fileContent, string processName)
        {
            string keyExt = ".css";
            string keyCode = "206";
            string subfolder = "csstxt";
            int chkCount = 0;
            List<AkamaiLogEntity> logs = ParseToEntity(fileContent);

            foreach (var log in logs)
            {
                if (log.URL.EndsWith(keyExt))
                {
                    chkCount += 1;
                    if (chkCount % 100 == 0)
                    {
                        Console.WriteLine("{0}, {1}, {2}, {3}", log.ResponseCode, log.IP, log.URL, log.LogTimeUTC);
                    }
                    MapReduce<string, int>(cssCountAll, log.URL);
                }
            }
        }

        public static void MapReduce<T, T1>(Dictionary<T, int> countBook, T key)
        {
            lock (mrLock)
            {
                int count = -1;
                if (countBook.TryGetValue(key, out count))
                {
                    count += 1;
                    countBook[key] = count;
                }
                else
                {
                    countBook[key] = 1;
                }
            }
        }

        public static List<AkamaiLogEntity> ParseToEntity(List<string> lines)
        {
            List<AkamaiLogEntity> entities = new List<AkamaiLogEntity>();
            foreach (string line in lines)
            {
                entities.Add(ParseToEntity(line));       
            }
            return entities;
        }

        /* For test */
        public static AkamaiLogEntity ParseToEntity_Bak(string line)
        {
            AkamaiLogEntity entity = new AkamaiLogEntity();

            int idx1 = 0, idx2 = 0;
            string restStr1, restStr2, restStr3, restStr4, restStr5, restStr6;
            string ip = "";
            DateTimeOffset tmpTime = new DateTimeOffset();
            idx1 = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
            ip = line.Substring(0, idx1 - 1);

            entity.IP = ip;

            restStr1 = line.Substring(idx1, line.Length - idx1);
            idx1 = restStr1.IndexOf("[", StringComparison.OrdinalIgnoreCase) + 1;
            idx2 = restStr1.IndexOf("]", StringComparison.OrdinalIgnoreCase);
            string tmpDate = restStr1.Substring(idx1, idx2 - idx1);
            DateTimeOffset.TryParseExact(tmpDate, "dd/MMM/yyyy:HH:mm:ss zzzzz", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out tmpTime); //ConvertTime(tmpDate);

            entity.LogTimeUTC = tmpTime;

            idx2 += 1;
            restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
            idx1 = restStr2.IndexOf("\"");
            idx1 += 1;

            restStr3 = restStr2.Substring(idx1, restStr2.Length - idx1);
            idx2 = restStr3.IndexOf("\"");

            entity.RequestString = restStr3.Substring(0, idx2);

            restStr4 = restStr3.Substring(idx2, restStr3.Length - idx2);
            idx1 = restStr4.IndexOf("\"");
            idx1 += 1;
            restStr5 = restStr4.Substring(idx1, restStr4.Length - idx1);
            idx2 = restStr5.IndexOf("\"");
            string[] numbers = restStr5.Substring(0, idx2).Trim().Split(' ');

            entity.ResponseCode = numbers[0];
            entity.Size = numbers[1];

            restStr6 = restStr5.Substring(idx2, restStr5.Length - idx2);
            string splitStr1 = "\"-\" \"";
            string splitStr2 = "\" \"-\"";
            idx1 = restStr6.IndexOf(splitStr1) + splitStr1.Length;
            idx2 = restStr6.LastIndexOf(splitStr2) - splitStr2.Length;

            entity.UserAgentString = restStr6.Substring(idx1, idx2);
            entity.AllLogString = line;

            string reqStr = entity.RequestString;
            idx1 = reqStr.IndexOf("/");

            entity.Method = reqStr.Substring(0, idx1).Trim();
            entity.URL = reqStr.Substring(idx1, reqStr.LastIndexOf(" ") - idx1).Trim();

            return entity;
        }

        public static AkamaiLogEntity ParseToEntity(string line)
        {
            AkamaiLogEntity entity = new AkamaiLogEntity();

            int idx1 = 0, idx2 = 0;
            string restStr1 = "";
            string restStr2 = "";
            string ip = "";
            DateTimeOffset[] tmpTimes = new DateTimeOffset[3];
            idx1 = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
            ip = line.Substring(0, idx1 - 1);

            entity.IP = ip;

            restStr1 = line.Substring(idx1, line.Length - idx1);
            idx1 = restStr1.IndexOf("[", StringComparison.OrdinalIgnoreCase) + 1;
            idx2 = restStr1.IndexOf("]", StringComparison.OrdinalIgnoreCase);
            string tmpDate = restStr1.Substring(idx1, idx2 - idx1);
            tmpTimes = ConvertTime(tmpDate);

            entity.LogTimeUTC = tmpTimes[0];

            idx2 += 1;
            restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
            idx1 = restStr2.IndexOf("\"");
            idx1 += 1;
            restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
            idx2 = restStr1.IndexOf("\"");

            entity.RequestString = restStr1.Substring(0, idx2);

            restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
            idx1 = restStr2.IndexOf("\"");
            idx1 += 1;
            restStr1 = restStr2.Substring(idx1, restStr2.Length - idx1);
            idx2 = restStr1.IndexOf("\"");
            string[] numbers = restStr1.Substring(0, idx2).Trim().Split(' ');

            entity.ResponseCode = numbers[0];
            entity.Size = numbers[1];

            restStr2 = restStr1.Substring(idx2, restStr1.Length - idx2);
            string splitStr1 = "\"-\" \"";
            string splitStr2 = "\" \"-\"";
            idx1 = restStr2.IndexOf(splitStr1) + splitStr1.Length;
            idx2 = restStr2.LastIndexOf(splitStr2) - splitStr2.Length;

            entity.UserAgentString = restStr2.Substring(idx1, idx2);
            entity.AllLogString = line;

            string reqStr = entity.RequestString;
            idx1 = reqStr.IndexOf("/");

            entity.Method = reqStr.Substring(0, idx1).Trim();
            entity.URL = reqStr.Substring(idx1, reqStr.LastIndexOf(" ") - idx1).Trim();

            return entity;
        }

        private static DateTimeOffset[] ConvertTime(string dateString)
        {
            DateTimeOffset utcOff = new DateTimeOffset();
            DateTimeOffset utc = new DateTimeOffset(),
                           pst = new DateTimeOffset(),
                           local = new DateTimeOffset();

            if (DateTimeOffset.TryParseExact(dateString, "dd/MMM/yyyy:HH:mm:ss zzzzz", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out utcOff))
            {
                utc = utcOff.UtcDateTime;
                //local = utc.ToLocalTime();
                //TimeZoneInfo pstZone = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
                //pst = TimeZoneInfo.ConvertTimeFromUtc(utc.UtcDateTime, pstZone);
            }
            else
            {
                Console.WriteLine("Invalid datetime string: " + dateString);
            }

            return new DateTimeOffset[] { utc, local, pst };
        }

        public static void RenameWriteToSubFolder(List<string> fileContent, string path, string subFolderName, string tailExt)
        {
            int a = path.LastIndexOf("\\");
            int b = path.LastIndexOf(".");
            string filename = path.Substring(a, b - a);
            string newDir = string.Format("{0}\\{1}\\", path.Substring(0, path.LastIndexOf("\\")), subFolderName);
            WriteToFile(fileContent, newDir + filename + tailExt);
        }

        public static void TallyIPs(List<string> logLines)
        {
            string ip = "";
            int endIdx = 0;
            int checkCnt = 0;
            foreach (string line in logLines)
            {
                endIdx = line.IndexOf("-", StringComparison.OrdinalIgnoreCase);
                checkCnt += 1;
                if (checkCnt % 100000 == 0) { Console.WriteLine(ip + " : " + checkCnt); }
                try
                {
                    ip = line.Substring(0, endIdx - 1);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }

                if (!string.IsNullOrEmpty(ip))
                {
                    lock (ipLock)
                    {
                        //Console.WriteLine(ip);
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

        public static Dictionary<string, string> BuildIndexInfo(string source)
        {
            string[] spliters = { ":" };
            try
            {
                using (FileStream fs = File.Open(source, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (BufferedStream bs = new BufferedStream(fs))
                using (StreamReader sr = new StreamReader(bs))
                {
                    string line = "";
                    while ((line = sr.ReadLine()) != null)
                    {
                        string[] items = line.Split(spliters, StringSplitOptions.None);
                        idxBook.Add(items[0], items[1]);
                        idxQue.Enqueue(items[0]);

                        /* For Test */
                        if (runTestCount && idxBook.Count >= testCount) break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }            
            return idxBook;
        }

        public static List<string> Decompress(FileInfo fileToDecompress)
        {
            List<string> lines = new List<string>();
            using (FileStream originalFileStream = fileToDecompress.OpenRead())
            {
                string currentFileName = fileToDecompress.FullName;
                string newFileName = currentFileName.Remove(currentFileName.Length - fileToDecompress.Extension.Length);

                using (MemoryStream memStream = new MemoryStream())
                using (GZipStream decompressionStream = new GZipStream(originalFileStream, CompressionMode.Decompress))
                using (StreamReader rdr = new StreamReader(memStream))
                {
                    decompressionStream.CopyTo(memStream);
                    memStream.Seek(0, SeekOrigin.Begin);
                    string s = String.Empty;
                    while ((s = rdr.ReadLine()) != null)
                    {
                        try
                        {
                            lines.Add(s);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    }
                    Console.WriteLine("Ready: {0}", fileToDecompress.Name);
                }
            }
            return lines;
        }

        public static int WriteToFile(List<string> content, string filePath)
        {
            foreach(string line in content)
            {
                WriteToFile(line, filePath);
            }
            return content.Count;
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
