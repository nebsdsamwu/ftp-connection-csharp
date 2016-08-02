using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Net;

namespace FTP01
{
    public class FTPConnection
    {
        private string host = null;
        private string user = null;
        private string pass = null;
        private FtpWebRequest ftpRequest = null;
        private FtpWebResponse ftpResponse = null;
        private Stream responseStream = null;
        private StreamReader reader = null;
        private FileStream fileStream = null;
        private int bufferSize = 2048;

        public FTPConnection(string hostIP, string userName, string password)
        {
            host = hostIP;
            user = userName;
            pass = password;
        }

        public void DownloadFile(string remoteFile, string localFile)
        {
            try
            {
                ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host + "/" + remoteFile);
                ftpRequest.Credentials = new NetworkCredential(user, pass);
                /* When in doubt, use these options */
                //ftpRequest.UseBinary = true;
                //ftpRequest.UsePassive = true;
                //ftpRequest.KeepAlive = true;

                /* Check App.config for other modifications for proxy error */
                /* my edit to solve Bad Gateway error ["http://www.codeproject.com/Questions/332730/FTP-proxy-problem-in-Csharp-application"] */
                ftpRequest.Proxy = null;
                ftpRequest.Method = WebRequestMethods.Ftp.DownloadFile;
                ftpResponse = (FtpWebResponse)ftpRequest.GetResponse();
                responseStream = ftpResponse.GetResponseStream();
                FileStream localFileStream = new FileStream(localFile, FileMode.Create);
                byte[] byteBuffer = new byte[bufferSize];
                int bytesRead = responseStream.Read(byteBuffer, 0, bufferSize);
                try
                {
                    while (bytesRead > 0)
                    {
                        localFileStream.Write(byteBuffer, 0, bytesRead);
                        bytesRead = responseStream.Read(byteBuffer, 0, bufferSize);
                    }
                }
                catch (Exception ex) { Console.WriteLine(ex.ToString()); }
                /* Resource Cleanup */
                finally { localFileStream.Close(); }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                responseStream.Close();
                ftpResponse.Close();
                ftpRequest = null;
            }
            return;
        }

        /* Get the list of filenames under the directory */
        public void GetDirFileList(string outputPath)
        {
            try
            {
                ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host);
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectoryDetails;

                ftpRequest.Credentials = new NetworkCredential(user, pass);

                /* Check App.config for other modifications for proxy error */
                /* my edit to solve Bad Gateway error ["http://www.codeproject.com/Questions/332730/FTP-proxy-problem-in-Csharp-application"] */
                ftpRequest.Proxy = null;

                ftpResponse = (FtpWebResponse)ftpRequest.GetResponse();
                responseStream = ftpResponse.GetResponseStream();

                // Write to file.
                //fileStream = File.Create(@"C:\AkaimaiLog\download\loglist.txt");
                fileStream = File.Create(outputPath);

                byte[] buffer = new byte[32 * 1024];
                int read;

                while ((read = responseStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    fileStream.Write(buffer, 0, read);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if (reader != null) reader.Close();
                if (fileStream != null) fileStream.Close();
                if (responseStream != null) responseStream.Close();
                if (ftpResponse != null) ftpResponse.Close();
                if (ftpRequest != null) ftpRequest = null;
            }
        }
    }
}
