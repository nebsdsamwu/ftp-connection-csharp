using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FTP01
{
    public class AkamaiLogEntity
    {
        public string IP { get; set; }
        public DateTimeOffset LogTimeUTC { get; set; }
        public string RequestString { get; set; }
        public string Method { get; set; }
        public string URL { get; set; }
        public string ResponseCode { get; set; }
        public string Size { get; set; }
        public string UserAgentString { get; set; }
        public string AllLogString { get; set; }

        public AkamaiLogEntity() { }

        public override string ToString()
        {
            return string.Format("{0}-###-{1}-###-{2}-###-{3}-###-{4}-###-{5}-###-{6}", IP, LogTimeUTC, RequestString, Method, URL, ResponseCode, Size);
        }
    }
}
