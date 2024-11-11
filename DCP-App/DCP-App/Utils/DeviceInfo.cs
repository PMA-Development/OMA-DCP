using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DCP_App.Utils
{
    public static class DeviceInfo
    {
        public static string Id = "";
        public static readonly string Type = "DCP";
        public static string State = "On";
        public static string TurbineId = "";
        public static readonly List<string> Actions = new List<string> { "ChangeState" };
    }
}
