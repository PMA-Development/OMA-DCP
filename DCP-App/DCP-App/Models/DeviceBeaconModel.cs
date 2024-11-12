using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DCP_App.Models
{
    public class DeviceBeaconModel
    {
        public string Id { get; set; } = "";
        public string Type { get; set; } = "DCP";
        public int CollectionInterval { get; set; }
        public string State { get; set; } = "On";
        public string TurbineId { get; set; } = "";
        public string IslandId { get; set; } = "";
    }
}
