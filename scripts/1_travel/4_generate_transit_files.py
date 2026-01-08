## 4_generate_transit_files.py
## a translation of generate_transit_files.py (the second half)
## Author: npeterson
## Translated + Updated by ccai (2026)

import os
import sys
import shutil
import arcpy
import pandas as pd
from datetime import date
import math
import time

class EmmeTransitNetwork:

    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        in_folder = os.path.join(mfhrn_path, "input")
        years_csv_path = os.path.join(in_folder, "input_years.csv")
        scenario_df = pd.read_csv(years_csv_path)
        scenario_df["scenario"] = scenario_df["scenario"]//100
        self.scenario_dict = scenario_df.set_index("scenario")["year"].to_dict()

        self.bn_out_folder = os.path.join(self.mhn_out_folder, "bus_network")

        self.cbd_zones = set(range(1, 48))

    # MAIN METHOD ---------------------------------------------------------------------------------

    def generate_transit_files(self):

        print("Generating transit files...")

        scenario_dict = self.scenario_dict
        emme_transit_folder = os.path.join(self.mhn_out_folder, "transit")

        # wipe out the transit folder
        if os.path.isdir(emme_transit_folder) == True:
            shutil.rmtree(emme_transit_folder)
        os.mkdir(emme_transit_folder)

        for scen in scenario_dict:

            emme_scen_folder = os.path.join(emme_transit_folder, f"{scen}00")
            os.mkdir(emme_scen_folder)

            self.write_bus_files(scen, emme_scen_folder)

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that writes the bus-specific files
    def write_bus_files(self, scen, folder_path):

        print(f"Writing bus files for scenario {scen}00...")

        today = date.today().strftime("%d%b%y").upper()

        scen_gdb = os.path.join(self.bn_out_folder, f"SCENARIO_{scen}.gdb")

        for tod in [1, 2, 3, 4]:

            line_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"scen_line_{tod}")
            itin_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"scen_itin_{tod}")

            line_fields = ["TRANSIT_LINE", "DESCRIPTION", "MODE", "VEHICLE_TYPE", 
                           "HEADWAY", "SPEED"]
            
            line_df = pd.DataFrame(
                data = [row for row in arcpy.da.SearchCursor(line_fc, line_fields)], 
                columns = line_fields)
            
            line_dict = line_df.set_index("TRANSIT_LINE").to_dict("index")

            itin_fields = ["TRANSIT_LINE", "ITIN_ORDER", "ITIN_A", "ITIN_B",
                           "DWELL_CODE", "LINE_SERV_TIME", "TTF"]
            
            itin_df = pd.DataFrame(
                data = [row for row in arcpy.da.SearchCursor(itin_fc, itin_fields)], 
                columns = itin_fields
            ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

            itin_dict = {k: v.to_dict(orient='records') for k, v in itin_df.groupby("TRANSIT_LINE")}

            bus_itin_file_path = os.path.join(folder_path, f"bus.itinerary_{tod}")
            bus_itin_file = open(bus_itin_file_path, "a")

            bus_itin_file.write(f"c BUS TRANSIT BATCHIN FILE FOR SCENARIO {scen}00 TOD {tod}\n")
            bus_itin_file.write(f"c {today}\n")
            bus_itin_file.write("c us1 holds segment travel time, us2 holds zone fare\n")
            bus_itin_file.write("t lines\n")

            for line in line_dict:

                # no corresponding itinerary - skip
                if line not in itin_dict:

                    continue

                # write header
                mode = line_dict[line]["MODE"]
                veh_type = line_dict[line]["VEHICLE_TYPE"]

                # adjust headway
                hdwy = line_dict[line]["HEADWAY"]

                hdwy = round(hdwy, 1)
                
                if hdwy.is_integer():
                    hdwy = int(hdwy)
                else:
                    if hdwy >= 100:
                        hdwy = int(hdwy)

                speed = line_dict[line]["SPEED"]

                desc = line_dict[line]["DESCRIPTION"]
                desc = desc[0:20]

                if len(desc) < 20:

                    desc = desc + ' ' * (20- len(desc))

                header = f"a  '{line}'   {mode}   {veh_type}"
                header += f"   {hdwy}   {speed}   '{desc}'\n"
                bus_itin_file.write(header)
                bus_itin_file.write("    path=no\n")

                itin = itin_dict[line]

                for index, record in enumerate(itin):

                    if index == 0:

                        bus_itin_file.write(f"    dwt=0.01\n    ")
                        itin_a = str(record["ITIN_A"])
                        itin_a = itin_a + ' ' * (5 - len(itin_a))
                        bus_itin_file.write(itin_a)

                    # dwell code

                    dwc =  record["DWELL_CODE"]
                    # make sure last stop is stop 
                    if index == len(itin) - 1:
                        dwc = "0"

                    dwt = ""
                    if dwc == "0":
                        dwt += "0.01"
                    elif dwc == "1":
                        dwt += "#0  "

                    # ttf 

                    ttf = record["TTF"]
                    ettf = ""

                    if ttf == "0" or ttf == "1":
                        ettf += "1"
                    else:
                        ettf += "2"

                    # line service time

                    lst = record["LINE_SERV_TIME"]

                    lst = round(lst, 1)
                    lst = max(lst, 0.1) # make sure it's at least 0.1 minutes
                    if lst.is_integer():
                        lst = int(lst)
                    else:
                        if lst >= 10:
                            lst = int(lst)

                    lst = str(lst)

                    if len(lst) < 3:
                        lst = lst + ' ' * (3 - len(lst))

                    itin_b = str(record["ITIN_B"])
                    itin_b = itin_b + ' ' * (5 - len(itin_b))

                    bus_itin_file.write(f"   dwt={dwt}   ttf={ettf}   us1={lst}    us2=0\n    {itin_b}")

                    if index == len(itin) - 1:
                        bus_itin_file.write(f"   lay=3\n")

            bus_itin_file.close()

start_time = time.time()

ETN = EmmeTransitNetwork()
ETN.generate_transit_files()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")

print("Done")