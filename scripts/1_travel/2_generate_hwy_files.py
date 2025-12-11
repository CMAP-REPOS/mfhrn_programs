## 2_generate_hwy_files.py
## a translation of generate_highway_files.py
## Author: npeterson
## Translated + Updated by ccai (2025)

import os
import sys
import shutil
import arcpy
import pandas as pd
from datetime import date
import math
import time

from modules.util_functions import create_directional_hwy_records

class EmmeHighwayNetwork:

    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        in_folder = os.path.join(mfhrn_path, "input")
        years_csv_path = os.path.join(in_folder, "input_years.csv")

        self.years_dict = pd.read_csv(years_csv_path).set_index("year")["scenario"].to_dict()

        # 212 + 221 are not in here bc of TOD restrictions
        self.hwymode_dict = {}
        self.hwymode_dict["ASH"] = ["201", "218"]
        self.hwymode_dict["ASHTb"] = [
            "202", "203", "204", "209", "210", "211", 
            "213", "225", "235", "237"]
        self.hwymode_dict["ASHTlb"] = [
            "207", "208", "214", "216", "217", "219",
            "227", "229", "231", "234", "238", "239",
            "240", "241", "242", "243", "244", "246",
            "247", "249"]
        self.hwymode_dict["ASHTmlb"] = [
            "205", "230", "245", "248"]
        self.hwymode_dict["AThmlb"] = ["300"]
        self.hwymode_dict["AH"] = ["500"]

    # MAIN METHOD ---------------------------------------------------------------------------------

    def generate_hwy_files(self):

        print("Generating highway files...")

        years_dict = self.years_dict
        arcpy.env.workspace = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        emme_hwy_folder = os.path.join(self.mhn_out_folder, "highway")

        # wipe out the highway folder
        if os.path.isdir(emme_hwy_folder) == True:
            shutil.rmtree(emme_hwy_folder)
        os.mkdir(emme_hwy_folder)

        for year in years_dict:

            scenario = years_dict[year]

            emme_scen_folder = os.path.join(emme_hwy_folder, str(scenario))
            os.mkdir(emme_scen_folder)

            self.write_ln_files(year, emme_scen_folder)
            self.write_linkshape_file(year, emme_scen_folder)

        print("Highway files generated.\n")

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that writes highway link and node files
    def write_ln_files(self, year, folder_path):

        scenario = self.years_dict[year]

        print(f"Writing link and node files for scenario {scenario}...")

        hwynode_fc = "hwynode_all"
        node_fields = [f.name for f in arcpy.ListFields(hwynode_fc) if (f.type!="Geometry")]
        node_fields += ["SHAPE@X", "SHAPE@Y"]
        hwynode_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode_fc, node_fields)], 
            columns = node_fields)
        
        max_zone_set = set(hwynode_df.zone17.to_list())
        max_zone_set.remove(9999)
        max_zone = max(max_zone_set)

        hwynode_dict = hwynode_df.set_index("NODE").to_dict("index")

        hwylink_fc = f"HWYLINK_{year}"
        hwylink_records = create_directional_hwy_records(hwylink_fc, 
                                                         where_clause = "NEW_BASELINK = '1'")
        
        hwylink_df = pd.DataFrame(hwylink_records).sort_values(["INODE", "JNODE"])
        hwylink_df = hwylink_df[hwylink_df.MODES != "400"] # 400 is only for transit networks

        for tod in list(range(0, 9)):

            ampm_links = []

            # general 
            if tod == 0: 
                ampm_links += ["1", "2", "3", "4", "5"]
            # overnight
            elif tod == 1: 
                ampm_links += ["1", "3", "4"]
            # AM peak + shoulder
            elif tod in [2, 3, 4]: 
                ampm_links += ["1", "2", "5"]
            # midday
            elif tod == 5:
                ampm_links += ["1", "2", "4"]
            # PM peak and shoulder
            elif tod in [6, 7, 8]:
                ampm_links += ["1", "3", "5"]

            hwylink_tod_df = hwylink_df[hwylink_df.AMPM.isin(ampm_links)]
            hwylink_tod_dict = hwylink_tod_df.set_index(["INODE", "JNODE"]).to_dict("index")
            node_set = set(hwylink_tod_df.INODE.to_list()) | set(hwylink_tod_df.JNODE.to_list())

            l1_file_path = os.path.join(folder_path, f"{scenario}0{tod}.l1")
            l2_file_path = os.path.join(folder_path, f"{scenario}0{tod}.l2")
            l1_file = open(l1_file_path, "a")
            l1_file.write("c a,i-node,j-node,length,modes,type,lanes,vdf\n")
            l1_file.write("t links init\n")

            l2_file = open(l2_file_path, "a")
            l2_file.write("c i-node,j-node,@speed,@width,@parkl,@cltl,@toll,@sigic,@rrx,@tipid\n")

            for link in hwylink_tod_dict:

                inode = link[0]
                jnode = link[1]

                len_str_inode = len(str(inode))
                len_str_jnode = len(str(jnode))

                space1 = " " * (7 - len_str_inode)
                space2 = " " * (7 - len_str_jnode)

                miles = hwylink_tod_dict[link]["MILES"]
                length = round(miles, 2)

                # calculate mode
                mode = hwylink_tod_dict[link]["MODES"]
                blvd = hwylink_tod_dict[link]["CHIBLVD"]
                vclearance = hwylink_tod_dict[link]["VCLEARANCE"]

                emode = "ASHThmlb" # default

                # if constant restriction
                for res_type in self.hwymode_dict:
                    if mode in self.hwymode_dict[res_type]:
                        emode = res_type

                # if overnight restriction
                if mode == "212" and tod == 1:
                    emode = "ASHTb"
                if mode == "221" and tod == 1:
                    emode = "ASH"

                # if part of chicago boulevard
                if blvd == 1:
                    emode = "ASH" # no trucks allowed

                # if there is a vehicle clearance
                if vclearance != 0:
                    if vclearance < 162:
                        emode = emode.replace("h", "") # minimum 13'6" clearance for heavy trucks
                    if vclearance < 150:
                        emode = emode.replace("m", "") # minimum 12'6" clearance for medium trucks
                    if vclearance < 138:
                        emode = emode.replace("l", "") # minimum 11'6" clearance for light trucks

                space3 = " " * (8 - len(emode))

                # calculate # of lanes
                lanes = hwylink_tod_dict[link]["THRULANES"]
                parklanes = hwylink_tod_dict[link]["PARKLANES"]
                parkres = hwylink_tod_dict[link]["PARKRES"]

                if str(tod) in parkres:
                    lanes += parklanes
                    parklanes = 0

                # vdf
                vdf = hwylink_tod_dict[link]["TYPE"]

                l1_file.write(f"a{space1}{inode}{space2}{jnode} {length} ")
                l1_file.write(f"{emode}{space3} 1 {lanes}  {vdf}\n")

                # extra attributes
                space1 = " " * (6 - len_str_inode)

                speed = hwylink_tod_dict[link]["POSTEDSPEED"]
                width = hwylink_tod_dict[link]["THRULANEWIDTH"]
                cltl = hwylink_tod_dict[link]["CLTL"]

                # toll
                toll = hwylink_tod_dict[link]["TOLLDOLLARS"]
                dist_fact = miles if vdf != "7" else 1

                tod_toll = 0

                try:
                    static_toll = float(toll)

                    if static_toll != 0:
                        tod_toll = round(static_toll * dist_fact, 2)
                except:
                    # if TOD is 0 - treat like TOD 8
                    dynamic_toll = float(toll.split()[tod - 1])
                    
                    tod_toll = round(dynamic_toll * dist_fact, 2)

                sigic = hwylink_tod_dict[link]["SIGIC"]
                rrx = hwylink_tod_dict[link]["RRGRADECROSS"]
                proj = hwylink_tod_dict[link]["PROJECT"]

                tipid = "0"

                if proj != None:
                    tipid = proj.lstrip("0").replace("-", "")

                l2_file.write(f"{space1}{inode}{space2}{jnode} ")
                l2_file.write(f"{speed}  {width}  {parklanes}  {cltl}  ")
                l2_file.write(f"{tod_toll}  {sigic}  {rrx}  {tipid}\n")

            l1_file.close()
            l2_file.close()

            # NODES
            n1_file_path = os.path.join(folder_path, f"{scenario}0{tod}.n1")
            n2_file_path = os.path.join(folder_path, f"{scenario}0{tod}.n2")
            n1_file = open(n1_file_path, "a")
            n1_file.write("c a,node,x,y\n")
            n1_file.write("t nodes init\n")

            n2_file = open(n2_file_path, "a")
            n2_file.write("c i-node,@zone,@atype,@imarea\n")

            for node in node_set:

                a = "a" if node > max_zone else "a*"

                len_str_node = len(str(node))

                space1 = " " * (8 - len(a) - len_str_node)

                point_x = str(hwynode_dict[node]["SHAPE@X"])[0:12]
                point_y = str(hwynode_dict[node]["SHAPE@Y"])[0:12]

                n1_file.write(f"{a}{space1}{node} {point_x} {point_y}\n")

                zone = hwynode_dict[node]["zone17"]
                capzone = hwynode_dict[node]["capzone17"]
                imarea = hwynode_dict[node]["IMArea"]

                space0 = " " * (6- len_str_node)

                n2_file.write(f"{space0}{node} {zone}  {capzone}  {imarea}\n")

            n1_file.close()
            n2_file.close()

    # helper method that writes highway linkshape file
    def write_linkshape_file(self, year, folder_path):

        scenario = self.years_dict[year]

        print(f"Writing linkshape files for scenario {scenario}...\n")

        today = date.today().strftime("%d%b%y").upper()

        linkshape_file_path = os.path.join(folder_path, "highway.linkshape")
        linkshape_file = open(linkshape_file_path, "a")

        linkshape_file.write(f"c HIGHWAY LINK SHAPE FILE FOR SCENARIO {scenario}\n")
        linkshape_file.write(f"c {today}\n")
        linkshape_file.write("t linkvertices\n")

        hwylink_fc = f"HWYLINK_{year}"
        fields = ["ANODE", "BNODE", "DIRECTIONS", "SHAPE@"]

        where_clause = "NEW_BASELINK = '1'"
        with arcpy.da.SearchCursor(hwylink_fc, fields, where_clause) as scursor:
            for row in scursor:
                
                anode = row[0]
                bnode = row[1]
                dirs = row[2]

                point_list = []

                for part in row[3]:
                    for point in part:

                        point_list.append((point.X, point.Y))

                linkshape_file.write(f"r {anode} {bnode}\n")
                for i in range(0, len(point_list)):

                    point= point_list[i]
                    x = point[0]
                    y = point[1]

                    point_string = f"a {anode} {bnode} {i + 1} {x} {y}\n"
                    linkshape_file.write(point_string)

                if dirs == "1":
                    continue

                linkshape_file.write(f"r {bnode} {anode}\n")
                for i in range(1, len(point_list) + 1):

                    point = point_list[-i]
                    x = point[0]
                    y = point[1]

                    point_string = f"a {bnode} {anode} {i} {x} {y}\n"
                    linkshape_file.write(point_string)

        linkshape_file.close()

start_time = time.time()

EHN = EmmeHighwayNetwork()
EHN.generate_hwy_files()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")

print("Done")