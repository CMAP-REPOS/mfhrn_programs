## ETN.py
## a combination of:
## 1) MHN.py
## 2) generate_highway_files_2.sas

## Author: npeterson
## Translated by ccai (2025)

import os
import sys
import shutil
import arcpy
import pandas as pd
from datetime import date

class EmmeTravelNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        in_folder = os.path.join(mfhrn_path, "input")
        years_csv_path = os.path.join(in_folder, "input_years.csv")

        self.years_list = pd.read_csv(years_csv_path)["year"].to_list()
        self.years_dict = pd.read_csv(years_csv_path).set_index("year")["scenario"].to_dict()

        # 212 + 221 are not in here bc of TOD restrictions
        self.hwymode_dict = {}
        self.hwymode_dict["ASH"] = [
            "201", "218"]
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

    # MAIN METHODS --------------------------------------------------------------------------------

    # method that generates files for input into EMME
    def generate_hwy_files(self):

        print("Generating highway files...")

        years_list = self.years_list
        years_dict = self.years_dict

        arcpy.env.workspace = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        emme_hwy_folder = os.path.join(self.mhn_out_folder, "highway")
        emme_ls_folder = os.path.join(self.mhn_out_folder, "linkshape")

        if os.path.isdir(emme_hwy_folder) == True:
            shutil.rmtree(emme_hwy_folder)
        os.mkdir(emme_hwy_folder)

        if os.path.isdir(emme_ls_folder) == True:
            shutil.rmtree(emme_ls_folder)
        os.mkdir(emme_ls_folder)

        for year in years_list:

            scenario = years_dict[year]

            emme_scen_folder = os.path.join(emme_hwy_folder, str(scenario))
            os.mkdir(emme_scen_folder)

            self.write_ln_files(year, emme_scen_folder)
            self.write_linkshape_file(year, emme_ls_folder)

        print("Highway files generated.\n")

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that creates records of directional links
    def create_directional_records(self, hwylink_fc):

        link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if (f.type!="Geometry")]
        where_clause = "NEW_BASELINK = '1'"
        lf_dict = {field: index for index, field in enumerate(link_fields)}

        hwylink_records = []

        with arcpy.da.SearchCursor(hwylink_fc, link_fields, where_clause) as scursor:
            for row in scursor:
                
                dirs = row[lf_dict["DIRECTIONS"]]

                common_attr_dict = {
                    "sigic": row[lf_dict["SIGIC"]],
                    "cltl": row[lf_dict["CLTL"]],
                    "rrgradex": row[lf_dict["RRGRADECROSS"]],
                    "toll": row[lf_dict["TOLLDOLLARS"]],
                    "modes": row[lf_dict["MODES"]],
                    "vclearance": row[lf_dict["VCLEARANCE"]],
                    "blvd" : row[lf_dict["CHIBLVD"]],
                    "miles": row[lf_dict["MILES"]],
                    "proj": row[lf_dict["PROJECT"]]
                }

                attr_dict = {
                    "inode" : row[lf_dict["ANODE"]],
                    "jnode" : row[lf_dict["BNODE"]],
                    "type": row[lf_dict["TYPE1"]],
                    "ampm": row[lf_dict["AMPM1"]],
                    "speed": row[lf_dict["POSTEDSPEED1"]],
                    "lanes": row[lf_dict["THRULANES1"]],
                    "feet": row[lf_dict["THRULANEWIDTH1"]],
                    "parklanes": row[lf_dict["PARKLANES1"]],
                    "parkres": row[lf_dict["PARKRES1"]]
                } | common_attr_dict

                hwylink_records.append(attr_dict)

                if dirs == "2":

                    # parkres is coded separately
                    rev_dict = {
                        "inode" : row[lf_dict["BNODE"]],
                        "jnode" : row[lf_dict["ANODE"]],
                        "type": row[lf_dict["TYPE1"]],
                        "ampm": row[lf_dict["AMPM1"]],
                        "speed": row[lf_dict["POSTEDSPEED1"]],
                        "lanes": row[lf_dict["THRULANES1"]],
                        "feet": row[lf_dict["THRULANEWIDTH1"]],
                        "parklanes": row[lf_dict["PARKLANES1"]],
                        "parkres": row[lf_dict["PARKRES2"]]
                    } | common_attr_dict

                    hwylink_records.append(rev_dict)

                elif dirs == "3":

                    rev_dict = {
                        "inode" : row[lf_dict["BNODE"]],
                        "jnode" : row[lf_dict["ANODE"]],
                        "type": row[lf_dict["TYPE2"]],
                        "ampm": row[lf_dict["AMPM2"]],
                        "speed": row[lf_dict["POSTEDSPEED2"]],
                        "lanes": row[lf_dict["THRULANES2"]],
                        "feet": row[lf_dict["THRULANEWIDTH2"]],
                        "parklanes": row[lf_dict["PARKLANES2"]],
                        "parkres": row[lf_dict["PARKRES2"]]
                    } | common_attr_dict

                    hwylink_records.append(rev_dict)

        return hwylink_records

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
        hwylink_records = self.create_directional_records(hwylink_fc)
        hwylink_df = pd.DataFrame(hwylink_records).sort_values(["inode", "jnode"])
        hwylink_df = hwylink_df[hwylink_df.modes != "4"] # 4 is only for transit networks

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
            
            hwylink_df = hwylink_df[hwylink_df.ampm.isin(ampm_links)]
            hwylink_dict = hwylink_df.set_index(["inode", "jnode"]).to_dict("index")
            node_set = set(hwylink_df.inode.to_list()) | set(hwylink_df.jnode.to_list())

            # LINKS
            l1_file_path = os.path.join(folder_path, f"{scenario}0{tod}.l1")
            l2_file_path = os.path.join(folder_path, f"{scenario}0{tod}.l2")
            l1_file = open(l1_file_path, "a")
            l1_file.write("c a,i-node,j-node,length,modes,type,lanes,vdf\n")
            l1_file.write("t links init\n")

            l2_file = open(l2_file_path, "a")
            l2_file.write("c i-node,j-node,@speed,@width,@parkl,@cltl,@toll,@sigic,@rrx,@tipid\n")

            for link in hwylink_dict:

                inode = link[0]
                jnode = link[1]

                len_str_inode = len(str(inode))
                len_str_jnode = len(str(jnode))

                space1 = " " * (7 - len_str_inode)
                space2 = " " * (7 - len_str_jnode)

                miles = hwylink_dict[link]["miles"]
                length = round(miles, 2)

                # calculate mode
                mode = hwylink_dict[link]["modes"]
                blvd = hwylink_dict[link]["blvd"]
                vclearance = hwylink_dict[link]["vclearance"]

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
                lanes = hwylink_dict[link]["lanes"]
                parklanes = hwylink_dict[link]["parklanes"]
                parkres = hwylink_dict[link]["parkres"]

                if str(tod) in parkres:
                    lanes += parklanes
                    parklanes = 0

                # vdf
                vdf = hwylink_dict[link]["type"]

                l1_file.write(f"a{space1}{inode}{space2}{jnode} {length} ")
                l1_file.write(f"{emode}{space3} 1 {lanes}  {vdf}\n")

                # extra attributes
                space1 = " " * (6 - len_str_inode)

                speed = hwylink_dict[link]["speed"]
                width = hwylink_dict[link]["feet"]
                cltl = hwylink_dict[link]["cltl"]

                # toll
                toll = hwylink_dict[link]["toll"]
                dist_fact = miles if vdf != "7" else 1

                tod_toll = 0

                try:
                    static_toll = float(toll)

                    if static_toll != 0:
                        tod_toll = round(static_toll * dist_fact, 2)
                except:
                    dynamic_toll = float(toll.split()[tod - 1])
                    # if TOD is 0 - treat like TOD 8
                    tod_toll = round(dynamic_toll * dist_fact, 2)

                sigic = hwylink_dict[link]["sigic"]
                rrx = hwylink_dict[link]["rrgradex"]
                proj = hwylink_dict[link]["proj"]

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

    # helper method that writes linkshape file
    def write_linkshape_file(self, year, folder_path):

        scenario = self.years_dict[year]

        print(f"Writing linkshape files for scenario {scenario}...\n")
        today = date.today().strftime("%d%b%y").upper()

        linkshape_file_path = os.path.join(folder_path, f"linkshape_{scenario}.in")
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