## EN.py
## a combination of:
## 1) generate_highway_files_2.sas

## Author: npeterson
## Translated by ccai (2025)

import os
import sys
import shutil
import arcpy
import pandas as pd
from datetime import date

class EmmeNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_MHN")

        years_csv_path = os.path.join(self.in_folder, "input_years.csv")

        self.years_list = pd.read_csv(years_csv_path)["year"].to_list()
        self.years_dict = pd.read_csv(years_csv_path).set_index("year")["scenario"].to_dict()

    # MAIN METHODS --------------------------------------------------------------------------------

    # method that generates files for input into EMME
    def generate_hwy_files(self):

        print("Generating highway files...")

        years_list = self.years_list
        years_dict = self.years_dict

        arcpy.env.workspace = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        emme_hwy_folder = os.path.join(self.mhn_out_folder, "highway")

        if os.path.isdir(emme_hwy_folder) == True:
            shutil.rmtree(emme_hwy_folder)
        os.mkdir(emme_hwy_folder)

        for year in years_list:

            scenario = years_dict[year]

            emme_scen_folder = os.path.join(emme_hwy_folder, str(scenario))
            os.mkdir(emme_scen_folder)


            self.write_ln_files(year, emme_scen_folder)
            self.write_linkshape_file(year, emme_scen_folder)

        print("Highway files generated.\n")

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that creates records of directional links for emme
    def create_directional_records(self, hwylink_fc):

        link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if (f.type!="Geometry")]
        where_clause = "NEW_BASELINK = '1'"
        lf_dict = {field: index for index, field in enumerate(link_fields)}

        hwylink_records = []

        with arcpy.da.SearchCursor(hwylink_fc, link_fields, where_clause) as scursor:
            for row in scursor:
                
                dirs = row[lf_dict["DIRECTIONS"]]

                attr_dict = {
                    "fnode" : row[lf_dict["ANODE"]],
                    "tnode" : row[lf_dict["BNODE"]],
                    "type": row[lf_dict["TYPE1"]],
                    "ampm": row[lf_dict["AMPM1"]],
                    "speed": row[lf_dict["POSTEDSPEED1"]],
                    "lanes": row[lf_dict["THRULANES1"]],
                    "feet": row[lf_dict["THRULANEWIDTH1"]],
                    "parklanes": row[lf_dict["PARKLANES1"]],
                    "parkres": row[lf_dict["PARKRES1"]],
                    "sigic": row[lf_dict["SIGIC"]],
                    "toll": row[lf_dict["TOLLDOLLARS"]],
                    "modes": row[lf_dict["MODES"]],
                    "truckres": row[lf_dict["TRUCKRES"]],
                    "vclearance": row[lf_dict["VCLEARANCE"]]
                }

                hwylink_records.append(attr_dict)

                if dirs == "2":

                    rev_dict = {
                        "fnode" : row[lf_dict["BNODE"]],
                        "tnode" : row[lf_dict["ANODE"]],
                        "type": row[lf_dict["TYPE1"]],
                        "ampm": row[lf_dict["AMPM1"]],
                        "speed": row[lf_dict["POSTEDSPEED1"]],
                        "lanes": row[lf_dict["THRULANES1"]],
                        "feet": row[lf_dict["THRULANEWIDTH1"]],
                        "parklanes": row[lf_dict["PARKLANES1"]],
                        "parkres": row[lf_dict["PARKRES2"]],
                        "sigic": row[lf_dict["SIGIC"]],
                        "toll": row[lf_dict["TOLLDOLLARS"]],
                        "modes": row[lf_dict["MODES"]],
                        "truckres": row[lf_dict["TRUCKRES"]],
                        "vclearance": row[lf_dict["VCLEARANCE"]]
                    }

                    hwylink_records.append(rev_dict)

                elif dirs == "3":

                    rev_dict = {
                        "fnode" : row[lf_dict["BNODE"]],
                        "tnode" : row[lf_dict["ANODE"]],
                        "type": row[lf_dict["TYPE2"]],
                        "ampm": row[lf_dict["AMPM2"]],
                        "speed": row[lf_dict["POSTEDSPEED2"]],
                        "lanes": row[lf_dict["THRULANES2"]],
                        "feet": row[lf_dict["THRULANEWIDTH2"]],
                        "parklanes": row[lf_dict["PARKLANES2"]],
                        "parkres": row[lf_dict["PARKRES2"]],
                        "sigic": row[lf_dict["SIGIC"]],
                        "toll": row[lf_dict["TOLLDOLLARS"]],
                        "modes": row[lf_dict["MODES"]],
                        "truckres": row[lf_dict["TRUCKRES"]],
                        "vclearance": row[lf_dict["VCLEARANCE"]]
                    }

                    hwylink_records.append(rev_dict)

        return hwylink_records

    # helper method that writes link and node files
    def write_ln_files(self, year, folder_path):
        
        scenario = self.years_dict[year]
        print(f"Writing link and node files for scenario {scenario}...")

        hwynode_fc = "hwynode_all"
        node_fields = [f.name for f in arcpy.ListFields(hwynode_fc) if (f.type!="Geometry")]
        node_fields += ["SHAPE@X", "SHAPE@Y"]
        hwynode_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode_fc, node_fields)], 
            columns = node_fields)
        
        max_zone_list = hwynode_df.zone17.to_list()
        max_zone_list.remove(9999)
        max_zone = max_zone_list.max()
        
        hwynode_dict = hwynode_df.set_index("NODE").to_dict("index")

        hwylink_fc = f"HWYLINK_{year}"
        hwylink_records = self.create_directional_records(hwylink_fc)
        hwylink_df = pd.DataFrame(hwylink_records)

        for tod in ["0"]:

            ampm_links = []

            if tod == "0":
                ampm_links += ["1", "2", "3", "4"]
            elif tod in ["2", "3", "4"]:
                ampm_links += ["1", "2"]
            elif tod == "5":
                ampm_links += ["1", "2", "4"]
            elif tod in ["6", "7", "8"]:
                ampm_links += ["1", "3"]
            elif tod == "1":
                ampm_links += ["1", "3", "4"]

            hwylink_df = hwylink_df[hwylink_df.ampm.isin(ampm_links)]
            node_set = set(hwylink_df.fnode.to_list()) | set(hwylink_df.tnode.to_list())

            n1_file_path = os.path.join(folder_path, f"{scenario}0{tod}.n1")
            n2_file_path = os.path.join(folder_path, f"{scenario}0{tod}.n2")
            n1_file = open(n1_file_path, "a")
            n2_file = open(n2_file_path, "a")

            for node in node_set:

                point_x = hwynode_dict[node]["SHAPE@X"]
                point_y = hwynode_dict[node]["SHAPE@Y"]

            n1_file.close()
            n2_file.close()

    # helper method that writes linkshape file
    def write_linkshape_file(self, year, folder_path):

        scenario = self.years_dict[year]

        print(f"Writing linkshape files for scenario {scenario}...")
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
                for index, point in enumerate(point_list):

                    x = point[0]
                    y = point[1]
                    point_string = f"a {anode} {bnode} {index + 1} {x} {y}\n"
                    linkshape_file.write(point_string)

                if dirs == "1":
                    continue

                linkshape_file.write(f"r {bnode} {anode}\n")
                for index, point in enumerate(reversed(point_list)):

                    x = point[0]
                    y = point[1]
                    point_string = f"a {bnode} {anode} {index + 1} {x} {y}\n"
                    linkshape_file.write(point_string)

        linkshape_file.close()
