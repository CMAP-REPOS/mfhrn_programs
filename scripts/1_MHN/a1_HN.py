## a1_HN.py 
## combination of:
## 1) MHN.py, 
## 2) process_highway_coding.sas,
## 3) coding_overlap.sas,
## 4) generate_highway_files_2.sas, and
## 5) import_highway_projects_2.sas

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd
import time

class HighwayNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
        
        self.in_folder = os.path.join(mfhrn_path, "input")
        self.in_gdb = os.path.join(self.in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_MHN")
        
        years_csv_path = os.path.join(self.in_folder, "input_years.csv")
        self.years_list = pd.read_csv(years_csv_path)["years"].to_list()

        # highway files - names of feature classes + tables in MHN
        self.hwy_files = [
            "hwynet/hwynet_arc",
            "hwynet/hwynet_node",
            "hwynet/hwyproj",
            "hwyproj_coding"
            ]
        
        # bus files - names of feature classes + tables in MHN
        # okay we need to do something about this situation
        self.bus_files = [
            "hwynet/bus_base",
            "hwynet/bus_current_2016",
            "hwynet/bus_current_2024",
            "hwynet/bus_future_2016",
            "hwynet/bus_future_2024",
            "bus_base_itin",
            "bus_current_itin_2016",
            "bus_current_itin_2024",
            "bus_future_itin_2016",
            "bus_future_itin_2024",
            "parknride" # not totally sure what this is. 
        ]
        
        # relationship classes in MHN 
        self.rel_classes = [
            "rel_arcs_to_bus_base_itin",
            "rel_arcs_to_bus_current_itin_2024",
            "rel_arcs_to_bus_future_itin_2024",
            "rel_arcs_to_hwyproj_coding",
            "rel_bus_base_to_itin",
            "rel_bus_current_itin_to_2024",
            "rel_bus_future_itin_to_2024",
            "rel_hwyproj_to_coding",
            "rel_nodes_to_parknride"
        ]
        
        self.current_gdb = self.in_gdb # as right now we're in the input folder 

        self.hwylink_df = None
        self.hwynode_df = None
        self.hwyproj_df = None
        self.hwyproj_years_df = None

        self.get_hwy_dfs()

        self.base_year = min(self.hwyproj_years_df.COMPLETION_YEAR.to_list()) - 1

    # helper function to get dfs 
    def get_hwy_dfs(self):

        hwylink = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwylink_fields = [f.name for f in arcpy.ListFields(hwylink) if f.type!="Geometry"]
        self.hwylink_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwylink, hwylink_fields)], 
            columns = hwylink_fields)
        
        hwynode = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode) if f.type != "Geometry"]
        self.hwynode_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode, hwynode_fields)], 
            columns = hwynode_fields)

        hwyproj = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_fields = [f.name for f in arcpy.ListFields(hwyproj)]
        self.hwyproj_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwyproj, hwyproj_fields)], 
            columns = hwyproj_fields)
        
        hwyproj_years = os.path.join(self.current_gdb, "hwynet/hwyproj")
        hwyproj_years_fields = [f.name for f in arcpy.ListFields(hwyproj_years) if f.type != "Geometry"]
        self.hwyproj_years_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwyproj_years, hwyproj_years_fields)],
            columns = hwyproj_years_fields)
        
    # helper function to delete relationship classes
    def del_rcs(self):

        for rc in self.rel_classes:
            rc_path = os.path.join(self.current_gdb, rc)
            arcpy.management.Delete(rc_path)

    # helper function to delete bus files
    def del_bus(self):

        for bus_file in self.bus_files:
            bus_path = os.path.join(self.current_gdb, bus_file)
            arcpy.management.Delete(bus_path)
    
    # function that generates base year gdb
    def generate_base_year(self):

        print("Copying base year...")

        # delete output folder + recreate it 
        mhn_out_folder = self.mhn_out_folder
        out_folder = os.path.dirname(mhn_out_folder)
        in_gdb = self.in_gdb
        base_year = self.base_year 

        if os.path.isdir(out_folder) == True:
            shutil.rmtree(out_folder)
        
        os.mkdir(out_folder)
        os.mkdir(mhn_out_folder)

        # copy GDB
        out_gdb = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        arcpy.management.Copy(in_gdb, out_gdb)
        self.current_gdb = out_gdb # update the HN's current gdb 

        self.del_rcs()
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_year_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")

        # to make my life easier - add a field with REP-ABB to the project df
        arcpy.management.AddField(hwyproj_coding_table, "REP_ABB", "TEXT") # to make my life easier 
        arcpy.management.CalculateField(
            in_table= hwyproj_coding_table,
            field="REP_ABB", expression="rep_abb(!REP_ANODE!, !REP_BNODE!)",
            expression_type="PYTHON3",
            code_block="""def rep_abb(rep_anode, rep_bnode):
            return str(rep_anode) + "-" + str(rep_bnode) + "-1" """,
            field_type="TEXT",
            enforce_domains="NO_ENFORCE_DOMAINS")
        
        arcpy.management.JoinField(hwyproj_coding_table, "TIPID", hwyproj_year_fc, "TIPID", "COMPLETION_YEAR")
        arcpy.management.AddFields(hwyproj_coding_table, [["USE", "SHORT"], ["PROCESS_NOTES", "TEXT"]])
        arcpy.management.CalculateField(hwyproj_coding_table, "USE", "1")

        # add fields to review updates (a la Volpe)
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        arcpy.management.AddFields(hwylink_fc, [["NEW_BASELINK", "TEXT"], ["PROJECT", "TEXT"], ["DESCRIPTION", "TEXT"]])
        arcpy.management.CalculateField(
            in_table= hwylink_fc,
            field="NEW_BASELINK",
            expression="!BASELINK!",
            expression_type="PYTHON3",
            code_block="",
            field_type="TEXT",
            enforce_domains="NO_ENFORCE_DOMAINS")

        print("Base year copied and prepared for modification.\n")

    # function that checks base links + nodes
    def check_hwy_fcs(self):

        print("Checking feature classes for errors...")

        self.get_hwy_dfs()
        hwynode_df = self.hwynode_df
        hwylink_df = self.hwylink_df 
        hwyproj_years_df = self.hwyproj_years_df

        # nodes must be unique 
        node_counts = hwynode_df.NODE.value_counts()
        
        if (node_counts.max() > 1):
            print(node_counts[node_counts > 1])
            sys.exit("These nodes violate unique node ID constraint. Crashing program.")

        all_node_set = set(hwynode_df.NODE.to_list())
        link_node_set = set(hwylink_df.ANODE.to_list()) | set(hwylink_df.BNODE.to_list())

        # nodes should not be disconnected
        extra_nodes = all_node_set - link_node_set 
        
        if (len(extra_nodes) > 0):
            print(extra_nodes)
            sys.exit("These nodes are not connected to any links. Crashing program.") 

        # link anode / bnode must be in available nodes 
        invalid_nodes = link_node_set - all_node_set

        if (len(invalid_nodes) > 0):
            print(invalid_nodes)
            sys.exit("These nodes are not present in the node feature class. Crashing program.")

        # duplicate anode-bnode combinations are not allowed
        ab_count_df = hwylink_df.groupby(["ANODE", "BNODE"]).size().reset_index()
        ab_count_df = ab_count_df.rename(columns = {0: "group_size"})
        
        if (ab_count_df["group_size"].max() > 1):
            print(ab_count_df[ab_count_df[["group_size"]] >= 2])
            sys.exit("Violating unique ANODE-BNODE constraint. Crashing program.")

        # directional links must be valid
        hwylink_abb_df = hwylink_df[["ANODE", "BNODE", "ABB", "DIRECTIONS"]]
        hwylink_dup_df = pd.merge(hwylink_abb_df, hwylink_abb_df.copy(), left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        directions_set = set(hwylink_dup_df.DIRECTIONS_x.to_list()) | set(hwylink_dup_df.DIRECTIONS_y.to_list())
        
        if directions_set != {'1'}:
            print(hwylink_dup_df[(hwylink_dup_df.DIRECTIONS_x != '1') | (hwylink_dup_df.DIRECTIONS_y != '1')])
            sys.exit("Violating directional link combinations. Crashing program.")

        # TIPIDs should be unique 
        tipid_count_df = hwyproj_years_df.groupby(["TIPID"]).size().reset_index()
        tipid_count_df = tipid_count_df.rename(columns = {0:"group_size"})

        if (tipid_count_df["group_size"].max() > 1):
            print(tipid_count_df[tipid_count_df["group_size"] >= 2])
            sys.exit("Violating unique TIPID constraint. Crashing program.")

        print("Base feature classes checked for errors.\n")
        
    # function that checks the project table
    def check_hwy_project_table(self):

        print("Checking base project table for errors...")
        
        mhn_out_folder = self.mhn_out_folder
        self.get_hwy_dfs() # update the HN's current dfs 

        base_project_table_errors = os.path.join(
            mhn_out_folder, 
            "base_project_table_errors.txt")
        error_file= open(base_project_table_errors, "a") # open error file, don't forget to close it!

        # create hwylink data structures now to be compared to later
        hwylink_df = self.hwylink_df
        toll1_list = hwylink_df[(hwylink_df.TYPE1 == "7") & (hwylink_df.POSTEDSPEED1 == 0)].ABB.to_list()
        toll2_list = hwylink_df[(hwylink_df.TYPE2 == "7") & (hwylink_df.POSTEDSPEED2 == 0)].ABB.to_list()

        hwylink_abb_df = hwylink_df[["ANODE", "BNODE", "BASELINK", "ABB"]]
        hwylink_dup_df = pd.merge(hwylink_abb_df, hwylink_abb_df.copy(), left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        hwylink_dup_set = set(hwylink_dup_df.ABB_x.to_list() + hwylink_dup_df.ABB_y.to_list())

        # projects with year 9999 are not well maintained 
        # only check integrity if != 9999

        hwyproj_df = self.hwyproj_df[self.hwyproj_df.COMPLETION_YEAR != 9999]
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        # primary key check
        # check that no TIPID + ABB is duplicated. 
        hwyproj_group_df = hwyproj_df.groupby(["TIPID", "ABB"]).size().reset_index()
        hwyproj_group_df = hwyproj_group_df.rename(columns = {0: "group_size"})
        hwyproj_dup_dict = hwyproj_group_df[hwyproj_group_df["group_size"] > 1].to_dict("records")

        dup_set = set()
        for dup in hwyproj_dup_dict: 
            dup_set.add((dup["TIPID"], dup["ABB"]))

        # don't use the duplicates
        fields = ["TIPID", "ABB", "USE", "PROCESS_NOTES"]
        dup_fail = 0
        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, "COMPLETION_YEAR <> 9999") as ucursor:
            for row in ucursor:
                if (row[0], row[1]) in dup_set:
                    row[2] = 0
                    row[3] = "Error: Duplicate TIPID-ABB combination. Must be unique."
                    dup_fail+=1
                    ucursor.updateRow(row)
        
        error_file.write("Primary key check:\n")
        if dup_fail != 0:
            error_file.write(f"{dup_fail} rows failed the duplicate check and had USE set to 0. Check output gdb.\n\n")
        else:
            error_file.write("No rows failed the duplicate check.\n\n")

        # check row by row 
        # don't have to check the validity of already discarded rows
        # just count the rows with mistakes

        row_fail = 0
        row_warning = 0
        fields = [f.name for f in arcpy.ListFields(hwyproj_coding_table) if f.name != "OBJECTID"]
        where_clause = "COMPLETION_YEAR <> 9999 AND USE = 1"
        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, where_clause) as ucursor:
            for row in ucursor:
                tipid = row[0] 
                abb = row[21]
                # use = row[26], process_notes = row[27]

                # for some reason these are saved as text fields but they should only contain numbers
                try: 
                    action_code = int(row[1])
                    new_directions = int(row[2])
                    new_type1 = int(row[3]); new_type2 = int(row[4])
                    new_ampm1 = int(row[5]); new_ampm2 = int(row[6])
                    new_modes = int(row[19])
                except:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Non-numeric values in fields where it should be numeric."
                    ucursor.updateRow(row)
                    continue

                new_postedspeed1 = row[7]; new_postedspeed2 = row[8]
                new_thrulanes1 = row[9]; new_thrulanes2 = row[10]
                new_thrulanewidth1 = row[11]; new_thrulanewidth2 = row[12]
                add_parklanes1 = row[13]; add_parklanes2 = row[14]
                add_sigic = row[15]
                add_cltl = row[16]
                add_rrgradecross = row[17]
                new_tolldollars = row[18]
                rep_abb = row[24]

                # check that TIPID is valid 
                tipids = self.hwyproj_years_df.TIPID.to_list()
                if tipid not in tipids: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: TIPID is not a legitimate project."
                    ucursor.updateRow(row)
                    continue

                # check that ABB is valid
                abbs = self.hwylink_df.ABB.to_list()
                if abb not in abbs:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: ABB is not an actual link."
                    ucursor.updateRow(row)
                    continue

                # check that values are within range 
                if action_code not in [1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Action code is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_directions not in [0, 1, 2, 3]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Directions flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_type1 not in [0, 1, 2, 3, 4, 5, 6, 7, 8]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Type 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_type2 not in [0, 1, 2, 3, 4, 5, 6, 7, 8]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Type 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_ampm1 not in [0, 1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: AMPM 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_ampm2 not in [0, 1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: AMPM 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_postedspeed1 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Posted speed 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_postedspeed2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Posted speed 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanes1 < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Through lanes 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanes2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Through lanes 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanewidth1 < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Through lanes width 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanewidth2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Through lanes width 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_sigic not in [0, 1]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Sigic flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_cltl not in [-1, 0, 1]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Cltl flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_rrgradecross not in [-1, 0, 1]: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Railroad crossing flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_tolldollars < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Toll dollar flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_modes not in [0, 1, 2, 3, 4, 5]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Mode flag is not valid."
                    ucursor.updateRow(row)
                    continue

                # check that action codes are valid. 
                baselink = int(abb[-1])
                if baselink == 0 and action_code not in [2, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Skeleton links cannot have action codes 1 or 3 applied to them."
                    ucursor.updateRow(row)
                    continue
                if baselink == 1 and action_code not in [1,3]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Regular links cannot have action codes 2 or 4 applied to them."
                    ucursor.updateRow(row)
                    continue

                # check that REP_ANODE + REP_BNODE are only associated with Action Code 2 
                if action_code in [1, 3, 4] and rep_abb != "0-0-1":
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: REP_ANODE + REP_BNODE are invalid if action code != 2."
                    ucursor.updateRow(row)
                    continue

                # check that action codes 2 + 3 don't have other attributes filled in 
                attributes = [new_directions, new_type1, new_type2, new_ampm1, new_ampm2,
                              new_postedspeed1, new_postedspeed2, new_thrulanes1, new_thrulanes2, 
                              new_thrulanewidth1, new_thrulanewidth2, add_parklanes1, add_parklanes2,
                              add_sigic, add_cltl, add_rrgradecross, new_tolldollars, new_modes]
                
                if action_code in [2, 3] and any(attribute != 0 for attribute in attributes):
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Error: Action Codes 2 + 3 should not be associated with any other attributes."
                    ucursor.updateRow(row)
                    continue

                # if action code 2, REP_ABB must be a regular link
                if action_code == 2 and rep_abb[-1] != "1":
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: Action code 2 must replace a regular link."
                    ucursor.updateRow(row)
                    continue

                # if action code 2, REP_ABB must exist 
                if action_code == 2 and rep_abb not in abbs:
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: Action code 2 must be associated with a valid REP_ABB."
                    ucursor.updateRow(row)
                    continue

                # if action code = 4, then required attributes must be filled
                req_fields = [new_directions, new_type1, new_ampm1, new_thrulanes1, new_thrulanewidth1, new_modes]
                if action_code == 4:
                    if any(field == 0 for field in req_fields):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Error: Missing required attribute(s) on new link."
                        ucursor.updateRow(row)
                        continue
                    elif new_type1 != 7 and new_postedspeed1 == 0:
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Error: Missing speed 1 on new link."
                        ucursor.updateRow(row)
                        continue

                # if action code = 1 or 4 and directions = 1 or 2
                # then all 2 fields should be 0 
                all_fields2 = [new_type2, new_ampm2, new_postedspeed2, new_thrulanes2, new_thrulanewidth2, add_parklanes2]

                if action_code in [1, 4] and new_directions in [1,2]:
                    if any(field2 != 0 for field2 in all_fields2):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = f"Error: Unusable '2' attributes for this direction = {new_directions} link."
                        ucursor.updateRow(row)
                        continue
                    
                # if action code = 1 or 4 and directions = 3 
                # then new_type2, new_ampm2, new_thrulanes2, + new_thrulanes2 should not be 0 
                req_fields2 = [new_type2, new_ampm2, new_thrulanes2, new_thrulanewidth2]

                if action_code in [1, 4] and new_directions == 3:
                    if any(field2 == 0 for field2 in req_fields2):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Error: Missing '2' attributes for this direction = 3 link."
                        ucursor.updateRow(row)
                        continue 
                    elif new_type2 !=7 and new_postedspeed2 == 0:
                        row_fail+=1
                        row[26] = 0
                        row[27] = "Error: Missing speed 2 on this direction = 3 link."
                        ucursor.updateRow(row)
                        continue

                # if link has potential for duplication
                # action code 2 should not be applied to it 
                if action_code == 2 and abb in hwylink_dup_set:
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: Use action code 4 on a link with potential issues with duplication."
                    ucursor.updateRow(row)
                    continue

                # if link has potential for duplication
                # cannot set new directions > 1 
                if new_directions > 1 and abb in hwylink_dup_set:
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: cannot set NEW_DIRECTIONS > 1 or else issue with duplication."
                    ucursor.updateRow(row)
                    continue

                # very specific situation with toll plazas
                if action_code == 1 and abb in toll1_list:
                    if new_type1 != 0 and new_postedspeed1 == 0: 
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Error: Missing new posted speed 1 on a link which changed from a toll plaza."
                        ucursor.updateRow(row)
                        continue

                if action_code == 1 and abb in toll2_list:
                    if new_type2 != 0 and new_postedspeed2 == 0:
                        row_fail+=1
                        row[26] = 0
                        row[27] = "Error: Missing new posted speed 2 on a link which changed from a toll plaza."
                        ucursor.updateRow(row)
                        continue

                # action code 1 should have at least 1 attribute filled in. 
                if action_code == 1 and all(attribute == 0 for attribute in attributes):
                    row_warning +=1
                    row[27] = "Warning: Action Code 1 should make at least one modification."
                    ucursor.updateRow(row)
                    continue

                # warn for links with potential duplication issues 
                if abb in hwylink_dup_set:
                    row_warning +=1 
                    row[27] = "Warning: changes on links with duplication potential should be reviewed."
                    ucursor.updateRow(row)
                    continue
            
            # out of for loop
        
        # out of cursor
        error_file.write("Individual row check:\n")
        if row_fail != 0:
            error_file.write(f"{row_fail} rows failed the individual row check and had USE set to 0. Check output gdb.\n")
        else:
            error_file.write("No rows failed the individual row check.\n")

        if row_warning != 0:
            error_file.write(f"{row_warning} rows passed the individual row check with warnings. Check output gdb.\n\n")
        else:
            error_file.write("\n")

        # row combo check 
        self.get_hwy_dfs()
        
        hwyproj_df = self.hwyproj_df
        hwyproj_df = hwyproj_df[(hwyproj_df.COMPLETION_YEAR != 9999) & (hwyproj_df.USE == 1)]

        error_file.write("Row combo check:\n")

        # check for links with both 2 + 4 applied to it 
        hwyproj_2_set = set(hwyproj_df[hwyproj_df.ACTION_CODE == "2"].ABB.to_list())
        hwyproj_4_set = set(hwyproj_df[hwyproj_df.ACTION_CODE == "4"].ABB.to_list())

        overlap_24_set = hwyproj_2_set.intersection(hwyproj_4_set)

        if len(overlap_24_set) > 0:

            overlap_24_string = str(overlap_24_set)[1:-1]
            where_clause = f"ABB in ({overlap_24_string})"
            with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, where_clause) as ucursor:
                for row in ucursor:
                    row[27] = "Warning: Link has both action codes 2 and 4 applied to it."
                    ucursor.updateRow(row)

            error_file.write(f"Links {overlap_24_string} have both action codes 2 and 4 applied to it.\n")
        
        else:
            error_file.write(f"No warnings about ACTION_CODE 2 + 4 overlapping.")

        # check for links which are replaced but not deleted
        hwyproj_3_set = set(hwyproj_df[hwyproj_df.ACTION_CODE == "3"].ABB.to_list())
        hwyproj_rep_set = set(hwyproj_df[hwyproj_df.REP_ABB != "0-0-1"].REP_ABB.to_list())

        no_delete_set = hwyproj_rep_set - hwyproj_3_set

        if len(no_delete_set) > 0:
        
            no_delete_string = str(no_delete_set)[1:-1]
            where_clause = f"REP_ABB in ({no_delete_string})"
            with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, where_clause) as ucursor:
                for row in ucursor:
                    row[27] = "Warning: Replaced link is not also deleted."
                    ucursor.updateRow(row)

            error_file.write(f"These 'replaced' links {no_delete_string} are not also deleted.\n")
        
        else:
            error_file.write(f"No warnings about replaced links not being deleted.")

        error_file.close()

        print("Base highway project table checked for errors.\n")

    # function that moves the base year up one year
    def hwy_forward_one_year(self):
        
        current_year = self.base_year + 1 

        self.get_hwy_dfs()
        hwyproj_df = self.hwyproj_df[self.hwyproj_df.USE == 1] # Only want the valid projects

        year_projects = hwyproj_df[hwyproj_df.COMPLETION_YEAR == current_year]
        year_projects = year_projects.set_index(["TIPID", "ABB"]).drop(columns = ["OBJECTID", "USE", "PROCESS_NOTES"])
        action_1_dict = year_projects[year_projects.ACTION_CODE == "1"].to_dict("index")
        action_2_dict = year_projects[year_projects.ACTION_CODE == "2"].to_dict("index")
        action_3_dict = year_projects[year_projects.ACTION_CODE == "3"].to_dict("index")
        action_4_dict = year_projects[year_projects.ACTION_CODE == "4"].to_dict("index")

        # these fields will change

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwyproj_table = os.path.join(self.current_gdb, "hwyproj_coding")

        link_fields = ["NEW_BASELINK", "ABB", "DIRECTIONS", "TYPE1", "TYPE2", "AMPM1", "AMPM2", 
                       "POSTEDSPEED1", "POSTEDSPEED2","THRULANES1", "THRULANES2", "THRULANEWIDTH1", "THRULANEWIDTH2", 
                       "PARKLANES1", "PARKLANES2", "SIGIC", "CLTL", "RRGRADECROSS", "TOLLDOLLARS", "MODES", 
                       "PROJECT", "DESCRIPTION"]
        proj_fields = [f.name for f in arcpy.ListFields(hwyproj_table) if f.name != "OBJECTID"]
        for index, field in enumerate(proj_fields):
            print(index, field)

        if len(action_1_dict) > 0:

            for action in action_1_dict:

                project = action[0]
                abb = action[1]

                edits = action_1_dict[action]
                new_directions = int(edits["NEW_DIRECTIONS"])
                new_type1 = int(edits["NEW_TYPE1"])
                new_type2 = int(edits["NEW_TYPE2"])
                new_ampm1 = int(edits["NEW_AMPM1"])
                new_ampm2 = int(edits["NEW_AMPM2"])
                new_postedspeed1 = edits["NEW_POSTEDSPEED1"]
                new_postedspeed2 = edits["NEW_POSTEDSPEED2"]
                new_thrulanes1 = edits["NEW_THRULANES1"]
                new_thrulanes2 = edits["NEW_THRULANES2"]
                new_thrulanewidth1 = edits["NEW_THRULANEWIDTH1"]
                new_thrulanewidth2 = edits["NEW_THRULANEWIDTH2"]
                add_parklanes1 = edits["ADD_PARKLANES1"]
                add_parklanes2 = edits["ADD_PARKLANES2"]
                add_sigic = edits["ADD_SIGIC"]
                add_cltl = edits["ADD_CLTL"]
                add_rrgradecross = edits["ADD_RRGRADECROSS"]
                new_tolldollars = edits["NEW_TOLLDOLLARS"]
                new_modes = int(edits["NEW_MODES"])

                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[2] = new_directions if new_directions != 0 else row[2]
                        row[3] = new_type1 if new_type1 != 0 else row[3]
                        row[4] = new_type2 if new_type2 != 0 else row[4]
                        row[5] = new_ampm1 if new_ampm1 != 0 else row[5]
                        row[6] = new_ampm2 if new_ampm2 != 0 else row[6]
                        row[7] = new_postedspeed1 if new_postedspeed1 != 0 else row[7]
                        row[8] = new_postedspeed2 if new_postedspeed2 != 0 else row[8]
                        row[9] = new_thrulanes1 if new_thrulanes1 != 0 else row[9]
                        row[10] = new_thrulanes2 if new_thrulanes2 != 0 else row[10]
                        row[11] = new_thrulanewidth1 if new_thrulanewidth1 != 0 else row[11]
                        row[12] = new_thrulanewidth2 if new_thrulanewidth2 != 0 else row[12]
                        row[13] = max((row[13] + add_parklanes1), 0)
                        row[14] = max((row[14] + add_parklanes2), 0)
                        row[15] = min(max((row[15] + add_sigic), 0), 1)
                        row[16] = min(max((row[16] + add_cltl), 0), 1)
                        row[17] = min(max((row[17] + add_rrgradecross), 0), 1)
                        row[18] = new_tolldollars if new_tolldollars != 0 else row[18]
                        row[19] = new_modes if new_modes != 0 else row[19]
                        row[20] = project
                        row[21] = f"Modified in {current_year}"
                        ucursor.updateRow(row)

                where_clause = f"ABB = '{abb}' AND COMPLETION_YEAR > {current_year} AND COMPLETION_YEAR <> 9999 "
                where_clause += "AND USE = 1 AND ACTION_CODE = '1'"
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:
                        row[2] = 0 if int(row[2]) == new_directions else row[2]
                        row[3] = 0 if int(row[3]) == new_type1 else row[3]
                        row[4] = 0 if int(row[4]) == new_type2 else row[4]
                        row[5] = 0 if int(row[5]) == new_ampm1 else row[5]
                        row[6] = 0 if int(row[6]) == new_ampm2 else row[6]
                        row[7] = 0 if row[7] == new_postedspeed1 else row[7]
                        row[8] = 0 if row[8] == new_postedspeed2 else row[8]
                        row[9] = 0 if row[9] == new_thrulanes1 else row[9]
                        row[10] = 0 if row[10] == new_thrulanes2 else row[10]
                        row[11] = 0 if row[11] == new_thrulanewidth1 else row[11]
                        row[12] = 0 if row[12] == new_thrulanewidth2 else row[12]
                        row[13] = row[13] - add_parklanes1 if row[13] != 0 else 0
                        row[14] = row[14] - add_parklanes2 if row[14] != 0 else 0
                        row[15] = 0 if row[15] == add_sigic else row[15]
                        row[16] = 0 if row[16] == add_cltl else row[16]
                        row[17] = 0 if row[17] == add_rrgradecross else row[17]
                        row[18] = 0 if row[18] == new_tolldollars else row[18]
                        row[19] = 0 if int(row[19]) == new_modes else row[19]

                        row[27] = f"Modified in {current_year}"
                        ucursor.updateRow(row)

                where_clause = f"ABB = '{abb}' AND COMPLETION_YEAR > {current_year} AND COMPLETION_YEAR <> 9999 "
                where_clause += "AND USE = 1 AND ACTION_CODE = '3'"
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[27] = f"Modified in {current_year}"
                        ucursor.updateRow(row)
                    
        if len(action_2_dict) > 0:
            
            for action in action_2_dict:
                
                project = action[0]
                abb = action[1]

                edits = action_2_dict[action]
                rep_abb = edits["REP_ABB"]

        self.base_year = current_year

    # function that builds future highways
    def build_future_hwys(self, years = None):

        mhn_out_folder = self.mhn_out_folder

        if years == None: 
            years = self.years_list

        build_years = []
        for year in years:
            if year > self.base_year: 
                build_years.append(year)

        build_years.sort()
        
        for scenario_year in build_years:

            print(f"Building highway network for {scenario_year}...")
            next_gdb = os.path.join(mhn_out_folder, f"MHN_{scenario_year}.gdb")
            arcpy.management.Copy(self.current_gdb, next_gdb)
            self.current_gdb = next_gdb

            # print(self.base_year, scenario_year)

            #for year in range(self.base_year, scenario_year):
            self.hwy_forward_one_year()

            #self.base_year = self.scenario_year

# main function for testing 
if __name__ == "__main__":

    start_time = time.time()
    HN = HighwayNetwork()
    HN.generate_base_year()
    HN.check_hwy_fcs()
    HN.check_hwy_project_table()
    HN.build_future_hwys()

    end_time = time.time()
    print(f"{round(end_time - start_time)}s to execute.")

    print("Done")