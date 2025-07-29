## a_HN.py 
## combination of:
## 1) MHN.py, 
## 2) process_highway_coding.sas,
## 3) coding_overlap.sas,
## 4) generate_highway_files_2.sas, and
## 5) import_highway_projects_2.sas

## Author: npeterson
## Translated by ccai (2025)

import math
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
        self.mhn_in_folder = os.path.join(self.in_folder, "1_MHN")
        self.in_gdb = os.path.join(self.mhn_in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_MHN")
        
        years_csv_path = os.path.join(self.in_folder, "input_years.csv")
        years_list_raw = pd.read_csv(years_csv_path)["years"].to_list()

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

        years_list = []
        for year in years_list_raw:
            if year >= self.base_year: 
                years_list.append(year)
        years_list.sort()

        self.years_list = years_list
        self.built_gdbs = []

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
            if arcpy.Exists(rc_path):
                arcpy.management.Delete(rc_path)

    # helper function that copies a gdb
    def copy_gdb_safe(self, input_gdb, output_gdb):
        while True:
            try: 
                if arcpy.Exists(output_gdb):
                    arcpy.management.Delete(output_gdb)
                
                arcpy.management.Copy(input_gdb, output_gdb)
            except:
                print("Copying GDB failed. Trying again...")
                pass
            else:
                break

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
        self.copy_gdb_safe(in_gdb, out_gdb)
        self.current_gdb = out_gdb # update the HN's current gdb 

        self.built_gdbs.append(self.current_gdb)
        self.del_rcs()
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")    

        hwyproj_year_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")

        # to make my life easier - add a field with REP-ABB to the project df
        arcpy.management.AddField(hwyproj_coding_table, "REP_ABB", "TEXT", field_length = 13) # to make my life easier 
        arcpy.management.JoinField(hwyproj_coding_table, "TIPID", hwyproj_year_fc, "TIPID", "COMPLETION_YEAR")
        arcpy.management.AddFields(hwyproj_coding_table, [["USE", "SHORT"], ["PROCESS_NOTES", "TEXT"]])

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
        mhn_out_folder = self.mhn_out_folder

        self.get_hwy_dfs()
        hwynode_df = self.hwynode_df
        hwylink_df = self.hwylink_df 
        hwyproj_years_df = self.hwyproj_years_df

        # keep track of the errors 
        errors = 0

        base_project_table_errors = os.path.join(
            mhn_out_folder, 
            "base_feature_class_errors.txt")
        error_file= open(base_project_table_errors, "a") # open error file, don't forget to close it!

        # nodes must be unique 
        node_counts = hwynode_df.NODE.value_counts()
        
        if (node_counts.max() > 1):
            bad_node_df = node_counts[node_counts > 1]
            error_file.write("These nodes violate unique node ID constraint.\n")
            error_file.write(bad_node_df.to_string() + "\n\n")
            errors += 1 

        all_node_set = set(hwynode_df.NODE.to_list())
        link_node_set = set(hwylink_df.ANODE.to_list()) | set(hwylink_df.BNODE.to_list())

        # nodes should not be disconnected
        extra_nodes = all_node_set - link_node_set 
        
        if (len(extra_nodes) > 0):
            bad_node_df = hwynode_df[hwynode_df.NODE.isin(extra_nodes)]
            error_file.write("These nodes are not connected to any links.\n")
            error_file.write(bad_node_df.to_string() + "\n\n")
            errors += 1

        # link anode / bnode must be in available nodes 
        invalid_nodes = link_node_set - all_node_set
        hwylink_abb_df = hwylink_df[["ANODE", "BNODE", "ABB", "DIRECTIONS"]]

        if (len(invalid_nodes) > 0):
            bad_link_df = hwylink_abb_df[(hwylink_abb_df.ANODE.isin(invalid_nodes)) | (hwylink_abb_df.BNODE.isin(invalid_nodes))]
            error_file.write("These links have end nodes not present in the node feature class.\n")
            error_file.write(bad_link_df.to_string() + "\n\n")
            errors += 1

        # duplicate anode-bnode combinations are not allowed
        ab_count_df = hwylink_df.groupby(["ANODE", "BNODE"]).size().reset_index()
        ab_count_df = ab_count_df.rename(columns = {0: "group_size"})
        
        if (ab_count_df["group_size"].max() > 1):
            bad_link_df = ab_count_df[ab_count_df[["group_size"]] >= 2]
            error_file.write("These links violate the unique ANODE-BNODE constraint.\n")
            error_file.write(bad_link_df.to_string() + "\n\n")
            errors += 1

        # directional links must be valid
        hwylink_dup_df = pd.merge(hwylink_abb_df, hwylink_abb_df.copy(), left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        directions_set = set(hwylink_dup_df.DIRECTIONS_x.to_list()) | set(hwylink_dup_df.DIRECTIONS_y.to_list())
        
        if directions_set != {'1'}:
            bad_link_df = hwylink_dup_df[(hwylink_dup_df.DIRECTIONS_x != '1') | (hwylink_dup_df.DIRECTIONS_y != '1')]
            error_file.write("These links violate the unique directions constraint.\n")
            error_file.write(bad_link_df.to_string() + "\n\n")
            errors += 1

        # TIPIDs should be unique 
        tipid_count_df = hwyproj_years_df.groupby(["TIPID"]).size().reset_index()
        tipid_count_df = tipid_count_df.rename(columns = {0:"group_size"})

        if (tipid_count_df["group_size"].max() > 1):
            bad_project_df = tipid_count_df[tipid_count_df["group_size"] >= 2]
            error_file.write("These projects violate the unique TIPID constraint.\n")
            error_file.write(bad_project_df.to_string() + "\n\n")
            errors +=1

        error_file.close()

        if errors > 0:
            sys.exit("Error(s) were detected in the feature class. Crashing program.")
        else:
            os.remove(base_project_table_errors)

        print("Base feature classes checked for errors.\n")
     
    # function that imports highway project coding
    def import_hwy_project_coding(self): 

        print("Importing highway project coding...")

        mhn_in_folder = self.mhn_in_folder
        mhn_out_folder = self.mhn_out_folder
        hwylink_df = self.hwylink_df 

        import_path = os.path.join(mhn_in_folder, "import_hwy_project_coding.xlsx")
        import_df = pd.read_excel(import_path)

        import_df = import_df.dropna(how = "all")

        if len(import_df) == 0:
            return

        import_df["tipid"] = import_df["tipid"].astype("string")

        # check where tipid, anode, bnode, action is null
        null_df = import_df[pd.isnull(import_df.tipid) | 
                            pd.isnull(import_df.anode) |
                            pd.isnull(import_df.bnode) |
                            pd.isnull(import_df.action)]
        
        import_errors_csv = os.path.join(mhn_out_folder, "import_project_coding_errors.csv")

        if len(null_df) > 0:
            null_df.to_csv(import_errors_csv, index = False)
            sys.exit("Row(s) detected where TIPID, ANODE, BNODE, or ACTION is null. Crashing program.")

        import_df = import_df.fillna(0)

        # check where anode + bnode don't correspond to a valid link
        abb_dict = hwylink_df[["ANODE", "BNODE", "ABB"]].set_index(["ANODE", "BNODE"]).to_dict("index")
        import_records = import_df.to_dict("records")
        bad_records = []

        for row in import_records:

            if (row["anode"], row["bnode"]) not in abb_dict:
                bad_records.append(row)

        if len(bad_records) > 0:
            bad_records_df = pd.DataFrame(bad_records)
            bad_records_df.to_csv(import_errors_csv, index = False)
            sys.exit("Row(s) detected where ANODE and BNODE don't correspond to a valid link. Crashing program.")

        # check where tipid-abb is not unique
        import_df["abb"] = import_df.apply(lambda x: abb_dict[(x["anode"], x["bnode"])]["ABB"], axis = 1)
        import_records = import_df.to_dict("records")

        tipid_abb_series = import_df.groupby(["tipid", "abb"]).size()
        duplicate_combos = tipid_abb_series[tipid_abb_series > 1].to_dict()

        duplicate_records = []

        for row in import_records:

            if (row["tipid"], row["abb"]) in duplicate_combos:
                duplicate_records.append(row)

        if len(duplicate_records) > 0:
            duplicate_records_df = pd.DataFrame(duplicate_records)
            duplicate_records_df.to_csv(import_errors_csv, index = False)
            sys.exit("Rows detected where TIPID - ABB is not unique. Crashing program.")

        # now add to the table 
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")    
        fields = ["TIPID", "ABB"]
        existing_list = []

        with arcpy.da.SearchCursor(hwyproj_coding_table, fields) as scursor:

            for row in scursor:
                existing_list.append((row[0], row[1]))

        update_rows = []
        insert_rows = []

        for record in import_records:

            if (record["tipid"], record["abb"]) in existing_list:
                update_rows.append(record)
            else:
                insert_rows.append(record)

        fields = [f.name for f in arcpy.ListFields(hwyproj_coding_table) if f.name != "OBJECTID"]
        # for index, field in enumerate(fields):
        #     print(index, field)

        # if it's already in the project table- just update 
        for update_row in update_rows:

            tipid = update_row["tipid"]
            abb = update_row["abb"]

            where_clause = f"TIPID = '{tipid}' AND ABB = '{abb}'"
            with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, where_clause) as ucursor:
                for row in ucursor:
                    
                    row[1] = update_row["action"]
                    row[2] = update_row["directions"]
                    row[3] = update_row["type1"]
                    row[4] = update_row["type2"]
                    row[5] = update_row["ampm1"]
                    row[6] = update_row["ampm2"]
                    row[7] = update_row["speed1"]
                    row[8] = update_row["speed2"]
                    row[9] = update_row["lanes1"]
                    row[10] = update_row["lanes2"]
                    row[11] = update_row["feet1"]
                    row[12] = update_row["feet2"]
                    row[13] = update_row["parklanes1"]
                    row[14] = update_row["parklanes2"]
                    row[15] = update_row["sigic"]
                    row[16] = update_row["cltl"]
                    row[17] = update_row["rr_grade_sep"]
                    row[18] = update_row["tolldollars"]
                    row[19] = update_row["modes"]
                    row[20] = update_row["tod"]
                    row[22] = update_row["rep_anode"]
                    row[23] = update_row["rep_bnode"]
                    row[27] = "Updated from import successfully."

                    ucursor.updateRow(row)

        # else- insert, however, must compute year 
        hwyproj_years_df = self.hwyproj_years_df
        hwyproj_years_dict = hwyproj_years_df.set_index("TIPID")["COMPLETION_YEAR"].to_dict()

        with arcpy.da.InsertCursor(hwyproj_coding_table, fields) as icursor:

            for insert_row in insert_rows:

                completion_year = None
                if insert_row["tipid"] in hwyproj_years_dict:
                    completion_year = hwyproj_years_dict[insert_row["tipid"]]

                row = [
                    insert_row["tipid"], 
                    insert_row["action"], 
                    insert_row["directions"],
                    insert_row["type1"],
                    insert_row["type2"],
                    insert_row["ampm1"],
                    insert_row["ampm2"],
                    insert_row["speed1"],
                    insert_row["speed2"],
                    insert_row["lanes1"],
                    insert_row["lanes2"],
                    insert_row["feet1"],
                    insert_row["feet2"],
                    insert_row["parklanes1"],
                    insert_row["parklanes2"],
                    insert_row["sigic"],
                    insert_row["cltl"],
                    insert_row["rr_grade_sep"],
                    insert_row["tolldollars"],
                    insert_row["modes"],
                    insert_row["tod"],
                    insert_row["abb"],
                    insert_row["rep_anode"],
                    insert_row["rep_bnode"],
                    None,
                    completion_year,
                    None,
                    "Inserted from import successfully."
                ]

                icursor.insertRow(row)

        print("Highway project coding imported.\n")

    # function that checks the project table
    def check_hwy_project_table(self):

        print("Checking base project table for errors...")
        
        hwylink_df = self.hwylink_df
        mhn_out_folder = self.mhn_out_folder
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")    

        # prepare added columns
        abb_dict = hwylink_df[["ANODE", "BNODE", "ABB"]].set_index(["ANODE", "BNODE"]).to_dict("index")
        fields = ["REP_ANODE", "REP_BNODE", "REP_ABB"]

        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields) as ucursor:
            for row in ucursor:

                anode = row[0]
                bnode = row[1]
                if (anode, bnode) in abb_dict:
                    rep_abb = abb_dict[(anode, bnode)]["ABB"]
                    row[2] = rep_abb

                else:
                    row[2] = "0"
                
                ucursor.updateRow(row)

        arcpy.management.CalculateField(hwyproj_coding_table, "USE", "1")

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

        hwyproj_df = self.hwyproj_df
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
        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields) as ucursor:
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
        where_clause = "USE = 1"
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
                if action_code in [1, 3, 4] and rep_abb != "0":
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

                # if action code 2, REP_ABB must exist 
                if action_code == 2 and rep_abb == "0":
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: Action code 2 must be associated with a valid REP_ABB."
                    ucursor.updateRow(row)
                    continue

                # if action code 2, REP_ABB must be a regular link
                if action_code == 2 and rep_abb[-1] != "1":
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Error: Action code 2 must replace a regular link."
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
        hwyproj_rep_set = set(hwyproj_df[hwyproj_df.REP_ABB != "0"].REP_ABB.to_list())

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

        # write to error excel
        self.get_hwy_dfs()
        hwyproj_df = self.hwyproj_df

        xl_path = os.path.join(mhn_out_folder, "base_project_table_errors.xlsx")

        rename_dict = {
            "TIPID" : "tipid",
            "ACTION_CODE" : "action",
            "NEW_DIRECTIONS" : "directions",
            "NEW_TYPE1" : "type1",
            "NEW_TYPE2" : "type2",
            "NEW_AMPM1" : "ampm1",
            "NEW_AMPM2" : "ampm2",
            "NEW_POSTEDSPEED1" : "speed1",
            "NEW_POSTEDSPEED2" : "speed2",
            "NEW_THRULANES1" : "lanes1",
            "NEW_THRULANES2" : "lanes2",
            "NEW_THRULANEWIDTH1" : "feet1",
            "NEW_THRULANEWIDTH2" : "feet2",
            "ADD_PARKLANES1" : "parklanes1",
            "ADD_PARKLANES2" : "parklanes2",
            "ADD_SIGIC" : "sigic",
            "ADD_CLTL" : "cltl",
            "ADD_RRGRADECROSS" : "rr_grade_sep",
            "NEW_TOLLDOLLARS" : "tolldollars",
            "NEW_MODES" : "modes",
            "TOD" : "tod",
            "ABB": "abb",
            "REP_ANODE" : "rep_anode",
            "REP_BNODE" : "rep_bnode"
        }

        hwyproj_xl_df = hwyproj_df[hwyproj_df.USE == 0].rename(columns = rename_dict)
        hwyproj_xl_df["anode"] = hwyproj_xl_df["abb"].apply(lambda x: x.split("-")[0])
        hwyproj_xl_df["bnode"] = hwyproj_xl_df["abb"].apply(lambda x: x.split("-")[1])
        hwyproj_xl_df.to_excel(xl_path, index = False)

        print("Base highway project table checked for errors.\n")

    # function that subsets to certain projects
    def subset_to_projects(self):

        print("Subsetting to projects...")

        mhn_in_folder = self.mhn_in_folder
        subset_hwy_path = os.path.join(mhn_in_folder, "subset_hwy_projects.csv")

        subset_df = pd.read_csv(subset_hwy_path).drop_duplicates()
        subset_df["TIPID"] = subset_df["TIPID"].astype("string")

        all_tipid_list = subset_df[subset_df.ABB == "all"].TIPID.to_list()
        spec_abb_list = list(subset_df[subset_df.ABB != "all"].set_index(["TIPID", "ABB"]).index.values)

        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        fields = ["TIPID", "ABB", "USE"]

        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields) as ucursor:
            for row in ucursor:

                tipid = row[0]
                abb = row[1]

                tipid_abb = (tipid, abb)
                
                if tipid not in all_tipid_list and tipid_abb not in spec_abb_list:
                    row[2] = 0
                    ucursor.updateRow(row)

        print("Subset complete.\n")

    # function that copies base links into the combined gdb 
    def copy_hwy_links(self):

        mhn_out_folder = self.mhn_out_folder
        mhn_all_name = "MHN_all.gdb"
        mhn_all_gdb = os.path.join(mhn_out_folder, mhn_all_name)

        hwylink = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        link_workspace = os.path.join(mhn_all_gdb, "hwylinks_all")

        base_links_name = f"HWYLINK_{self.base_year}"
        arcpy.management.CreateFeatureclass(link_workspace, base_links_name, template = hwylink, spatial_reference = 26771)
        base_links = os.path.join(link_workspace, base_links_name)

        hwylink_fields = [f.name for f in arcpy.ListFields(hwylink) if f.name != "OBJECTID" and f.type != "Geometry"]
        hwylink_fields += ["SHAPE@"]

        with arcpy.da.SearchCursor(hwylink, hwylink_fields) as scursor:
            with arcpy.da.InsertCursor(base_links, hwylink_fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

    # function that creates a gdb of all built years 
    def create_combined_gdb(self):

        print("Creating combined gdb...")

        final_year = max(self.years_list)
        mhn_out_folder = self.mhn_out_folder

        # create a combined gdb to store + check final output 
        mhn_all_name = "MHN_all.gdb"
        mhn_all_gdb = os.path.join(mhn_out_folder, mhn_all_name)
        arcpy.management.CreateFileGDB(mhn_out_folder, mhn_all_name)

        # make an fc of the projects which will be applied 
        arcpy.management.CreateFeatureclass(mhn_all_gdb, "hwyproj_applied", "POLYLINE", spatial_reference = 26771)
        hwyproj_applied = os.path.join(mhn_all_gdb, "hwyproj_applied")

        hwyproj = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_fields = [[f.name, f.type] for f in arcpy.ListFields(hwyproj) if (f.name != "OBJECTID")]
        hwyproj_fields_2 = [f.name for f in arcpy.ListFields(hwyproj) if (f.name != ("OBJECTID"))]

        arcpy.management.AddFields(hwyproj_applied, hwyproj_fields)

        where_clause = f"USE = 1 AND COMPLETION_YEAR <= {final_year}"
        with arcpy.da.SearchCursor(hwyproj, hwyproj_fields_2, where_clause) as scursor:
            with arcpy.da.InsertCursor(hwyproj_applied, hwyproj_fields_2) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

        geom_fields = ["SHAPE@", "ABB"]
        hwylink = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        
        with arcpy.da.UpdateCursor(hwyproj_applied, geom_fields) as ucursor:
            for u_row in ucursor:

                abb = u_row[1]

                geom = None
                where_clause = f"ABB = '{abb}'"

                with arcpy.da.SearchCursor(hwylink, geom_fields, where_clause) as scursor:
                    for s_row in scursor:

                        geom = s_row[0]

                ucursor.updateRow([geom, abb])

        # make an fc of the links which had multiple updates 
        hwyproj_multiple = os.path.join(mhn_all_gdb, "hwyproj_multiple")
        arcpy.management.Sort(hwyproj_applied, hwyproj_multiple, "COMPLETION_YEAR")

        hwyproj_df = self.hwyproj_df
        hwyproj_df_use = hwyproj_df[(hwyproj_df.USE == 1) & (hwyproj_df.COMPLETION_YEAR <= final_year)]
        hwyproj_multiple_dict = hwyproj_df_use.ABB.value_counts()[hwyproj_df_use.ABB.value_counts() >= 2].to_dict()

        with arcpy.da.UpdateCursor(hwyproj_multiple, "ABB") as ucursor:
            for row in ucursor:

                if row[0] not in hwyproj_multiple_dict:
                    ucursor.deleteRow()

        # copy the nodes 
        hwynode = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        arcpy.management.CreateFeatureclass(mhn_all_gdb, "hwynode_all", template = hwynode, spatial_reference = 26771)
        hwynode_all = os.path.join(mhn_all_gdb, "hwynode_all")
        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode) if f.name != "OBJECTID" and f.type != "Geometry"]
        hwynode_fields += ["SHAPE@XY"]

        with arcpy.da.SearchCursor(hwynode, hwynode_fields) as scursor:
            with arcpy.da.InsertCursor(hwynode_all, hwynode_fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

        # create a feature dataset to store the links 
        # then copy over the base links
        arcpy.management.CreateFeatureDataset(mhn_all_gdb, "hwylinks_all", spatial_reference = 26771)
        self.copy_hwy_links()
        
        print("Combined gdb created.\n")

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

        exclude_fields = ["OBJECTID", "ANODE", "BNODE", "BASELINK"]
        link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if (f.type!="Geometry" and f.name not in exclude_fields)]
        # for index, field in enumerate(link_fields):
        #     print(index, field)
        proj_fields = [f.name for f in arcpy.ListFields(hwyproj_table) if f.name != "OBJECTID"]
        # for index, field in enumerate(proj_fields):
        #     print(index, field)

        if len(action_1_dict) > 0:

            for action in action_1_dict:

                project = action[0]
                abb = action[1]

                edits = action_1_dict[action]
                new_directions = edits["NEW_DIRECTIONS"]
                new_type1 = edits["NEW_TYPE1"]
                new_type2 = edits["NEW_TYPE2"]
                new_ampm1 = edits["NEW_AMPM1"]
                new_ampm2 = edits["NEW_AMPM2"]
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
                new_modes = edits["NEW_MODES"]

                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[2] = new_directions if new_directions != "0" else row[2]
                        row[3] = new_type1 if new_type1 != "0" else row[3]
                        row[4] = new_type2 if new_type2 != "0" else row[4]
                        row[5] = new_ampm1 if new_ampm1 != "0" else row[5]
                        row[6] = new_ampm2 if new_ampm2 != "0" else row[6]
                        row[7] = new_postedspeed1 if new_postedspeed1 != 0 else row[7]
                        row[8] = new_postedspeed2 if new_postedspeed2 != 0 else row[8]
                        row[9] = new_thrulanes1 if new_thrulanes1 != 0 else row[9]
                        row[10] = new_thrulanes2 if new_thrulanes2 != 0 else row[10]
                        row[11] = new_thrulanewidth1 if new_thrulanewidth1 != 0 else row[11]
                        row[12] = new_thrulanewidth2 if new_thrulanewidth2 != 0 else row[12]
                        row[13] = max((row[13] + add_parklanes1), 0)
                        row[14] = max((row[14] + add_parklanes2), 0)
                        row[17] = min(max((row[17] + add_sigic), 0), 1)
                        row[18] = min(max((row[18] + add_cltl), 0), 1)
                        row[19] = min(max((row[19] + add_rrgradecross), 0), 1)
                        row[21] = new_tolldollars if new_tolldollars != 0 else row[21]
                        row[22] = new_modes if new_modes != "0" else row[22]
                        row[36] = project
                        row[37] = f"Modified in {current_year}"

                        # if directions = 1 or 2
                        # empty all "2" fields
                        if row[2] in ["1", "2"]:
                            row[4] = "0"
                            row[6] = "0"
                            row[8] = 0
                            row[10] = 0
                            row[12] = 0
                            row[14] = 0
                        
                        if row[2] == "1":
                            row[16] = ""

                        ucursor.updateRow(row)

                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '1'"

                # if modified and modified again
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:
                        row[2] = "0" if row[2] == new_directions else row[2]
                        row[3] = "0" if row[3] == new_type1 else row[3]
                        row[4] = "0" if row[4] == new_type2 else row[4]
                        row[5] = "0" if row[5] == new_ampm1 else row[5]
                        row[6] = "0" if row[6] == new_ampm2 else row[6]
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
                        row[19] = "0" if row[19] == new_modes else row[19]

                        row[27] = f"Modified in {current_year}"
                        ucursor.updateRow(row)

                # if modified and deleted- no impact
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '3'"
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[27] = f"Modified in {current_year}"
                        ucursor.updateRow(row)
                    
        if len(action_2_dict) > 0:
            
            # invert action_2_dict

            inverted_2_dict = {}
            for action in action_2_dict:

                project = action[0]
                abb = action[1]
                edits = action_2_dict[action]
                rep_abb = edits["REP_ABB"]

                if (project, rep_abb) not in inverted_2_dict:
                    inverted_2_dict[(project, rep_abb)] = [abb]
                else:
                    inverted_2_dict[(project, rep_abb)].append(abb)
                
            for action in inverted_2_dict:

                project = action[0]
                rep_abb = action[1]
                abb_list = inverted_2_dict[action]
                abb_list_string = [f"'{abb}'" for abb in abb_list]
                abb_string = ", ".join(abb_list_string)

                directions = "0"
                type1 = "0"
                type2 = "0"
                ampm1 = "0"
                ampm2 = "0"
                postedspeed1 = 0 
                postedspeed2 = 0
                thrulanes1 = 0
                thrulanes2 = 0
                thrulanewidth1 = 0
                thrulanewidth2 = 0
                parklanes1 = 0
                parklanes2 = 0
                sigic = 0
                cltl = 0
                rrgradecross = 0
                tollsys = 0
                tolldollars = 0
                modes = "0"
                truckrte = "0"
                truckres = "0"
                vclearance = 0
                meso = 0

                s_where_clause = f"ABB = '{rep_abb}'"
                u_where_clause = f"ABB in ({abb_string})"

                # get the attributes from the regular link
                with arcpy.da.SearchCursor(hwylink_fc, link_fields, s_where_clause) as scursor:
                    for row in scursor:
                        directions = row[2]
                        type1 = row[3]
                        type2 = row[4]
                        ampm1 = row[5]
                        ampm2 = row[6]
                        postedspeed1 = row[7]
                        postedspeed2 = row[8]
                        thrulanes1 = row[9]
                        thrulanes2 = row[10]
                        thrulanewidth1 = row[11]
                        thrulanewidth2 = row[12]
                        parklanes1 = row[13]
                        parklanes2 = row[14]
                        sigic = row[17]
                        cltl = row[18]
                        rrgradecross = row[19]
                        tollsys = row[20]
                        tolldollars = row[21]
                        modes = row[22]
                        truckrte = row[26]
                        truckres = row[27]
                        vclearance = row[29]
                        meso = row[32]

                # and give them to the skeleton link
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, u_where_clause) as ucursor:
                    for row in ucursor:

                        row[2] = directions
                        row[3] = type1
                        row[4] = type2
                        row[5] = ampm1
                        row[6] = ampm2
                        row[7] = postedspeed1 
                        row[8] = postedspeed2
                        row[9] = thrulanes1
                        row[10] = thrulanes2
                        row[11] = thrulanewidth1
                        row[12] = thrulanewidth2
                        row[13] = parklanes1
                        row[14] = parklanes2
                        row[17] = sigic
                        row[18] = cltl
                        row[19] = rrgradecross
                        row[20] = tollsys
                        row[21] = tolldollars
                        row[22] = modes
                        row[26] = truckrte
                        row[27] = truckres
                        row[29] = vclearance
                        row[32] = meso

                        row[35] = "1"
                        row[36] = project
                        row[37] = f"Replaced {rep_abb} in {current_year}"
                        ucursor.updateRow(row)

                for abb in abb_list:

                    # if you replaced a link once, you can't replace it again
                    where_clause = "USE = 1 "
                    where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                    where_clause += "AND ACTION_CODE = '2'"
                    
                    with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                        for row in ucursor: 

                            row[26] = 0
                            row[27] = f"Replaced {rep_abb} in {current_year}"

                            ucursor.updateRow(row)

                    # if you replaced a link, you can't add it again - only modify
                    where_clause = "USE = 1 "
                    where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                    where_clause += "AND ACTION_CODE = '4'"

                    with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                        for row in ucursor: 

                            row[1] = "1" # new action code - 1 

                            row[2] = 0 if row[2] == directions else row[2]
                            row[3] = 0 if row[3] == type1 else row[3]
                            row[4] = 0 if row[4] == type2 else row[4]
                            row[5] = 0 if row[5] == ampm1 else row[5]
                            row[6] = 0 if row[6] == ampm2 else row[6]
                            row[7] = 0 if row[7] == postedspeed1 else row[7]
                            row[8] = 0 if row[8] == postedspeed2 else row[8]
                            row[9] = 0 if row[9] == thrulanes1 else row[9]
                            row[10] = 0 if row[10] == thrulanes2 else row[10]
                            row[11] = 0 if row[11] == thrulanewidth1 else row[11]
                            row[12] = 0 if row[12] == thrulanewidth2 else row[12]
                            row[13] = row[13] - parklanes1 if row[13] != 0 else 0
                            row[14] = row[14] - parklanes2 if row[14] != 0 else 0
                            row[15] = 0 if row[15] == sigic else row[15]
                            row[16] = 0 if row[16] == cltl else row[16]
                            row[17] = 0 if row[17] == rrgradecross else row[17]
                            row[18] = 0 if row[18] == tolldollars else row[18]
                            row[19] = 0 if row[19] == modes else row[19]
          
                            row[27] = f"Replaced {rep_abb} in {current_year}"

                            ucursor.updateRow(row)

        if len(action_3_dict) > 0:
            
            for action in action_3_dict:

                project = action[0]
                abb = action[1]

                directions = "0"
                type1 = "0"
                type2 = "0"
                ampm1 = "0"
                ampm2 = "0"
                postedspeed1 = 0 
                postedspeed2 = 0
                thrulanes1 = 0
                thrulanes2 = 0
                thrulanewidth1 = 0
                thrulanewidth2 = 0
                parklanes1 = 0
                parklanes2 = 0
                sigic = 0
                cltl = 0
                rrgradecross = 0
                tolldollars = 0
                modes = "0"

                # set to skeleton link but get attributes
                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        directions = row[2]
                        type1 = row[3]
                        type2 = row[4]
                        ampm1 = row[5]
                        ampm2 = row[6]
                        postedspeed1 = row[7]
                        postedspeed2 = row[8]
                        thrulanes1 = row[9]
                        thrulanes2 = row[10]
                        thrulanewidth1 = row[11]
                        thrulanewidth2 = row[12]
                        parklanes1 = row[13]
                        parklanes2 = row[14]
                        sigic = row[17]
                        cltl = row[18]
                        rrgradecross = row[19]
                        tolldollars = row[21]
                        modes = row[22]

                        row[35] = "0"
                        row[36] = project
                        row[37] = f"Deleted in {current_year}"

                        ucursor.updateRow(row)

                # if a link is deleted, it cannot be modified or deleted again
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE in ('1', '3')"

                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:
                        row[26] = 0 
                        row[27] = f"Deleted in {current_year}"

                        ucursor.updateRow(row)

                where_clause = "USE = 1 "
                where_clause += f"AND REP_ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '2'"

                # if a link is deleted
                # a skeleton that replaces it, is now added 
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[1] = 4 # becomes add 

                        row[2] = directions
                        row[3] = type1
                        row[4] = type2
                        row[5] = ampm1
                        row[6] = ampm2
                        row[7] = postedspeed1
                        row[8] = postedspeed2
                        row[9] = thrulanes1
                        row[10] = thrulanes2
                        row[11] = thrulanewidth1
                        row[12] = thrulanewidth2
                        row[13] = parklanes1
                        row[14] = parklanes2
                        row[15] = sigic
                        row[16] = cltl
                        row[17] = rrgradecross
                        row[18] = tolldollars
                        row[19] = modes
                        
                        row[22] = 0 # rep_anode = 0
                        row[23] = 0 # rep_bnode = 0

                        row[27] = f"Deleted replacement link in {current_year}"

                        ucursor.updateRow(row)

        if len(action_4_dict) > 0:

            for action in action_4_dict:

                project = action[0]
                abb = action[1]

                edits = action_4_dict[action]
                new_directions = edits["NEW_DIRECTIONS"]
                new_type1 = edits["NEW_TYPE1"]
                new_type2 = edits["NEW_TYPE2"]
                new_ampm1 = edits["NEW_AMPM1"]
                new_ampm2 = edits["NEW_AMPM2"]
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
                new_modes = edits["NEW_MODES"]

                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:
                        row[2] = new_directions
                        row[3] = new_type1
                        row[4] = new_type2
                        row[5] = new_ampm1
                        row[6] = new_ampm2
                        row[7] = new_postedspeed1
                        row[8] = new_postedspeed2
                        row[9] = new_thrulanes1
                        row[10] = new_thrulanes2
                        row[11] = new_thrulanewidth1
                        row[12] = new_thrulanewidth2
                        row[13] = add_parklanes1
                        row[14] = add_parklanes2
                        row[17] = add_sigic
                        row[18] = add_cltl
                        row[19] = add_rrgradecross
                        row[21] = new_tolldollars
                        row[22] = new_modes

                        row[35] = "1"
                        row[36] = project
                        row[37] = f"Added in {current_year}"

                        ucursor.updateRow(row)

                # if added can't be replaced
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '2'"
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[26] = 0
                        row[27] = f"Added in {current_year}"
                        ucursor.updateRow(row)

                # if added can't be added again 
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '4'"
                with arcpy.da.UpdateCursor(hwyproj_table, proj_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[1] = "1"
                        
                        row[2] = "0" if row[2] == new_directions else row[2]
                        row[3] = "0" if row[3] == new_type1 else row[3]
                        row[4] = "0" if row[4] == new_type2 else row[4]
                        row[5] = "0" if row[5] == new_ampm1 else row[5]
                        row[6] = "0" if row[6] == new_ampm2 else row[6]
                        row[7] = 0 if row[7] == new_postedspeed1 else row[7]
                        row[8] = 0 if row[8] == new_postedspeed2 else row[8]
                        row[9] = 0 if row[9] == new_thrulanes1 else row[9]
                        row[10] = 0 if row[10] == new_thrulanes2 else row[10]
                        row[11] = 0 if row[11] == new_thrulanewidth1 else row[11]
                        row[12] = 0 if row[12] == new_thrulanewidth2 else row[12]
                        row[13] = row[13] - add_parklanes1
                        row[14] = row[14] - add_parklanes2
                        row[15] = 0 if row[15] == add_sigic else row[15]
                        row[16] = 0 if row[16] == add_cltl else row[16]
                        row[17] = 0 if row[17] == add_rrgradecross else row[17]
                        row[18] = 0 if row[18] == new_tolldollars else row[18]
                        row[19] = "0" if row[19] == new_modes else row[19]

                        row[27] = f"Added in {current_year}"
                        ucursor.updateRow(row)
        
        # set use on all projects completed this year to 0 
        where_clause = f"COMPLETION_YEAR = {current_year}"
        with arcpy.da.UpdateCursor(hwyproj_table, ["USE", "PROCESS_NOTES"], where_clause) as ucursor:
            for row in ucursor:
                row[0] = 0
                row[1] = f"Completed in {current_year}"

                ucursor.updateRow(row)

        self.base_year = current_year

    # function that builds future highways
    def build_future_hwys(self, subset = False, build_years = None):

        mhn_out_folder = self.mhn_out_folder

        if build_years == None: 
            build_years = self.years_list.copy()

        if self.base_year in build_years:
            build_years.remove(self.base_year)

        if subset == True:
            self.subset_to_projects()
        
        self.create_combined_gdb()
        
        # build the future highways 
        for build_year in build_years:

            print(f"Building highway network for {build_year}...")
            next_gdb = os.path.join(mhn_out_folder, f"MHN_{build_year}.gdb")
            self.copy_gdb_safe(self.current_gdb, next_gdb)
            self.current_gdb = next_gdb

            for year in range(self.base_year, build_year):
                self.hwy_forward_one_year()

            # copy built links into combined gdb 
            self.copy_hwy_links()

            # add the built gdb into the list of built gdbs
            self.built_gdbs.append(self.current_gdb)

        print("All years built.\n")

    # function that cleans up the output gdbs 
    def finalize_hwy_data(self):

        print("Finalizing highway data...")

        built_gdbs = self.built_gdbs

        hwyproj_years_df = self.hwyproj_years_df

        hwyproj_year_df = hwyproj_years_df[["TIPID", "COMPLETION_YEAR"]]
        hwyproj_mcp_df = hwyproj_years_df[hwyproj_years_df.MCP_ID.notnull()][["TIPID", "MCP_ID"]]
        hwyproj_rsp_df = hwyproj_years_df[hwyproj_years_df.RSP_ID.notnull()][["TIPID", "RSP_ID"]]
        hwyproj_rcp_df = hwyproj_years_df[hwyproj_years_df.RCP_ID.notnull()][["TIPID", "RCP_ID"]]
        hwyproj_notes_df = hwyproj_years_df[hwyproj_years_df.Notes.notnull()][["TIPID", "Notes"]]
        
        hwyproj_year_dict = hwyproj_year_df.set_index("TIPID").to_dict("index")
        hwyproj_mcp_dict = hwyproj_mcp_df.set_index("TIPID").to_dict("index")
        hwyproj_rsp_dict = hwyproj_rsp_df.set_index("TIPID").to_dict("index")
        hwyproj_rcp_dict = hwyproj_rcp_df.set_index("TIPID").to_dict("index")
        hwyproj_notes_dict = hwyproj_notes_df.set_index("TIPID").to_dict("index")
    
        for gdb in built_gdbs:
            hwylink = os.path.join(gdb, "hwynet/hwynet_arc")
            hwynode = os.path.join(gdb, "hwynet/hwynet_node")
            hwyproj = os.path.join(gdb, "hwyproj_coding")
            hwyproj_years = os.path.join(gdb, "hwynet/hwyproj")

            # remove deleted links
            # change abb to reflect new baselink
            deleted_links = []
            remaining_nodes = set()
            abb_to_new_abb = {}
            fields = ["BASELINK", "NEW_BASELINK", "ANODE", "BNODE", "ABB"]
            with arcpy.da.UpdateCursor(hwylink, fields) as ucursor:
                for row in ucursor:

                    if row[0] == "1" and row[1] == "0":
                        abb = row[4]
                        deleted_links.append(abb)
                        ucursor.deleteRow()
                    
                    else:
                        row[0] = row[1] # baselink = new baselink 

                        abb = row[4]
                        new_abb = f"{row[2]}-{row[3]}-{row[0]}"

                        abb_to_new_abb[abb] = new_abb
                        remaining_nodes.add(row[2])
                        remaining_nodes.add(row[3])

                        row[4] = new_abb
                        ucursor.updateRow(row)

            arcpy.management.DeleteField(hwylink, ["NEW_BASELINK", "PROJECT", "DESCRIPTION"])

            # delete nodes which are not connected to links
            with arcpy.da.UpdateCursor(hwynode, ["NODE"]) as ucursor:
                for row in ucursor:

                    if row[0] not in remaining_nodes:
                        ucursor.deleteRow()

            # in project table, drop use = 0 + projects on deleted links
            # else, update its abb
            fields = ["USE", "ABB"]
            with arcpy.da.UpdateCursor(hwyproj, fields) as ucursor:
                for row in ucursor:

                    # if row[0] == 0 or row[1] in deleted_links:
                    if row[0] == 0:
                        ucursor.deleteRow()

                    else:
                        abb = row[1]
                        row[1] = abb_to_new_abb[abb]
                        ucursor.updateRow(row)


            arcpy.management.DeleteField(hwyproj, ["REP_ABB", "COMPLETION_YEAR", 
                                                   "USE", "PROCESS_NOTES"])

            # make an fc of the remaining projects
            arcpy.management.CreateFeatureclass(gdb, "hwyproj_remaining", "POLYLINE", spatial_reference = 26771)
            hwyproj_remaining = os.path.join(gdb, "hwyproj_remaining")
            arcpy.management.AddFields(hwyproj_remaining, [["TIPID", "TEXT"], ["ABB", "TEXT"]])

            with arcpy.da.SearchCursor(hwyproj, ["TIPID", "ABB"]) as scursor:
                with arcpy.da.InsertCursor(hwyproj_remaining, ["TIPID", "ABB"]) as icursor:

                    for row in scursor:
                        icursor.insertRow(row)

            geom_fields = ["SHAPE@", "ABB"]
            with arcpy.da.UpdateCursor(hwyproj_remaining, geom_fields) as ucursor:
                for u_row in ucursor:

                    abb = u_row[1]

                    geom = None
                    where_clause = f"ABB = '{abb}'"

                    with arcpy.da.SearchCursor(hwylink, geom_fields, where_clause) as scursor:
                        for s_row in scursor:

                            geom = s_row[0]
                            
                    ucursor.updateRow([geom, abb])

            hwyproj_dissolve = os.path.join(gdb, "hwyproj_dissolve")
            arcpy.management.Dissolve(hwyproj_remaining, hwyproj_dissolve, ["TIPID"])

            arcpy.management.AddFields(hwyproj_dissolve, [["TIPID_NEW", "TEXT", "TIPID", 10],
                                                          ["COMPLETION_YEAR", "SHORT"],
                                                          ["MCP_ID", "TEXT", "MCP_ID", 6],
                                                          ["RSP_ID", "LONG"], ["RCP_ID", "LONG"],
                                                          ["Notes", "Text"]])

            fields = ["TIPID", "TIPID_NEW", "COMPLETION_YEAR",
                      "MCP_ID", "RSP_ID", "RCP_ID", "Notes"]
            
            with arcpy.da.UpdateCursor(hwyproj_dissolve, fields) as ucursor:
                for row in ucursor:

                    tipid = row[0]
                    row[1] = tipid

                    row[2] = hwyproj_year_dict[tipid]["COMPLETION_YEAR"]

                    if tipid in hwyproj_mcp_dict:
                        row[3] = hwyproj_mcp_dict[tipid]["MCP_ID"]

                    if tipid in hwyproj_rsp_dict:
                        row[4] = hwyproj_rsp_dict[tipid]["RSP_ID"]

                    if tipid in hwyproj_rcp_dict:
                        row[5] = int(hwyproj_rcp_dict[tipid]["RCP_ID"])

                    if tipid in hwyproj_notes_dict:
                        row[6] = hwyproj_notes_dict[tipid]["Notes"]

                    ucursor.updateRow(row)
            
            arcpy.management.DeleteField(hwyproj_dissolve, ["TIPID"])
            arcpy.management.AlterField(hwyproj_dissolve, "TIPID_NEW", new_field_name = "TIPID")

            with arcpy.da.UpdateCursor(hwyproj_years, ["TIPID"]) as ucursor:
                for row in ucursor:
                    ucursor.deleteRow()

            arcpy.management.Append(hwyproj_dissolve, hwyproj_years, "TEST")
            arcpy.management.Delete(hwyproj_remaining)
            arcpy.management.Delete(hwyproj_dissolve)

        print("Highway data finalized.\n")

# main function for testing 
if __name__ == "__main__":

    start_time = time.time()

    HN = HighwayNetwork()
    HN.generate_base_year()
    HN.import_hwy_project_coding()
    HN.check_hwy_fcs()
    HN.check_hwy_project_table()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")