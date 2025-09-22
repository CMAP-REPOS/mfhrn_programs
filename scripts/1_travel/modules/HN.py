## HN.py 
## a combination of:
## 1) MHN.py, 
## 2) process_highway_coding.sas,
## 3) coding_overlap.sas,
## 4) import_highway_projects_2.sas

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

class HighwayNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.in_folder = os.path.join(mfhrn_path, "input")
        self.mhn_in_folder = os.path.join(self.in_folder, "1_travel")
        self.mhn_in_gdb = os.path.join(self.mhn_in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        years_csv_path = os.path.join(self.in_folder, "input_years.csv")
        years_list_raw = pd.read_csv(years_csv_path)["year"].to_list()

        # highway files - names of feature classes + tables in MHN
        self.hwy_files = [
            "hwynet/hwynet_arc",
            "hwynet/hwynet_node",
            "hwynet/hwyproj",
            "hwyproj_coding"
            ]
        
        # bus files - names of feature classes + tables in MHN
        self.bus_files = [
            "hwynet/bus_base",
            "hwynet/bus_current",
            "hwynet/bus_future",
            "bus_base_itin",
            "bus_current_itin",
            "bus_future_itin",
            "parknride" # not totally sure what this is. 
        ]

        self.rel_classes = [
            "rel_hwyproj_to_coding",
            "rel_arcs_to_hwyproj_coding",
            "rel_bus_base_to_itin",
            "rel_bus_current_to_itin",
            "rel_bus_future_to_itin",
            "rel_arcs_to_bus_base_itin",
            "rel_arcs_to_bus_current_itin",
            "rel_arcs_to_bus_future_itin",
            "rel_nodes_to_parknride"
        ]

        self.current_gdb = self.mhn_in_gdb

        self.hwylink_df = None
        self.hwynode_df = None
        self.hwyproj_df = None
        self.coding_df = None

        self.get_hwy_dfs()

        self.base_year = min(self.hwyproj_df.COMPLETION_YEAR.to_list()) - 1

        years_list = []
        for year in years_list_raw:
            if year >= self.base_year: 
                years_list.append(year)
        years_list.sort()

        self.years_list = years_list

        self.built_gdbs = []
        
    # MAIN METHODS --------------------------------------------------------------------------------

    # method that creates base hwy gdb
    def create_base_hwy(self):

        print("Copying base year...")

        # delete output folder + recreate it 
        mhn_out_folder = self.mhn_out_folder
        out_folder = os.path.dirname(mhn_out_folder)
        mhn_in_gdb = self.mhn_in_gdb
        base_year = self.base_year 

        if os.path.isdir(out_folder) == True:
            shutil.rmtree(out_folder)
        
        os.mkdir(out_folder)
        os.mkdir(mhn_out_folder)

        # copy GDB
        mhn_out_gdb = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        self.copy_gdb_safe(mhn_in_gdb, mhn_out_gdb)
        self.current_gdb = mhn_out_gdb # !!! update the HN's current gdb

        self.built_gdbs.append(self.current_gdb)
        self.del_rcs()
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")    

        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwyproj_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        # join years onto project coding
        arcpy.management.JoinField(coding_table, "TIPID", hwyproj_fc, "TIPID", "COMPLETION_YEAR")

        # add fields to review updates (a la Volpe)
        arcpy.management.AddField(hwynode_fc, "DESCRIPTION", "TEXT")
        arcpy.management.AddFields(hwylink_fc, [["NEW_BASELINK", "TEXT"], ["DESCRIPTION", "TEXT"], ["PROJECT", "TEXT"]])
        arcpy.management.CalculateField(
            in_table= hwylink_fc,
            field="NEW_BASELINK",
            expression="!BASELINK!",
            expression_type="PYTHON3",
            code_block="",
            field_type="TEXT",
            enforce_domains="NO_ENFORCE_DOMAINS")
        arcpy.management.AddField(hwyproj_fc, "DESCRIPTION", "TEXT")
        
        arcpy.management.AddFields(coding_table, [["PROCESS_NOTES", "TEXT"], ["USE", "SHORT"]])

        print("Base year copied and prepared for modification.\n")

    # method that checks the base feature classes
    def check_hwy_fcs(self):

        print("Checking feature classes for errors...")
        mhn_out_folder = self.mhn_out_folder

        self.get_hwy_dfs()
        hwynode_df = self.hwynode_df
        hwylink_df = self.hwylink_df 
        hwyproj_df = self.hwyproj_df

        # PK CHECK
        base_feature_class_errors = os.path.join(
            mhn_out_folder, 
            "base_feature_class_errors.txt")

        if os.path.exists(base_feature_class_errors):
            os.remove(base_feature_class_errors)

        error_file= open(base_feature_class_errors, "a")
        
        dup_fail = 0

        # nodes must be unique 
        node_counts = hwynode_df.NODE.value_counts()

        if (node_counts.max() > 1):
            bad_node_df = node_counts[node_counts > 1]
            error_file.write("These nodes violate unique NODE constraint.\n")
            error_file.write(bad_node_df.to_string() + "\n\n")
            dup_fail += 1

        # anode-bnode combinations must be unique
        ab_count_df = hwylink_df.groupby(["ANODE", "BNODE"]).size().reset_index()
        ab_count_df = ab_count_df.rename(columns = {0: "group_size"})
        
        if (ab_count_df["group_size"].max() > 1):
            bad_link_df = ab_count_df[ab_count_df["group_size"] >= 2]
            error_file.write("These links violate the unique ANODE-BNODE constraint.\n")
            error_file.write(bad_link_df.to_string() + "\n\n")
            dup_fail += 1

        # tipids should be unique 
        tipid_count_df = hwyproj_df.groupby(["TIPID"]).size().reset_index()
        tipid_count_df = tipid_count_df.rename(columns = {0:"group_size"})

        if (tipid_count_df["group_size"].max() > 1):
            bad_project_df = tipid_count_df[tipid_count_df["group_size"] >= 2]
            error_file.write("These projects violate the unique TIPID constraint.\n")
            error_file.write(bad_project_df.to_string() + "\n\n")
            dup_fail +=1

        if dup_fail > 0:
            sys.exit("Duplicates in the feature classes were found. Crashing program.")

        # create data structures now to be compared to later
        all_node_set = set(hwynode_df.NODE.to_list())
        link_node_set = set(hwylink_df.ANODE.to_list()) | set(hwylink_df.BNODE.to_list())

        hwylink_abb_df = hwylink_df[["ANODE", "BNODE", "ABB", "DIRECTIONS"]]
        hwylink_rev_df = pd.merge(hwylink_abb_df, hwylink_abb_df.copy(), 
                                  left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        hwylink_rev_set = set(hwylink_rev_df.ABB_x.to_list())

        coded_dict, range_dict = self.get_domain_dicts()

        # ROW CHECK

        # check nodes
        node_fail = 0
        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwynode_fields = ["NODE", "subzone17", "zone17", "capzone17", "IMArea", "DESCRIPTION"]

        # wipe description 
        arcpy.management.CalculateField(hwynode_fc, "DESCRIPTION", '" "', "PYTHON3")
        
        with arcpy.da.UpdateCursor(hwynode_fc, hwynode_fields) as ucursor:
            for row in ucursor:

                if row[0] not in link_node_set: # check that nodes are not disconnected
                    node_fail +=1
                    row[5] = "Error: node not connected to links"
                    ucursor.updateRow(row)
                    continue

        if node_fail > 0:
            error_file.write(f"{node_fail} nodes failed the individual row check. Check output node fc.\n")
        else:
            error_file.write("No nodes failed the individual row check.\n")
        
        # check links
        link_fail = 0

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()
        rev_lf_dict = {lf_dict[k]: k for k in lf_dict.keys()}

        link_domain_dict = self.get_field_domain_dict(hwylink_fc)

        desc_pos = lf_dict["DESCRIPTION"]

        # wipe description 
        arcpy.management.CalculateField(hwylink_fc, "DESCRIPTION", '" "', "PYTHON3")

        with arcpy.da.UpdateCursor(hwylink_fc, link_fields) as ucursor:
            for row in ucursor:

                # check that nodes are valid
                anode = row[lf_dict["ANODE"]]
                bnode = row[lf_dict["BNODE"]]
                if anode not in all_node_set or bnode not in all_node_set:
                    link_fail +=1
                    row[desc_pos] = "Error: ANODE or BNODE not in node fc"
                    ucursor.updateRow(row)
                    continue

                # check that ABB equals anode-bnode-baselink
                baselink = row[lf_dict["BASELINK"]]
                abb = row[lf_dict["ABB"]]
                correct_abb = f"{anode}-{bnode}-{baselink}"
                if abb != correct_abb:
                    link_fail +=1
                    row[desc_pos] = "Error: ABB is not equal to ANODE-BNODE-BASELINK"
                    ucursor.updateRow(row)
                    continue

                # check that directional links are valid
                dirs = row[lf_dict["DIRECTIONS"]]
                if abb in hwylink_rev_set and dirs != "1":
                    link_fail += 1
                    row[desc_pos] = "Error: this ABB must have directions = 1"
                    ucursor.updateRow(row)
                    continue

                # check for domain violations
                domain_violation = 0
                for pos in rev_lf_dict:

                    field = rev_lf_dict[pos]
                    domain = link_domain_dict[field]
                    
                    if domain in coded_dict:
                        coded_values = coded_dict[domain]
                        if row[pos] != None and row[pos] not in coded_values:
                            domain_violation += 1

                    elif domain in range_dict:
                        min_val, max_val = range_dict[domain]
                        if row[pos] < min_val or row[pos] > max_val:
                            domain_violation += 1

                if domain_violation > 0:
                    link_fail +=1
                    row[desc_pos] = "Error: Domain violation"
                    ucursor.updateRow(row)
                    continue

                type1 = row[lf_dict["TYPE1"]]
                type2 = row[lf_dict["TYPE2"]]
                ampm1 = row[lf_dict["AMPM1"]]
                ampm2 = row[lf_dict["AMPM2"]]
                speed1 = row[lf_dict["POSTEDSPEED1"]]
                speed2 = row[lf_dict["POSTEDSPEED2"]]
                lanes1 = row[lf_dict["THRULANES1"]]
                lanes2 = row[lf_dict["THRULANES2"]]
                feet1 = row[lf_dict["THRULANEWIDTH1"]]
                feet2 = row[lf_dict["THRULANEWIDTH2"]]
                parklanes1 = row[lf_dict["PARKLANES1"]]
                parklanes2 = row[lf_dict["PARKLANES2"]]
                parkres1 = row[lf_dict["PARKRES1"]]
                parkres2 = row[lf_dict["PARKRES2"]]
                buslanes1 = row[lf_dict["BUSLANES1"]]
                buslanes2 = row[lf_dict["BUSLANES2"]]
                sigic = row[lf_dict["SIGIC"]]
                cltl = row[lf_dict["CLTL"]]
                rrgradex = row[lf_dict["RRGRADECROSS"]]
                toll = row[lf_dict["TOLLDOLLARS"]]
                modes = row[lf_dict["MODES"]]
                vclearance = row[lf_dict["VCLEARANCE"]]

                # check that directions is not 0
                if dirs == "0":
                    link_fail += 1
                    row[desc_pos] = "Error: Directions must not be 0"
                    ucursor.updateRow(row)
                    continue

                # check that skeleton links don't have coded info
                zero_fields = [type1, type2, ampm1, ampm2, speed1, speed2, 
                               lanes1, lanes2, feet1, feet2, parklanes1, parklanes2, buslanes1, buslanes2, 
                               sigic, cltl, rrgradex, toll, modes, vclearance]
                
                if baselink == "0":
                    if any(str(field) != "0" for field in zero_fields):
                        link_fail += 1
                        row[desc_pos] = "Error: Skeleton links should not have project coded values"
                        ucursor.updateRow(row)
                        continue

                    if parkres1 != "-" or parkres2 != "-":
                        link_fail += 1
                        row[desc_pos] = "Error: Skeleton links cannot have PARKRES filled in"
                        ucursor.updateRow(row)
                        continue

                # check that existing links have all required fields filled in
                req_fields = [type1, ampm1, lanes1, feet1, modes]
                
                if baselink == "1":
                    if any(str(field) == "0" for field in req_fields):
                        link_fail += 1
                        row[desc_pos] = "Error: Missing required attribute(s) on link"
                        ucursor.updateRow(row)
                        continue
                    if type1 != "7" and speed1 == 0:
                        link_fail += 1
                        row[desc_pos] = "Error: Missing SPEED1 on link"
                        ucursor.updateRow(row)
                        continue

                # check that existing links with dirs = 1 or 2
                # only have 1 fields filled in
                all_fields2 = [type2, ampm2, speed2, lanes2, feet2, parklanes2, buslanes2]

                if baselink == "1" and dirs in ["1", "2"]:
                    if any(str(field) != "0" for field in all_fields2):
                        link_fail += 1
                        row[desc_pos] = f"Error: Unusable '2' attributes for this DIRECTIONS = {dirs} link"
                        ucursor.updateRow(row)
                        continue

                # check that existing links with dirs = 1
                # do not have parkres2 filled in
                if baselink == "1" and dirs == "1":
                    if parkres2 != "-" :
                        link_fail += 1
                        row[desc_pos] = f"Error: Unusable PARKRES2 on DIRECTIONS = 1 link"
                        ucursor.updateRow(row)
                        continue

                # check that existing links with dirs = 3 
                # have all 2 fields filled in
                req_fields2 = [type2, ampm2, speed2, lanes2]

                if baselink == "1" and dirs == "3":
                    if any(str(field) == "0" for field in req_fields2):
                        link_fail += 1
                        row[desc_pos] = f"Error: Missing required '2' attribute(s) on DIRECTIONS = 3 link"
                        ucursor.updateRow(row)
                        continue
                    if type2 != "7" and speed2 == 0:
                        link_fail += 1
                        row[desc_pos] = "Error: Missing SPEED2 on link"
                        ucursor.updateRow(row)
                        continue

                # check parkres != 0
                if parkres1 == "0" or parkres2 == "0":
                    link_fail += 1
                    row[desc_pos] = "Error: '0' is reserved for CHANGE_PARKRES. Did you mean '-'?"
                    ucursor.updateRow(row)
                    continue

                # check vclearance != -1 
                if vclearance < 0:
                    link_fail += 1
                    row[desc_pos] = "Error: VCLEARANCE cannot be negative."
                    ucursor.updateRow(row)
                    continue

                # check toll is correct
                try:
                    static_toll = float(toll)
                except:
                    dynamic_toll = toll.split()

                    if len(dynamic_toll) != 8:
                        link_fail += 1
                        row[desc_pos] = "Error: Toll must be a decimal or a string of 8 decimals"
                        ucursor.updateRow(row)
                        continue
                    
                    try:
                        dynamic_toll = [float(tod_toll) for tod_toll in dynamic_toll]
                    except:
                        link_fail += 1
                        row[desc_pos] = "Error: Toll must be a decimal or a string of 8 decimals"
                        ucursor.updateRow(row)
                        continue

        if link_fail > 0:
            error_file.write(f"{link_fail} links failed the individual row check. Check output link fc.\n")
        else:
            error_file.write("No links failed the individual row check.\n")

        if node_fail != 0 or link_fail != 0:
            sys.exit(f"There are {node_fail} nodes with issues and {link_fail} links with issues. Crashing program.")

        # CONNECTIVITY CHECK

        error_file.close()

        os.remove(base_feature_class_errors)

        print("Base feature classes checked for errors.\n")

    # method that imports highway project coding
    def import_hwyproj_coding(self): 

        print("Importing highway project coding...")

        mhn_in_folder = self.mhn_in_folder
        mhn_out_folder = self.mhn_out_folder
        hwylink_df = self.hwylink_df 

        import_path = os.path.join(mhn_in_folder, "import_hwyproj_coding.xlsx")

        if not os.path.exists(import_path):
            print("No highway projects imported. Checking base table for integrity.\n")
            return

        import_df = pd.read_excel(import_path)

        import_df = import_df.dropna(how = "all")

        if len(import_df) == 0:
            print("No highway projects imported. Checking base table for integrity.\n")
            return

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
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")    
        fields = ["TIPID", "ABB"]
        existing_list = []

        with arcpy.da.SearchCursor(coding_table, fields) as scursor:

            for row in scursor:
                existing_list.append((row[0], row[1]))

        delete_rows = []
        update_rows = {}
        insert_rows = []

        for record in import_records:
            tipid = record["tipid"]
            abb = record["abb"]

            if record["remove"] == "Y":
                delete_rows.append((tipid, abb))
            elif (tipid, abb) in existing_list:
                update_rows[(tipid, abb)] = record
            else:
                insert_rows.append(record)

        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()

        # update and delete rows
        with arcpy.da.UpdateCursor(coding_table, coding_fields) as ucursor:
            for row in ucursor:
                    
                tipid = row[cf_dict["TIPID"]]
                abb = row[cf_dict["ABB"]]

                if (tipid, abb) in update_rows:

                    new_attrs = update_rows[tipid, abb]
                    row[cf_dict["ACTION_CODE"]] = new_attrs["action"]
                    row[cf_dict["NEW_DIRECTIONS"]] = new_attrs["directions"]
                    row[cf_dict["NEW_TYPE1"]] = new_attrs["type1"]
                    row[cf_dict["NEW_TYPE2"]] = new_attrs["type2"]
                    row[cf_dict["NEW_AMPM1"]] = new_attrs["ampm1"]
                    row[cf_dict["NEW_AMPM2"]] = new_attrs["ampm2"]
                    row[cf_dict["NEW_POSTEDSPEED1"]] = new_attrs["speed1"]
                    row[cf_dict["NEW_POSTEDSPEED2"]] = new_attrs["speed2"]
                    row[cf_dict["NEW_THRULANES1"]] = new_attrs["lanes1"]
                    row[cf_dict["NEW_THRULANES2"]] = new_attrs["lanes2"]
                    row[cf_dict["NEW_THRULANEWIDTH1"]] = new_attrs["feet1"]
                    row[cf_dict["NEW_THRULANEWIDTH2"]] = new_attrs["feet2"]
                    row[cf_dict["ADD_PARKLANES1"]] = new_attrs["parklanes1"]
                    row[cf_dict["ADD_PARKLANES2"]] = new_attrs["parklanes2"]
                    row[cf_dict["CHANGE_PARKRES1"]] = new_attrs["parkres1"]
                    row[cf_dict["CHANGE_PARKRES2"]] = new_attrs["parkres2"]
                    row[cf_dict["ADD_BUSLANES1"]] = new_attrs["buslanes1"]
                    row[cf_dict["ADD_BUSLANES2"]] = new_attrs["buslanes2"]
                    row[cf_dict["ADD_SIGIC"]] = new_attrs["sigic"]
                    row[cf_dict["ADD_CLTL"]] = new_attrs["cltl"]
                    row[cf_dict["ADD_RRGRADECROSS"]] = new_attrs["rrgradex"]
                    row[cf_dict["NEW_TOLLDOLLARS"]] = new_attrs["tolldollars"]
                    row[cf_dict["NEW_MODES"]] = new_attrs["modes"]
                    row[cf_dict["NEW_VCLEARANCE"]] = new_attrs["vclearance"]
                    row[cf_dict["PROCESS_NOTES"]] = "Updated from import successfully."

                    ucursor.updateRow(row)

                elif (tipid, abb) in delete_rows:
                    ucursor.deleteRow()
        
        # insert rows
        # have to calculate completion year
        hwyproj_df = self.hwyproj_df
        years_dict = hwyproj_df.set_index("TIPID")["COMPLETION_YEAR"].to_dict()

        with arcpy.da.InsertCursor(coding_table, coding_fields) as icursor:

            for new_attrs in insert_rows:

                row = [None] * len(coding_fields)

                tipid = new_attrs["tipid"]

                row[cf_dict["TIPID"]] = tipid
                row[cf_dict["ABB"]] = new_attrs["abb"]
                row[cf_dict["ACTION_CODE"]] = new_attrs["action"]
                row[cf_dict["NEW_DIRECTIONS"]] = new_attrs["directions"]
                row[cf_dict["NEW_TYPE1"]] = new_attrs["type1"]
                row[cf_dict["NEW_TYPE2"]] = new_attrs["type2"]
                row[cf_dict["NEW_AMPM1"]] = new_attrs["ampm1"]
                row[cf_dict["NEW_AMPM2"]] = new_attrs["ampm2"]
                row[cf_dict["NEW_POSTEDSPEED1"]] = new_attrs["speed1"]
                row[cf_dict["NEW_POSTEDSPEED2"]] = new_attrs["speed2"]
                row[cf_dict["NEW_THRULANES1"]] = new_attrs["lanes1"]
                row[cf_dict["NEW_THRULANES2"]] = new_attrs["lanes2"]
                row[cf_dict["NEW_THRULANEWIDTH1"]] = new_attrs["feet1"]
                row[cf_dict["NEW_THRULANEWIDTH2"]] = new_attrs["feet2"]
                row[cf_dict["ADD_PARKLANES1"]] = new_attrs["parklanes1"]
                row[cf_dict["ADD_PARKLANES2"]] = new_attrs["parklanes2"]
                row[cf_dict["CHANGE_PARKRES1"]] = new_attrs["parkres1"]
                row[cf_dict["CHANGE_PARKRES2"]] = new_attrs["parkres2"]
                row[cf_dict["ADD_BUSLANES1"]] = new_attrs["buslanes1"]
                row[cf_dict["ADD_BUSLANES2"]] = new_attrs["buslanes2"]
                row[cf_dict["ADD_SIGIC"]] = new_attrs["sigic"]
                row[cf_dict["ADD_CLTL"]] = new_attrs["cltl"]
                row[cf_dict["ADD_RRGRADECROSS"]] = new_attrs["rrgradex"]
                row[cf_dict["NEW_TOLLDOLLARS"]] = new_attrs["tolldollars"]
                row[cf_dict["NEW_MODES"]] = new_attrs["modes"]
                row[cf_dict["NEW_VCLEARANCE"]] = new_attrs["vclearance"]
                
                if tipid in years_dict:
                    row[cf_dict["COMPLETION_YEAR"]] = years_dict[tipid]
                else:
                    row[cf_dict["COMPLETION_YEAR"]] = 0

                row[cf_dict["PROCESS_NOTES"]] = "Inserted from import successfully"

                icursor.insertRow(row)

        print("Highway project coding imported.\n")

    # method that checks the project coding table
    def check_hwyproj_coding_table(self):

        print("Checking base project table for errors...")
        
        mhn_out_folder = self.mhn_out_folder
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        arcpy.management.CalculateField(coding_table, "USE", "1")

        self.get_hwy_dfs() # update the HN's current dfs 

        hwylink_df = self.hwylink_df
        coding_df = self.coding_df

        base_project_table_errors = os.path.join(
            mhn_out_folder, 
            "base_project_table_errors.txt")
        
        if os.path.exists(base_project_table_errors):
            os.remove(base_project_table_errors)

        error_file= open(base_project_table_errors, "a") # open error file, don't forget to close it!

        # create hwylink data structures now to be compared to later
        
        conn_list = hwylink_df[hwylink_df.TYPE1 == "6"].ABB.to_list()

        tipid_list = self.hwyproj_df.TIPID.to_list()
        abb_list = hwylink_df.ABB.to_list()

        hwylink_abb_df = hwylink_df[["ANODE", "BNODE", "BASELINK", "ABB"]]
        hwylink_dup_df = pd.merge(hwylink_abb_df, hwylink_abb_df.copy(), left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        hwylink_dup_set = set(hwylink_dup_df.ABB_x.to_list() + hwylink_dup_df.ABB_y.to_list())

        coded_dict, range_dict = self.get_domain_dicts()

        # wipe Process Notes field
        arcpy.management.CalculateField(coding_table, "PROCESS_NOTES", '" "', "PYTHON3")

        # primary key check
        # check that no TIPID + ABB is duplicated. 
        hwyproj_group_df = coding_df.groupby(["TIPID", "ABB"]).size().reset_index()
        hwyproj_group_df = hwyproj_group_df.rename(columns = {0: "group_size"})
        hwyproj_dup_dict = hwyproj_group_df[hwyproj_group_df["group_size"] > 1].to_dict("records")

        dup_set = set()
        for dup in hwyproj_dup_dict: 
            dup_set.add((dup["TIPID"], dup["ABB"]))

        # don't use the duplicates
        fields = ["TIPID", "ABB", "USE", "PROCESS_NOTES"]
        dup_fail = 0
        with arcpy.da.UpdateCursor(coding_table, fields) as ucursor:
            for row in ucursor:
                if (row[0], row[1]) in dup_set:
                    row[2] = 0
                    row[3] = "Error: Duplicate TIPID-ABB combination. Must be unique."
                    dup_fail+=1
                    ucursor.updateRow(row)
        
        error_file.write("Primary key check:\n")
        if dup_fail != 0:
            error_file.write(f"{dup_fail} rows failed the duplicate check and had USE set to 0. Check output coding table.\n\n")
        else:
            error_file.write("No rows failed the duplicate check.\n\n")

        # check row by row 
        # don't have to check the validity of already discarded rows
        # just count the rows with mistakes

        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()
        rev_cf_dict = {cf_dict[k]: k for k in cf_dict.keys()}

        coding_domain_dict = self.get_field_domain_dict(coding_table)

        # get indices of the project coding fields
        use_pos = cf_dict["USE"]
        notes_pos = cf_dict["PROCESS_NOTES"]

        row_fail = 0
        row_warning = 0
        where_clause = "USE = 1"

        with arcpy.da.UpdateCursor(coding_table, coding_fields, where_clause) as ucursor:
            for row in ucursor:
                tipid = row[cf_dict["TIPID"]] 
                abb = row[cf_dict["ABB"]]

                # check that TIPID is valid 
                if tipid not in tipid_list: 
                    row_fail+=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: TIPID is not a legitimate project."
                    ucursor.updateRow(row)
                    continue

                # check that ABB is valid
                if abb not in abb_list:
                    row_fail+=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: ABB is not an actual link."
                    ucursor.updateRow(row)
                    continue

                # check for domain violations
                domain_violation = 0
                for pos in rev_cf_dict:

                    field = rev_cf_dict[pos]
                    domain = coding_domain_dict[field]

                    if domain in coded_dict:
                        coded_values = coded_dict[domain]
                        if row[pos] not in coded_values:
                            domain_violation += 1
                    
                    elif domain in range_dict:
                        min_val, max_val = range_dict[domain]
                        if row[pos] < min_val or row[pos] > max_val:
                            domain_violation += 1

                if domain_violation > 0:
                    link_fail += 1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: Domain violation"
                    ucursor.updateRow(row)
                    continue

                action = row[cf_dict["ACTION_CODE"]]

                # check that action codes are valid
                baselink = abb[-1]
                if baselink == "0" and action != "4":
                    row_fail+=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: Skeleton links cannot have action codes 1 or 3 applied to them."
                    ucursor.updateRow(row)
                    continue

                if baselink == "1" and action not in ["1", "3"]:
                    row_fail+=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: Regular links cannot have action code 4 applied to them."
                    ucursor.updateRow(row)
                    continue

                ndirs = row[cf_dict["NEW_DIRECTIONS"]]
                ntype1 = row[cf_dict["NEW_TYPE1"]]
                ntype2 = row[cf_dict["NEW_TYPE2"]]
                nampm1 = row[cf_dict["NEW_AMPM1"]]
                nampm2 = row[cf_dict["NEW_AMPM2"]]
                nspeed1 = row[cf_dict["NEW_POSTEDSPEED1"]]
                nspeed2 = row[cf_dict["NEW_POSTEDSPEED2"]]
                nlanes1 = row[cf_dict["NEW_THRULANES1"]]
                nlanes2 = row[cf_dict["NEW_THRULANES2"]]
                nfeet1 = row[cf_dict["NEW_THRULANEWIDTH1"]]
                nfeet2 = row[cf_dict["NEW_THRULANEWIDTH2"]]
                aparklanes1 = row[cf_dict["ADD_PARKLANES1"]]
                aparklanes2 = row[cf_dict["ADD_PARKLANES2"]]
                cparkres1 = row[cf_dict["CHANGE_PARKRES1"]]
                cparkres2 = row[cf_dict["CHANGE_PARKRES2"]]
                abuslanes1 = row[cf_dict["ADD_BUSLANES1"]]
                abuslanes2 = row[cf_dict["ADD_BUSLANES2"]]
                asigic = row[cf_dict["ADD_SIGIC"]]
                acltl = row[cf_dict["ADD_CLTL"]]
                arrgradex = row[cf_dict["ADD_RRGRADECROSS"]]
                ntoll = row[cf_dict["NEW_TOLLDOLLARS"]]
                nmodes = row[cf_dict["NEW_MODES"]]
                nvclearance = row[cf_dict["NEW_VCLEARANCE"]]

                # check for valid action code = 3
                attributes = [
                    ndirs, ntype1, ntype2, nampm1, nampm2, 
                    nspeed1, nspeed2, nlanes1, nlanes2, nfeet1, nfeet2, 
                    aparklanes1, aparklanes2, cparkres1, cparkres2, 
                    abuslanes1, abuslanes2, asigic, acltl, arrgradex, 
                    ntoll, nmodes, nvclearance]

                if action == "3" and any(str(attribute) != "0" for attribute in attributes):
                    row_fail+=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: Action Code 3 cannot have other attributes filled in."
                    ucursor.updateRow(row)
                    continue

                # check for valid toll 
                try:
                    static_ntoll = float(ntoll)
                except:
                    dynamic_ntoll = ntoll.split()

                    if len(dynamic_ntoll) != 8:
                        row_fail +=1
                        row[use_pos] = 0
                        row[notes_pos] = "Error: Toll must be a decimal or a string of 8 decimals"
                        ucursor.updateRow(row)
                        continue

                    try:
                        dynamic_ntoll = [float(tod_ntoll) for tod_ntoll in dynamic_ntoll]
                    except:
                        row_fail += 1
                        row[use_pos] = 0
                        row[notes_pos] = "Error: Toll must be a decimal or a string of 8 decimals"
                        ucursor.updateRow(row)
                        continue

                # if action code = 4
                # required attributes must be filled in
                req_fields = [ndirs, ntype1, nampm1, nlanes1, nfeet1, nmodes]
                if action == "4":
                    if any(str(field) == "0" for field in req_fields):
                        row_fail+=1
                        row[use_pos] = 0 
                        row[notes_pos] = "Error: Missing required attribute(s) on new link."
                        ucursor.updateRow(row)
                        continue
                    elif ntype1 != "7" and nspeed1 == 0:
                        row_fail+=1
                        row[use_pos] = 0 
                        row[notes_pos] = "Error: Missing SPEED1 on new link."
                        ucursor.updateRow(row)
                        continue

                # if action code = 1 or 4 and directions = 1 or 2
                # then all 2 fields should be 0 
                all_fields2 = [ntype2, nampm2, nspeed2, nlanes2, 
                               nfeet2, aparklanes2, cparkres2, abuslanes2]

                if action in ["1", "4"] and ndirs in ["1", "2"]:
                    if any(str(field2) != "0" for field2 in all_fields2):
                        row_fail+=1
                        row[use_pos] = 0 
                        row[notes_pos] = f"Error: Unusable '2' attributes on NEW_DIRECTIONS = {ndirs} link."
                        ucursor.updateRow(row)
                        continue

                # if action code = 1 or 4 and directions = 3 
                # then required 2 fields must be filled in
                req_fields2 = [ntype2, nampm2, nlanes2, nfeet2]

                if action in ["1", "4"] and ndirs == "3":
                    if any(str(field2) == "0" for field2 in req_fields2):
                        row_fail+=1
                        row[use_pos] = 0 
                        row[notes_pos] = "Error: Missing '2' attributes on NEW_DIRECTIONS = 3 link."
                        ucursor.updateRow(row)
                        continue 
                    elif ntype2 != "7" and nspeed2 == 0:
                        row_fail+=1
                        row[use_pos] = 0
                        row[notes_pos] = "Error: Missing speed 2 on NEW_DIRECTIONS = 3 link."
                        ucursor.updateRow(row)
                        continue

                # if link has potential for duplication
                # cannot set new directions > 1 
                if ndirs in ["2", "3"] and abb in hwylink_dup_set:
                    row_fail+=1
                    row[use_pos] = 0 
                    row[notes_pos] = "Error: cannot set NEW_DIRECTIONS > to 2 or 3 or else issue with duplication."
                    ucursor.updateRow(row)
                    continue

                # centroid connectors should not have project coding 
                if abb in conn_list:
                    row_fail +=1
                    row[use_pos] = 0
                    row[notes_pos] = "Error: cannot apply coding to centroid connectors."
                    ucursor.updateRow(row)
                    continue

                # action code 1 should have at least 1 attribute filled in. 
                if action == "1" and all(str(attribute) == "0" for attribute in attributes):
                    row_warning +=1
                    row[notes_pos] = "Warning: Action Code 1 should make at least one modification."
                    ucursor.updateRow(row)
                    continue

            # out of for loop

        # out of cursor
        error_file.write("Individual row check:\n")
        if row_fail != 0:
            error_file.write(f"{row_fail} rows failed the individual row check and had USE set to 0. Check output coding table.\n")
        else:
            error_file.write("No rows failed the individual row check.\n")

        if row_warning != 0:
            error_file.write(f"{row_warning} rows passed the individual row check with warnings. Check output coding table.\n\n")
        else:
            error_file.write("\n")

        # row combo check
        self.get_hwy_dfs()

        coding_df = self.coding_df
        applied_df = coding_df[(coding_df.COMPLETION_YEAR != 9999) & (coding_df.USE == 1)]

        error_file.write("Row combo check:\n")

        # find links which have multiple actions applied in a single year
        year_edits_df = applied_df.groupby(["ABB", "COMPLETION_YEAR"]).size().reset_index()
        year_edits_df = year_edits_df.rename(columns = {0: "group_size"})
        year_edits_df = year_edits_df[year_edits_df.group_size >= 2]

        year_edits_dict = year_edits_df.set_index(["ABB", "COMPLETION_YEAR"]).to_dict("index")
        
        fields = ["ABB", "COMPLETION_YEAR", "PROCESS_NOTES"]
        with arcpy.da.UpdateCursor(coding_table, fields) as ucursor:
            for row in ucursor:
                
                if (row[0], row[1]) in year_edits_dict:

                    row[2] = f"Warning: Multiple actions were applied to this link in a single year."
                    ucursor.updateRow(row)

        if len(year_edits_dict) > 0:

            message = f"{len(year_edits_dict)} links exist where multiple actions were applied in a single year. "
            message += "Check output coding table.\n"
            error_file.write(message)
        
        # find dead skeleton links
        self.get_hwy_dfs()
        hwylink_df = self.hwylink_df
        coding_df = self.coding_df

        skeleton_links_list = hwylink_df[hwylink_df.BASELINK == "0"].ABB.to_list()
        add_links_set = set(coding_df[coding_df.ACTION_CODE == "4"].ABB.to_list())

        dead_links_list = [link for link in skeleton_links_list if link not in add_links_set]

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        fields = ["ABB", "DESCRIPTION"]

        with arcpy.da.UpdateCursor(hwylink_fc, fields) as ucursor:
            for row in ucursor:

                if row[0] in dead_links_list:
                    row[1] = "Dead skeleton link is never added."
                    ucursor.updateRow(row)

        if len(dead_links_list) > 0:
            message = f"{len(dead_links_list)} skeleton links exist which are never added. "
            message += "Check output link fc.\n"
            error_file.write(message)

        error_file.close()

        # write problematic rows to error file

        xl_path = os.path.join(mhn_out_folder, "base_project_table_errors.xlsx")

        rename_dict = {
            "TIPID": "tipid",
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
            "CHANGE_PARKRES1": "parkres1",
            "CHANGE_PARKRES2": "parkres2",
            "ADD_BUSLANES1": "buslanes1",
            "ADD_BUSLANES2": "buslanes2",
            "ADD_SIGIC": "sigic",
            "ADD_CLTL": "cltl",
            "ADD_RRGRADECROSS": "rrgradex",
            "NEW_TOLLDOLLARS": "tolldollars",
            "NEW_MODES": "modes",
            "NEW_VCLEARANCE": "vclearance"
        }

        hwyproj_xl_df = coding_df[coding_df.PROCESS_NOTES.notnull()].rename(columns = rename_dict)
        hwyproj_xl_df["anode"] = hwyproj_xl_df["ABB"].apply(lambda x: x.split("-")[0])
        hwyproj_xl_df["bnode"] = hwyproj_xl_df["ABB"].apply(lambda x: x.split("-")[1])
        hwyproj_xl_df["remove"] = None

        hwyproj_xl_df = hwyproj_xl_df.sort_values(["PROCESS_NOTES"])
        
        col_order = ["tipid", "anode", "bnode", "action", "directions",
                     "type1", "type2", "ampm1", "ampm2", "speed1", "speed2", 
                     "lanes1", "lanes2", "feet1", "feet2",
                     "parklanes1", "parklanes2", "parkres1", "parkres2",
                     "buslanes1", "buslanes2", "sigic", "cltl", "rrgradex",
                     "tolldollars", "modes", "vclearance", "remove",
                     "ABB", "COMPLETION_YEAR", "PROCESS_NOTES", "USE"]
        
        hwyproj_xl_df = hwyproj_xl_df[col_order]

        hwyproj_xl_df.to_excel(xl_path, index = False)
        print("Base highway project table checked for errors.\n")

    # method that builds future highways
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

    # method that cleans up the output gdbs 
    def finalize_hwy_data(self):

        print("Finalizing highway data...")

        built_gdbs = self.built_gdbs

        for gdb in built_gdbs:
            
            hwylink_fc = os.path.join(gdb, "hwynet/hwynet_arc")
            hwynode_fc = os.path.join(gdb, "hwynet/hwynet_node")
            coding_table = os.path.join(gdb, "hwyproj_coding")
            hwyproj_fc = os.path.join(gdb, "hwynet/hwyproj")

            # remove deleted links
            # change abb to reflect new baselink

            remaining_nodes = set()
            abb_to_new_abb = {}
            fields = ["BASELINK", "NEW_BASELINK", "ANODE", "BNODE", "ABB"]

            with arcpy.da.UpdateCursor(hwylink_fc, fields) as ucursor:
                for row in ucursor:

                    if row[0] == "1" and row[1] == "0":
                        abb = row[4]
                        ucursor.deleteRow()

                    else:
                        row[0] = row[1] # baselink = new baselink 

                        abb = row[4]
                        new_abb = f"{row[2]}-{row[3]}-{row[0]}"

                        abb_to_new_abb[abb] = new_abb
                        remaining_nodes.update([row[2], row[3]])

                        row[4] = new_abb
                        ucursor.updateRow(row)

            # delete nodes which are not connected to links
            with arcpy.da.UpdateCursor(hwynode_fc, ["NODE"]) as ucursor:
                for row in ucursor:

                    if row[0] not in remaining_nodes:
                        ucursor.deleteRow()

            # in project table, drop use = 0 
            # else, update its abb
            fields = ["USE", "ABB"]
            with arcpy.da.UpdateCursor(coding_table, fields) as ucursor:
                for row in ucursor:

                    # if row[0] == 0 or row[1] in deleted_links:
                    if row[0] == 0:
                        ucursor.deleteRow()

                    else:
                        abb = row[1]
                        row[1] = abb_to_new_abb[abb]
                        ucursor.updateRow(row)

            # make an fc of the remaining projects
            arcpy.management.CreateFeatureclass(gdb, "coding_remaining", "POLYLINE", spatial_reference = 26771)
            coding_remaining = os.path.join(gdb, "coding_remaining")
            arcpy.management.AddFields(coding_remaining, [["TIPID", "TEXT"], ["ABB", "TEXT"]])

            with arcpy.da.SearchCursor(coding_table, ["TIPID", "ABB"]) as scursor:
                with arcpy.da.InsertCursor(coding_remaining, ["TIPID", "ABB"]) as icursor:

                    for row in scursor:
                        icursor.insertRow(row)

            geom_fields = ["SHAPE@", "ABB"]
            with arcpy.da.UpdateCursor(coding_remaining, geom_fields) as ucursor:
                for u_row in ucursor:

                    abb = u_row[1]

                    geom = None
                    where_clause = f"ABB = '{abb}'"

                    with arcpy.da.SearchCursor(hwylink_fc, geom_fields, where_clause) as scursor:
                        for s_row in scursor:

                            geom = s_row[0]
                            
                    ucursor.updateRow([geom, abb])
            
            # dissolve projects and get geometries
            hwyproj_dissolve = os.path.join(gdb, "hwyproj_dissolve")
            arcpy.management.Dissolve(coding_remaining, hwyproj_dissolve, ["TIPID"])

            geom_dict = {}

            with arcpy.da.SearchCursor(hwyproj_dissolve, ["TIPID", "SHAPE@"]) as scursor:
                for row in scursor:
                    
                    tipid = row[0]
                    geom = row[1]

                    multi_array = arcpy.Array()
                    for part in geom:
                        
                        part_array = arcpy.Array([point for point in part])
                        multi_array.append(part_array)

                    polyline = arcpy.Polyline(multi_array, spatial_reference = 26771)
                    geom_dict[tipid] = polyline

            # in original hwyproj fc, drop if deleted
            # else update its geometry

            with arcpy.da.UpdateCursor(hwyproj_fc, ["TIPID", "SHAPE@"]) as ucursor:
                for row in ucursor:

                    tipid = row[0]

                    if tipid in geom_dict:
                        row[1] = geom_dict[tipid]
                        ucursor.updateRow(row)

                    else:
                        ucursor.deleteRow()

            arcpy.management.Delete(coding_remaining)
            arcpy.management.Delete(hwyproj_dissolve)
            
            arcpy.management.DeleteField(hwynode_fc, ["DESCRIPTION"])
            arcpy.management.DeleteField(hwylink_fc, ["NEW_BASELINK", "DESCRIPTION", "PROJECT"])
            arcpy.management.DeleteField(hwyproj_fc, ["DESCRIPTION"])
            arcpy.management.DeleteField(coding_table, ["COMPLETION_YEAR", "PROCESS_NOTES", "USE"])
            
            self.add_rcs()

            print("Highway data finalized.\n")

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method to get fields 
    def get_hwy_fields(self):

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if (f.type!="Geometry" and f.name != "OBJECTID")]
        lf_dict = {field: index for index, field in enumerate(link_fields)}

        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")
        coding_fields = [f.name for f in arcpy.ListFields(coding_table) if f.name != "OBJECTID"]
        cf_dict = {field: index for index, field in enumerate(coding_fields)}

        return link_fields, lf_dict, coding_fields, cf_dict
    
    # helper method to get dfs 
    def get_hwy_dfs(self):

        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        self.hwylink_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwylink_fc, link_fields)], 
            columns = link_fields)
        
        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode_fc) if (f.type!="Geometry" and f.name != "OBJECTID")]
        self.hwynode_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode_fc, hwynode_fields)], 
            columns = hwynode_fields)
        
        hwyproj_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")
        hwyproj_fields = [f.name for f in arcpy.ListFields(hwyproj_fc) if (f.type!="Geometry" and f.name != "OBJECTID")]
        self.hwyproj_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwyproj_fc, hwyproj_fields)],
            columns = hwyproj_fields)
        
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")
        self.coding_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(coding_table, coding_fields)], 
            columns = coding_fields)
        
    # helper method to delete relationship classes
    def del_rcs(self):

        for rc in self.rel_classes:

            rc_path = os.path.join(self.current_gdb, rc)
            if arcpy.Exists(rc_path):
                arcpy.management.Delete(rc_path)

    # helper method that copies a gdb
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
            ''
        
    # helper method to get fields domains
    def get_field_domain_dict(self, fc):

        fc_path = os.path.join(self.current_gdb, fc)
        fields = arcpy.ListFields(fc_path)

        field_domain_dict = {}

        for field in fields:
            field_domain_dict[field.name] = field.domain

        return field_domain_dict

    # helper method to get domains
    def get_domain_dicts(self):

        domains = arcpy.da.ListDomains(self.current_gdb)

        coded_dict = {}
        range_dict = {}

        for domain in domains:
            if domain.domainType == "CodedValue":
                coded_dict[domain.name] = domain.codedValues
            elif domain.domainType == "Range":
                range_dict[domain.name] = domain.range

        return coded_dict, range_dict

    # helper method that subsets to certain projects
    def subset_to_projects(self):

        print("Subsetting to projects...")

        mhn_in_folder = self.mhn_in_folder
        subset_hwy_path = os.path.join(mhn_in_folder, "subset_hwy_projects.csv")

        subset_df = pd.read_csv(subset_hwy_path).drop_duplicates()

        all_tipid_list = subset_df[subset_df.ABB == "all"].TIPID.to_list()
        spec_abb_list = list(subset_df[subset_df.ABB != "all"].set_index(["TIPID", "ABB"]).index.values)

        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        fields = ["TIPID", "ABB", "USE"]

        with arcpy.da.UpdateCursor(coding_table, fields) as ucursor:
            for row in ucursor:

                tipid = row[0]
                abb = row[1]

                tipid_abb = (tipid, abb)
                
                if tipid not in all_tipid_list and tipid_abb not in spec_abb_list:
                    row[2] = 0
                    ucursor.updateRow(row)

        print("Subset complete.\n")

    # helper method that copies highway links into the combined gdb
    def copy_hwy_links(self):

        mhn_out_folder = self.mhn_out_folder
        mhn_all_name = "MHN_all.gdb"
        mhn_all_gdb = os.path.join(mhn_out_folder, mhn_all_name)

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        link_workspace = os.path.join(mhn_all_gdb, "hwylinks_all")

        base_links_name = f"HWYLINK_{self.base_year}"
        arcpy.management.CreateFeatureclass(link_workspace, base_links_name, template = hwylink_fc, spatial_reference = 26771)
        base_links = os.path.join(link_workspace, base_links_name)

        link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if f.name != "OBJECTID" and f.type != "Geometry"]
        link_fields += ["SHAPE@"]

        with arcpy.da.SearchCursor(hwylink_fc, link_fields) as scursor:
            with arcpy.da.InsertCursor(base_links, link_fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

    # helper method that creates a gdb of all built years 
    def create_combined_gdb(self):

        print("Creating combined gdb...")

        final_year = max(self.years_list)
        mhn_out_folder = self.mhn_out_folder

        # create a combined gdb to store + check final output 
        mhn_all_gdb = os.path.join(mhn_out_folder, "MHN_all.gdb")
        self.copy_gdb_safe(self.current_gdb, mhn_all_gdb)

        for file in (self.hwy_files + self.bus_files):
            arcpy.management.Delete(os.path.join(mhn_all_gdb, file))
        arcpy.management.Delete(os.path.join(mhn_all_gdb, "hwynet"))

        # copy relevant project coding
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")
        applied_table = os.path.join(mhn_all_gdb, "coding_applied")

        where_clause = f"USE = 1 AND COMPLETION_YEAR <= {final_year}"
        arcpy.management.MakeTableView(coding_table, "coding_view", where_clause)
        arcpy.management.CopyRows("coding_view", applied_table)

        # create table of links which had multiple actions applied 
        multiple_table = os.path.join(mhn_all_gdb, "coding_multiple")
        arcpy.management.Sort(applied_table, multiple_table, "COMPLETION_YEAR")

        coding_df = self.coding_df 
        coding_df_use = coding_df[(coding_df.USE == 1) & (coding_df.COMPLETION_YEAR <= final_year)]
        multiple_dict = coding_df_use.ABB.value_counts()[coding_df_use.ABB.value_counts() >= 2].to_dict()

        with arcpy.da.UpdateCursor(multiple_table, "ABB") as ucursor:
            for row in ucursor:

                if row[0] not in multiple_dict:
                    ucursor.deleteRow()

        # copy the nodes 
        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        arcpy.management.CreateFeatureclass(mhn_all_gdb, "hwynode_all", template = hwynode_fc, spatial_reference = 26771)
        hwynode_all = os.path.join(mhn_all_gdb, "hwynode_all")

        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode_fc) if f.name != "OBJECTID" and f.type != "Geometry"]
        hwynode_fields += ["SHAPE@XY"]

        with arcpy.da.SearchCursor(hwynode_fc, hwynode_fields) as scursor:
            with arcpy.da.InsertCursor(hwynode_all, hwynode_fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

        # create a feature dataset to store the links 
        # then copy over the base links
        arcpy.management.CreateFeatureDataset(mhn_all_gdb, "hwylinks_all", spatial_reference = 26771)
        self.copy_hwy_links()

        # add relationship classes for ease of checking
        base_links_fc = os.path.join(mhn_all_gdb, "hwylinks_all", f"HWYLINK_{self.base_year}")
        rel_arcs_to_applied = os.path.join(mhn_all_gdb, "rel_arcs_to_applied")
        rel_arcs_to_multiple = os.path.join(mhn_all_gdb, "rel_arcs_to_multiple")

        arcpy.management.CreateRelationshipClass(
            base_links_fc, applied_table, rel_arcs_to_applied,
            "SIMPLE", "coding_applied", "base_arcs", "NONE", "ONE_TO_MANY", 
            "NONE", "ABB", "ABB")
        
        arcpy.management.CreateRelationshipClass(
            base_links_fc, multiple_table, rel_arcs_to_multiple,
            "SIMPLE", "coding_multiple", "base_arcs", "NONE", "ONE_TO_MANY", 
            "NONE", "ABB", "ABB")

    # helper method that moves the base year up one year
    def hwy_forward_one_year(self):
        
        current_year = self.base_year + 1 

        self.get_hwy_dfs()
        coding_df = self.coding_df[self.coding_df.USE == 1] # Only want the valid projects

        year_projects = coding_df[coding_df.COMPLETION_YEAR == current_year]
        year_projects = year_projects.set_index(["TIPID", "ABB"]).drop(columns = ["USE", "PROCESS_NOTES"])
        action_1_dict = year_projects[year_projects.ACTION_CODE == "1"].to_dict("index")
        action_3_dict = year_projects[year_projects.ACTION_CODE == "3"].to_dict("index")
        action_4_dict = year_projects[year_projects.ACTION_CODE == "4"].to_dict("index")

        # these fields will change
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()

        # get indices of the highway link fields
        dirs_pos = lf_dict["DIRECTIONS"]
        type1_pos = lf_dict["TYPE1"]
        type2_pos = lf_dict["TYPE2"]
        ampm1_pos = lf_dict["AMPM1"]
        ampm2_pos = lf_dict["AMPM2"]
        speed1_pos = lf_dict["POSTEDSPEED1"]
        speed2_pos = lf_dict["POSTEDSPEED2"]
        lanes1_pos = lf_dict["THRULANES1"]
        lanes2_pos = lf_dict["THRULANES2"]
        feet1_pos = lf_dict["THRULANEWIDTH1"]
        feet2_pos = lf_dict["THRULANEWIDTH2"]
        parklanes1_pos = lf_dict["PARKLANES1"]
        parklanes2_pos = lf_dict["PARKLANES2"]
        parkres1_pos = lf_dict["PARKRES1"]
        parkres2_pos = lf_dict["PARKRES2"]
        buslanes1_pos = lf_dict["BUSLANES1"]
        buslanes2_pos = lf_dict["BUSLANES2"]
        sigic_pos = lf_dict["SIGIC"]
        cltl_pos = lf_dict["CLTL"]
        rrgradex_pos = lf_dict["RRGRADECROSS"]
        toll_pos = lf_dict["TOLLDOLLARS"]
        modes_pos = lf_dict["MODES"]
        vclearance_pos = lf_dict["VCLEARANCE"]
        nbaselink_pos = lf_dict["NEW_BASELINK"]
        proj_pos = lf_dict["PROJECT"]
        desc_pos = lf_dict["DESCRIPTION"]

        # get indices of the project coding fields
        action_pos = cf_dict["ACTION_CODE"]
        ndirs_pos = cf_dict["NEW_DIRECTIONS"]
        ntype1_pos = cf_dict["NEW_TYPE1"]
        ntype2_pos = cf_dict["NEW_TYPE2"]
        nampm1_pos = cf_dict["NEW_AMPM1"]
        nampm2_pos = cf_dict["NEW_AMPM2"]
        nspeed1_pos = cf_dict["NEW_POSTEDSPEED1"]
        nspeed2_pos = cf_dict["NEW_POSTEDSPEED2"]
        nlanes1_pos = cf_dict["NEW_THRULANES1"]
        nlanes2_pos = cf_dict["NEW_THRULANES2"]
        nfeet1_pos = cf_dict["NEW_THRULANEWIDTH1"]
        nfeet2_pos = cf_dict["NEW_THRULANEWIDTH2"]
        aparklanes1_pos = cf_dict["ADD_PARKLANES1"]
        aparklanes2_pos = cf_dict["ADD_PARKLANES2"]
        cparkres1_pos = cf_dict["CHANGE_PARKRES1"]
        cparkres2_pos = cf_dict["CHANGE_PARKRES2"]
        abuslanes1_pos = cf_dict["ADD_BUSLANES1"]
        abuslanes2_pos = cf_dict["ADD_BUSLANES2"]
        asigic_pos = cf_dict["ADD_SIGIC"]
        acltl_pos = cf_dict["ADD_CLTL"]
        arrgradex_pos = cf_dict["ADD_RRGRADECROSS"]
        ntoll_pos = cf_dict["NEW_TOLLDOLLARS"]
        nmodes_pos = cf_dict["NEW_MODES"]
        nvclearance_pos = cf_dict["NEW_VCLEARANCE"]
        notes_pos = cf_dict["PROCESS_NOTES"]

        if len(action_1_dict) > 0:

            for action in action_1_dict:

                project = action[0]
                abb = action[1]

                edits = action_1_dict[action]
                ndirs = edits["NEW_DIRECTIONS"]
                ntype1 = edits["NEW_TYPE1"]
                ntype2 = edits["NEW_TYPE2"]
                nampm1 = edits["NEW_AMPM1"]
                nampm2 = edits["NEW_AMPM2"]
                nspeed1 = edits["NEW_POSTEDSPEED1"]
                nspeed2 = edits["NEW_POSTEDSPEED2"]
                nlanes1 = edits["NEW_THRULANES1"]
                nlanes2 = edits["NEW_THRULANES2"]
                nfeet1 = edits["NEW_THRULANEWIDTH1"]
                nfeet2 = edits["NEW_THRULANEWIDTH2"]
                aparklanes1 = edits["ADD_PARKLANES1"]
                aparklanes2 = edits["ADD_PARKLANES2"]
                cparkres1 = edits["CHANGE_PARKRES1"]
                cparkres2 = edits["CHANGE_PARKRES2"]
                abuslanes1 = edits["ADD_BUSLANES1"]
                abuslanes2 = edits["ADD_BUSLANES2"]
                asigic = edits["ADD_SIGIC"]
                acltl = edits["ADD_CLTL"]
                arrgradex = edits["ADD_RRGRADECROSS"]
                ntoll = edits["NEW_TOLLDOLLARS"]
                nmodes = edits["NEW_MODES"]
                nvclearance = edits["NEW_VCLEARANCE"]

                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[dirs_pos] = ndirs if ndirs != "0" else row[dirs_pos]
                        row[type1_pos] = ntype1 if ntype1 != "0" else row[type1_pos]
                        row[type2_pos] = ntype2 if ntype2 != "0" else row[type2_pos]
                        row[ampm1_pos] = nampm1 if nampm1 != "0" else row[ampm1_pos]
                        row[ampm2_pos] = nampm2 if nampm2 != "0" else row[ampm2_pos]
                        row[speed1_pos] = nspeed1 if nspeed1 != 0 else row[speed1_pos]
                        row[speed2_pos] = nspeed2 if nspeed2 != 0 else row[speed2_pos]
                        row[lanes1_pos] = nlanes1 if nlanes1 != 0 else row[lanes1_pos]
                        row[lanes2_pos] = nlanes2 if nlanes2 != 0 else row[lanes2_pos]
                        row[feet1_pos] = nfeet1 if nfeet1 != 0 else row[feet1_pos]
                        row[feet2_pos] = nfeet2 if nfeet2 != 0 else row[feet2_pos]
                        row[parklanes1_pos] = max((row[parklanes1_pos] + aparklanes1), 0)
                        row[parklanes2_pos] = max((row[parklanes2_pos] + aparklanes2), 0)
                        row[parkres1_pos] = cparkres1 if cparkres1 != "0" else row[parkres1_pos]
                        row[parkres2_pos] = cparkres2 if cparkres2 != "0" else row[parkres2_pos]
                        row[buslanes1_pos] = min(max((row[buslanes1_pos] + abuslanes1), 0), 1)
                        row[buslanes2_pos] = min(max((row[buslanes2_pos] + abuslanes2), 0), 1)
                        row[sigic_pos] = min(max((row[sigic_pos] + asigic), 0), 1)
                        row[cltl_pos] = min(max((row[cltl_pos] + acltl), 0), 1)
                        row[rrgradex_pos] = min(max((row[rrgradex_pos] + arrgradex), 0), 1)

                        if ntoll != "0":
                            row[toll_pos] = ntoll 
                        if ntoll == "-1":
                            row[toll_pos] = "0"

                        row[modes_pos] = nmodes if nmodes != "0" else row[modes_pos]

                        if nvclearance != 0:
                            row[vclearance_pos] = nvclearance
                        if nvclearance == -1:
                            row[vclearance_pos] = 0

                        row[proj_pos] = project
                        row[desc_pos] = f"Modified in {current_year}"

                        # if directions = 1 or 2
                        # empty all "2" fields
                        if row[dirs_pos] in ["1", "2"]:
                            row[type2_pos] = "0"
                            row[ampm2_pos] = "0"
                            row[speed2_pos] = 0
                            row[lanes2_pos] = 0
                            row[feet2_pos] = 0
                            row[parklanes2_pos] = 0
                            row[buslanes2_pos] = 0

                        # if directions = 1 remove parkres2
                        if row[dirs_pos] == "1":
                            row[parkres2_pos] = "-"

                        ucursor.updateRow(row)

                where_clause = "USE = 1"
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '1'"

                # if modified and modified again
                with arcpy.da.UpdateCursor(coding_table, coding_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[ndirs_pos] = "0" if row[ndirs_pos] == ndirs else row[ndirs_pos]
                        row[ntype1_pos] = "0" if row[ntype1_pos] == ntype1 else row[ntype1_pos]
                        row[ntype2_pos] = "0" if row[ntype2_pos] == ntype1 else row[ntype2_pos]
                        row[nampm1_pos] = "0" if row[nampm1_pos] == nampm1 else row[nampm1_pos]
                        row[nampm2_pos] = "0" if row[nampm2_pos] == nampm2 else row[nampm2_pos]
                        row[nspeed1_pos] = 0 if row[nspeed1_pos] == nspeed1 else row[nspeed1_pos]
                        row[nspeed2_pos] = 0 if row[nspeed2_pos] == nspeed2 else row[nspeed2_pos]
                        row[nlanes1_pos] = 0 if row[nlanes1_pos] == nlanes1 else row[nlanes1_pos]
                        row[nlanes2_pos] = 0 if row[nlanes2_pos] == nlanes2 else row[nlanes2_pos]
                        row[nfeet1_pos] = 0 if row[nfeet1_pos] == nfeet1 else row[nfeet1_pos]
                        row[nfeet2_pos] = 0 if row[nfeet2_pos] == nfeet1 else row[nfeet2_pos]
                        row[aparklanes1_pos] = row[aparklanes1_pos] - aparklanes1 if row[aparklanes1_pos] != 0 else 0
                        row[aparklanes2_pos] = row[aparklanes2_pos] - aparklanes1 if row[aparklanes2_pos] != 0 else 0
                        row[cparkres1_pos] = "0" if row[cparkres1_pos] == cparkres1 else row[cparkres1_pos]
                        row[cparkres2_pos] = "0" if row[cparkres2_pos] == cparkres2 else row[cparkres2_pos]
                        row[abuslanes1_pos] = 0 if row[abuslanes1_pos] == abuslanes1 else row[abuslanes1_pos]
                        row[abuslanes2_pos] = 0 if row[abuslanes2_pos] == abuslanes1 else row[abuslanes2_pos]
                        row[asigic_pos] = 0 if row[asigic_pos] == asigic else row[asigic_pos]
                        row[acltl_pos] = 0 if row[acltl_pos] == acltl else row[acltl_pos]
                        row[arrgradex_pos] = 0 if row[arrgradex_pos] == arrgradex else row[arrgradex_pos]
                        row[ntoll_pos] = "0" if row[ntoll_pos] == ntoll else row[ntoll_pos]
                        row[nmodes_pos] = "0" if row[nmodes_pos] == nmodes else row[nmodes_pos]
                        row[nvclearance_pos] = 0 if row[nvclearance_pos] == nvclearance else row[nvclearance_pos]

                        row[notes_pos] = f"Modified in {current_year}"
                        ucursor.updateRow(row)

                # if modified and deleted- no impact
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '3'"

                spec_fields = ["PROCESS_NOTES"]
                with arcpy.da.UpdateCursor(coding_table, spec_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[0] = f"Modified in {current_year}"
                        ucursor.updateRow(row)

        if len(action_3_dict) > 0:
            
            for action in action_3_dict:

                project = action[0]
                abb = action[1]

                # set to skeleton link
                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[nbaselink_pos] = "0"
                        row[proj_pos] = project
                        row[desc_pos] = f"Deleted in {current_year}"
                        ucursor.updateRow(row)

                # if a link is deleted, it cannot be modified or deleted again
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE in ('1', '3')"

                spec_fields = ["USE", "PROCESS_NOTES"]

                with arcpy.da.UpdateCursor(coding_table, spec_fields, where_clause) as ucursor:
                    for row in ucursor:
                        row[0] = 0 
                        row[1] = f"Deleted in {current_year}"

                        ucursor.updateRow(row)

        if len(action_4_dict) > 0:

            for action in action_4_dict:

                project = action[0]
                abb = action[1]

                edits = action_4_dict[action]
                ndirs = edits["NEW_DIRECTIONS"]
                ntype1 = edits["NEW_TYPE1"]
                ntype2 = edits["NEW_TYPE2"]
                nampm1 = edits["NEW_AMPM1"]
                nampm2 = edits["NEW_AMPM2"]
                nspeed1 = edits["NEW_POSTEDSPEED1"]
                nspeed2 = edits["NEW_POSTEDSPEED2"]
                nlanes1 = edits["NEW_THRULANES1"]
                nlanes2 = edits["NEW_THRULANES2"]
                nfeet1 = edits["NEW_THRULANEWIDTH1"]
                nfeet2 = edits["NEW_THRULANEWIDTH2"]
                aparklanes1 = edits["ADD_PARKLANES1"]
                aparklanes2 = edits["ADD_PARKLANES2"]
                cparkres1 = edits["CHANGE_PARKRES1"]
                cparkres2 = edits["CHANGE_PARKRES2"]
                abuslanes1 = edits["ADD_BUSLANES1"]
                abuslanes2 = edits["ADD_BUSLANES2"]
                asigic = edits["ADD_SIGIC"]
                acltl = edits["ADD_CLTL"]
                arrgradex = edits["ADD_RRGRADECROSS"]
                ntoll = edits["NEW_TOLLDOLLARS"]
                nmodes = edits["NEW_MODES"]
                nvclearance = edits["NEW_VCLEARANCE"]

                where_clause = f"ABB = '{abb}'"
                with arcpy.da.UpdateCursor(hwylink_fc, link_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[dirs_pos] = ndirs
                        row[type1_pos] = ntype1
                        row[type2_pos] = ntype2
                        row[ampm1_pos] = nampm1
                        row[ampm2_pos] = nampm2
                        row[speed1_pos] = nspeed1
                        row[speed2_pos] = nspeed2
                        row[lanes1_pos] = nlanes1
                        row[lanes2_pos] = nlanes2
                        row[feet1_pos] = nfeet1
                        row[feet2_pos] = nfeet2
                        row[parklanes1_pos] = max((row[parklanes1_pos] + aparklanes1), 0)
                        row[parklanes2_pos] = max((row[parklanes2_pos] + aparklanes2), 0)
                        row[parkres1_pos] = cparkres1 if cparkres1 != "0" else row[parkres1_pos]
                        row[parkres2_pos] = cparkres2 if cparkres2 != "0" else row[parkres2_pos]
                        row[buslanes1_pos] = min(max((row[buslanes1_pos] + abuslanes1), 0), 1)
                        row[buslanes2_pos] = min(max((row[buslanes2_pos] + abuslanes2), 0), 1)
                        row[sigic_pos] = min(max((row[sigic_pos] + asigic), 0), 1)
                        row[cltl_pos] = min(max((row[cltl_pos] + acltl), 0), 1)
                        row[rrgradex_pos] = min(max((row[rrgradex_pos] + arrgradex), 0), 1)
                        row[toll_pos] = ntoll if ntoll != "-1" else "0"
                        row[modes_pos] = nmodes
                        row[vclearance_pos] = nvclearance if nvclearance != -1 else 0

                        row[nbaselink_pos] = "1"
                        row[proj_pos] = project
                        row[desc_pos] = f"Added in {current_year}"

                        ucursor.updateRow(row)

                # if added can't be added again 
                where_clause = "USE = 1 "
                where_clause += f"AND ABB = '{abb}' AND COMPLETION_YEAR > {current_year} "
                where_clause += "AND ACTION_CODE = '4'"
                with arcpy.da.UpdateCursor(coding_table, coding_fields, where_clause) as ucursor:
                    for row in ucursor:

                        row[action_pos] = "1" # becomes modify

                        row[ndirs_pos] = "0" if row[ndirs_pos] == ndirs else row[ndirs_pos]
                        row[ntype1_pos] = "0" if row[ntype1_pos] == ntype1 else row[ntype1_pos]
                        row[ntype2_pos] = "0" if row[ntype2_pos] == ntype1 else row[ntype2_pos]
                        row[nampm1_pos] = "0" if row[nampm1_pos] == nampm1 else row[nampm1_pos]
                        row[nampm2_pos] = "0" if row[nampm2_pos] == nampm2 else row[nampm2_pos]
                        row[nspeed1_pos] = 0 if row[nspeed1_pos] == nspeed1 else row[nspeed1_pos]
                        row[nspeed2_pos] = 0 if row[nspeed2_pos] == nspeed2 else row[nspeed2_pos]
                        row[nlanes1_pos] = 0 if row[nlanes1_pos] == nlanes1 else row[nlanes1_pos]
                        row[nlanes2_pos] = 0 if row[nlanes2_pos] == nlanes2 else row[nlanes2_pos]
                        row[nfeet1_pos] = 0 if row[nfeet1_pos] == nfeet1 else row[nfeet1_pos]
                        row[nfeet2_pos] = 0 if row[nfeet2_pos] == nfeet1 else row[nfeet2_pos]
                        row[aparklanes1_pos] = row[aparklanes1_pos] - aparklanes1
                        row[aparklanes2_pos] = row[aparklanes2_pos] - aparklanes2
                        row[cparkres1_pos] = "0" if row[cparkres1_pos] == cparkres1 else row[cparkres1_pos]
                        row[cparkres2_pos] = "0" if row[cparkres2_pos] == cparkres2 else row[cparkres2_pos]
                        row[abuslanes1_pos] = 0 if row[abuslanes1_pos] == abuslanes1 else row[abuslanes1_pos]
                        row[abuslanes2_pos] = 0 if row[abuslanes2_pos] == abuslanes1 else row[abuslanes2_pos]
                        row[asigic_pos] = 0 if row[asigic_pos] == asigic else row[asigic_pos]
                        row[acltl_pos] = 0 if row[acltl_pos] == acltl else row[acltl_pos]
                        row[arrgradex_pos] = 0 if row[arrgradex_pos] == arrgradex else row[arrgradex_pos]
                        row[ntoll_pos] = "0" if row[ntoll_pos] == ntoll else row[ntoll_pos]
                        row[nmodes_pos] = "0" if row[nmodes_pos] == nmodes else row[nmodes_pos]
                        row[nvclearance_pos] = 0 if row[nvclearance_pos] == nvclearance else row[nvclearance_pos]

                        row[notes_pos] = f"Added in {current_year}"
                        ucursor.updateRow(row)

        # set use on all projects completed this year to 0 
        where_clause = f"COMPLETION_YEAR = {current_year}"
        with arcpy.da.UpdateCursor(coding_table, ["USE", "PROCESS_NOTES"], where_clause) as ucursor:
            for row in ucursor:
                row[0] = 0
                row[1] = f"Completed in {current_year}"

                ucursor.updateRow(row)

        self.base_year = current_year

    # helper method to add relationship classes
    def add_rcs(self):

        arcpy.env.workspace = self.current_gdb

        # add rel_hwyproj_to_coding
        arcpy.management.CreateRelationshipClass(
            "hwyproj", "hwyproj_coding", "rel_hwyproj_to_coding",
            "COMPOSITE", "hwyproj_coding", "hwyproj", "FORWARD", "ONE_TO_MANY", 
            "NONE", "TIPID", "TIPID")
        
        # add rel_arcs_to_hwyproj_coding
        arcpy.management.CreateRelationshipClass(
            "hwynet_arc", "hwyproj_coding", "rel_arcs_to_hwyproj_coding",
            "SIMPLE", "hwyproj_coding", "hwynet_arc", "NONE", "ONE_TO_MANY", 
            "NONE", "ABB", "ABB")
        
        # add rel_bus_x_to_itin
        xes = ["base", "current", "future"]

        for x in xes:
            arcpy.management.CreateRelationshipClass(
                f"bus_{x}", f"bus_{x}_itin", f"rel_bus_{x}_to_itin",
                "COMPOSITE", f"bus_{x}_itin", f"bus_{x}", "FORWARD", "ONE_TO_MANY", 
                "NONE", "TRANSIT_LINE", "TRANSIT_LINE")
            
        # add rel_arcs_to_bus_x_itin
        for x in xes:
            arcpy.management.CreateRelationshipClass(
                "hwynet_arc", f"bus_{x}_itin", f"rel_arcs_to_bus_{x}_itin",
                "SIMPLE", f"bus_{x}_itin", "hwynet_arc", "NONE", "ONE_TO_MANY",
                "NONE", "ABB", "ABB")
            
        # add rel_nodes_to_parknride
        arcpy.management.CreateRelationshipClass(
            "hwynet_node", "parknride", "rel_nodes_to_parknride",
            "SIMPLE", "parknride", "hwynet_node", "NONE", "ONE_TO_MANY",
            "NONE", "NODE", "NODE")