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

    # method that creates base year gdb
    def create_base_year(self):

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
        out_gdb = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        self.copy_gdb_safe(mhn_in_gdb, out_gdb)
        self.current_gdb = out_gdb # !!! update the HN's current gdb

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
        arcpy.management.AddFields(hwylink_fc, [["NEW_BASELINK", "TEXT"], ["PROJECT", "TEXT"], ["DESCRIPTION", "TEXT"]])
        arcpy.management.CalculateField(
            in_table= hwylink_fc,
            field="NEW_BASELINK",
            expression="!BASELINK!",
            expression_type="PYTHON3",
            code_block="",
            field_type="TEXT",
            enforce_domains="NO_ENFORCE_DOMAINS")
        arcpy.management.AddField(hwyproj_fc, "DESCRIPTION", "TEXT")
        
        arcpy.management.AddFields(coding_table, [["USE", "SHORT"], ["PROCESS_NOTES", "TEXT"]])

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
        
        with arcpy.da.UpdateCursor(hwynode_fc, hwynode_fields) as ucursor:
            for row in ucursor:

                if row[0] not in link_node_set: # check that nodes are not disconnected
                    node_fail +=1
                    row[5] = "Error: node not connected to links"
                    ucursor.updateRow(row)
                    continue

        if node_fail > 0:
            error_file.write(f"{node_fail} nodes failed the individual row check. Check output gdb.\n")
        else:
            error_file.write("No nodes failed the individual row check.\n")
        
        # check links
        link_fail = 0

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        link_fields, lf_dict, coding_fields, cf_dict = self.get_hwy_fields()
        rev_lf_dict = {lf_dict[k]: k for k in lf_dict.keys()}

        link_domain_dict = self.get_field_domain_dict(hwylink_fc)

        desc_pos = lf_dict["DESCRIPTION"]

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
                park1 = row[lf_dict["PARKLANES1"]]
                park2 = row[lf_dict["PARKLANES2"]]
                parkres1 = row[lf_dict["PARKRES1"]]
                parkres2 = row[lf_dict["PARKRES2"]]
                bus1 = row[lf_dict["BUSLANES1"]]
                bus2 = row[lf_dict["BUSLANES2"]]
                sigic = row[lf_dict["SIGIC"]]
                cltl = row[lf_dict["CLTL"]]
                rr = row[lf_dict["RRGRADECROSS"]]
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
                               lanes1, lanes2, feet1, feet2, park1, park2, bus1, bus2, 
                               sigic, cltl, rr, toll, modes, vclearance]
                
                if baselink == "0":
                    if any(int(field) != 0 for field in zero_fields):
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
                    if any(int(field) == 0 for field in req_fields):
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
                all_fields2 = [type2, ampm2, speed2, lanes2, feet2, park2, bus2]

                if baselink == "1" and dirs in ["1", "2"]:
                    if any(int(field) != 0 for field in all_fields2):
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
                    if any(int(field) == 0 for field in req_fields2):
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
                # check vclearance != -1 
                # check toll is correct

        if link_fail > 0:
            error_file.write(f"{link_fail} links failed the individual row check. Check output gdb.\n")
        else:
            error_file.write("No links failed the individual row check.\n")
                    
        error_file.close()

    # method that cleans up the output gdbs 
    def finalize_hwy_data(self):

        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwyproj_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        arcpy.management.DeleteField(hwynode_fc, ["DESCRIPTION"])
        arcpy.management.DeleteField(hwylink_fc, ["NEW_BASELINK", "PROJECT", "DESCRIPTION"])
        arcpy.management.DeleteField(hwyproj_fc, ["DESCRIPTION"])
        arcpy.management.DeleteField(coding_table, ["COMPLETION_YEAR", "USE", "PROCESS_NOTES"])

        self.add_rcs()

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