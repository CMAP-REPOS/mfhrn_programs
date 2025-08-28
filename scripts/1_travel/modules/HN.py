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

        # PK check
        base_feature_class_errors = os.path.join(
            mhn_out_folder, 
            "base_feature_class_errors.txt")
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


        # row check


        error_file.close()

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

    # helper method that deletes extra highway fields
    def delete_extra_hwy_fields(self):

        hwynode_fc = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwyproj_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")
        coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        arcpy.management.DeleteField(hwynode_fc, ["DESCRIPTION"])
        arcpy.management.DeleteField(hwylink_fc, ["NEW_BASELINK", "PROJECT", "DESCRIPTION"])
        arcpy.management.DeleteField(hwyproj_fc, ["DESCRIPTION"])
        arcpy.management.DeleteField(coding_table, ["USE", "PROCESS_NOTES"])