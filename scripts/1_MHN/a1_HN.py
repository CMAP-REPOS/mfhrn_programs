## a1_HN.py 
## combination of:
## 1) MHN.py, 
## 2) process_highway_coding.sas,
## 3) coding_overlap.sas,
## 4) generate_highway_files_2.sas, and
## 5) import_highway_projects_2.sas

## Author: npeterson
## Translation by by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

class HighwayNetwork:

    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
        
        self.in_folder = os.path.join(mfhrn_path, "input")
        self.in_gdb = os.path.join(self.in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_MHN")
        
        years_csv_path = os.path.join(self.in_folder, "years.csv")
        self.years_list = pd.read_csv(years_csv_path)["years"].to_list()

        # highway files - names of feature classes + tables in MHN
        self.hwy_files = [
            "hwynet/hwynet_arc",
            "hwynet/hwynet_node",
            "hwynet/hwyproj",
            "hwyproj_coding"
            ]
        
        # bus files - names of feature classes + tables in MHN
        # TODO - THERE ARE JUNK FILES IN HERE!! 
        self.bus_files = [
            "hwynet/bus_base",
            "hwynet/bus_current",
            "hwynet/bus_future",
            "bus_base_itin",
            "bus_base_itin_old",
            "bus_base_itin_new",
            "bus_current_itin",
            "bus_current_itin_old",
            "bus_current_itin_new",
            "bus_future_itin",
            "bus_future_itin_old",
            "bus_future_itin_new",
            "parknride" # not totally sure what this is. 
        ]
        
        # relationship classes in MHN 
        self.rel_classes = [
            "rel_arcs_to_bus_base_itin",
            "rel_arcs_to_bus_current_itin",
            "rel_arcs_to_bus_future_itin",
            "rel_arcs_to_hwyproj_coding",
            "rel_bus_base_to_itin",
            "rel_bus_current_to_itin",
            "rel_bus_future_to_itin",
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
        self.hwylink_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwylink, hwylink_fields)], 
                                       columns = hwylink_fields)
        
        hwynode = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode) if f.type != "Geometry"]
        self.hwynode_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwynode, hwynode_fields)], 
                                       columns = hwynode_fields)

        hwyproj = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_fields = [f.name for f in arcpy.ListFields(hwyproj)]
        self.hwyproj_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwyproj, hwyproj_fields)], 
                                        columns = hwyproj_fields)
        
        hwyproj_years = os.path.join(self.current_gdb, "hwynet/hwyproj")
        hwyproj_years_fields = ["TIPID", "COMPLETION_YEAR"]
        self.hwyproj_years_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwyproj_years, hwyproj_years_fields)],
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
    
    def generate_base_year(self):

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
        arcpy.management.CalculateField(in_table= hwyproj_coding_table,
                                        field="REP_ABB", expression="rep_abb(!REP_ANODE!, !REP_BNODE!)",
                                        expression_type="PYTHON3",
                                        code_block="""def rep_abb(rep_anode, rep_bnode):
                                        return str(rep_anode) + "-" + str(rep_bnode)""",
                                        field_type="TEXT",
                                        enforce_domains="NO_ENFORCE_DOMAINS")
        
        arcpy.management.JoinField(hwyproj_coding_table, "TIPID", hwyproj_year_fc, "TIPID")
        arcpy.management.AddField(hwyproj_coding_table, "NOTES", "TEXT")
        arcpy.management.DeleteField(hwyproj_coding_table, ["TIPID_1", "MCP_ID", "RSP_ID", "Shape_Length"])

        # add fields to review updates (a la Volpe)
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        arcpy.management.AddFields(hwylink_fc, [["PROJECT", "TEXT"], ["DESCRIPTION", "TEXT"]])

        self.get_hwy_dfs() # update the HN's current dfs 

        print("Base year copied and prepared for modification.")

    # def clean_base_project_table(self):
        
    #     mhn_out_folder = self.mhn_out_folder
    #     base_year = self.base_year

        # projects with year 9999 are not well maintained 
        # only check integrity if <2050 

    #     hwyproj_2050_df = hwyproj_df[hwyproj_df.COMPLETION_YEAR <= 2050]

    #     # make sure that every arc which is getting replaced 
    #     # is also deleted (3)

    #     rep_abbs_df = hwyproj_2050_df[hwyproj_2050_df.REP_ABB != "0-0-1"][["TIPID", "REP_ABB", "COMPLETION_YEAR"]].drop_duplicates()
    #     action_3_df = hwyproj_2050_df[hwyproj_2050_df.ACTION_CODE == "3"][["TIPID", "ABB", "COMPLETION_YEAR", "ACTION_CODE"]]
    #     check_3_df = pd.merge(rep_abbs_df, action_3_df, how = "left", left_on= ["TIPID", "REP_ABB", "COMPLETION_YEAR"], right_on = ["TIPID", "ABB", "COMPLETION_YEAR"])
    #     check_3_df_okay = check_3_df[check_3_df.ACTION_CODE.notnull()] # has a corresponding delete
    #     check_3_df_null = check_3_df[check_3_df.ACTION_CODE.isnull()] # does not have a corresponding delete 
        
    #     print(f"{len(check_3_df_okay)} out of {len(check_3_df)} of the replaced links were also deleted in that same TIPID + year")
    #     print(f"{len(check_3_df_null)} were not. Writing to base_project_table_notes.txt")
        
    #     base_project_table_notes = os.path.join(mhn_out_folder, "base_project_table_notes.txt")

    #     with open(base_project_table_notes, "a") as f:
    #         f.write("These replaced links were not also deleted.\n")
    #         f.write(check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_string())
    #         f.write("\n\n")

    #     check_3_dict_null = check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_dict("records")

    #     with arcpy.da.Editor(out_GDB):
    #         # every other field has a default of 0
    #         fields = ["TIPID", "ACTION_CODE", "ABB", "NEW_TOLLDOLLARS", "NEW_MODES", "COMPLETION_YEAR", "NOTES"]
    #         with arcpy.da.InsertCursor(hwyproj_coding_table, fields) as icursor:
    #             for row in check_3_dict_null:
    #                 icursor.insertRow((row["TIPID"], "3", row["REP_ABB"], 0, "0", row["COMPLETION_YEAR"], "added action code 3"))

# main function for testing 
if __name__ == "__main__":

    HN = HighwayNetwork()
    HN.generate_base_year()

    print("Done")