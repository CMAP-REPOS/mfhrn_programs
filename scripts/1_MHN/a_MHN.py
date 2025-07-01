## a_MHN.py 
## a translation of MHN.py
## Author: npeterson
## Translation by by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

class MasterHighwayNetwork:

    # code paths
    base_path = os.path.abspath("..\..")
    in_folder = os.path.join(base_path, "input")
    in_GDB = os.path.join(in_folder, "MHN.gdb")
    years_csv_path = os.path.join(in_folder, "years.csv")

    out_folder = os.path.join(base_path, "output")
    mhn_out_folder = os.path.join(out_folder, "1_MHN") 
    years_list = pd.read_csv(years_csv_path)["years"].to_list()

    # relationship classes 
    rel_classes = ["rel_arcs_to_bus_base_itin",
                   "rel_arcs_to_bus_current_itin",
                   "rel_arcs_to_bus_future_itin",
                   "rel_arcs_to_hwyproj_coding",
                   "rel_bus_base_to_itin",
                   "rel_bus_current_to_itin",
                   "rel_bus_future_to_itin",
                   "rel_hwyproj_to_coding",
                   "rel_nodes_to_parknride"]

    # get data
    hwyproj_coding = os.path.join(in_GDB, "hwyproj_coding")
    hwyproj_coding_fields = [f.name for f in arcpy.ListFields(hwyproj_coding)]
    hwyproj_coding_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwyproj_coding, hwyproj_coding_fields)], 
                                     columns = hwyproj_coding_fields)
    
    hwyproj_years = os.path.join(in_GDB, "hwynet/hwyproj")
    hwyproj_years_fields = ["TIPID", "COMPLETION_YEAR"]
    hwyproj_years_df = pd.DataFrame(data = [row for row in arcpy.da.SearchCursor(hwyproj_years, hwyproj_years_fields)],
                                    columns = hwyproj_years_fields)
    
    base_year = min(hwyproj_years_df.COMPLETION_YEAR.to_list()) - 1
    hwyproj_df = pd.merge(hwyproj_coding_df, hwyproj_years_df, how = "left", on = "TIPID") ## projects with their years 

    def generate_base_year(self):

        # delete output folder + recreate it- maybe move to a different method? 
        out_folder = self.out_folder
        mhn_out_folder = self.mhn_out_folder
        in_GDB = self.in_GDB
        base_year = self.base_year
        rel_classes = self.rel_classes
        
        if os.path.isdir(out_folder) == True:
            shutil.rmtree(out_folder)
        
        os.mkdir(out_folder)
        os.mkdir(mhn_out_folder)
        
        # copy gdb
        out_GDB = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        arcpy.management.Copy(in_GDB, out_GDB)

        # delete relationship classes 
        for rc in rel_classes:
            rc_path = os.path.join(out_GDB, rc)
            arcpy.management.Delete(rc_path)

        print("Base year copied with relationship classes deleted.")

    def clean_base_project_table(self):

        mhn_out_folder = self.mhn_out_folder
        base_year = self.base_year

        hwyproj_df = self.hwyproj_df 
        hwyproj_df["REP_ABB"] = hwyproj_df['REP_ANODE'].astype("string") + "-" + hwyproj_df["REP_BNODE"].astype("string") + "-1"

        # projects with year 9999 are not well maintained 
        # only check integrity if <2050 

        hwyproj_2050_df = hwyproj_df[hwyproj_df.COMPLETION_YEAR <= 2050]

        # make sure that every arc which is getting replaced 
        # is also deleted (3)

        rep_abbs_df = hwyproj_2050_df[hwyproj_2050_df.REP_ABB != "0-0-1"][["TIPID", "REP_ABB", "COMPLETION_YEAR"]].drop_duplicates()
        action_3_df = hwyproj_2050_df[hwyproj_2050_df.ACTION_CODE == "3"][["TIPID", "ABB", "COMPLETION_YEAR", "ACTION_CODE"]]
        check_3_df = pd.merge(rep_abbs_df, action_3_df, how = "left", left_on= ["TIPID", "REP_ABB", "COMPLETION_YEAR"], right_on = ["TIPID", "ABB", "COMPLETION_YEAR"])
        check_3_df_okay = check_3_df[check_3_df.ACTION_CODE.notnull()] # has a corresponding delete
        check_3_df_null = check_3_df[check_3_df.ACTION_CODE.isnull()] # does not have a corresponding delete 
        
        print(f"{len(check_3_df_okay)} out of {len(check_3_df)} of the replaced links were also deleted in that same TIPID + year")
        print(f"{len(check_3_df_null)} were not. Writing to base_project_table_notes.txt")
        
        base_project_table_notes = os.path.join(mhn_out_folder, "base_project_table_notes.txt")

        with open(base_project_table_notes, "a") as f:
            f.write("These replaced links were not also deleted.\n")
            f.write(check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_string())
            f.write("\n\n")

        check_3_dict_null = check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_dict("records")

        # add them onto the project table 
        out_GDB = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        hwyproj_coding_table = os.path.join(out_GDB, "hwyproj_coding")
        hwyproj_year_fc = os.path.join(out_GDB, "hwynet/hwyproj")

        arcpy.management.JoinField(hwyproj_coding_table, "TIPID", hwyproj_year_fc, "TIPID")
        arcpy.management.AddField(hwyproj_coding_table, "NOTES", "TEXT")
        arcpy.management.DeleteField(hwyproj_coding_table, ["TIPID_1", "MCP_ID", "RSP_ID", "Shape_Length"])

        with arcpy.da.Editor(out_GDB):
            # every other field has a default of 0
            fields = ["TIPID", "ACTION_CODE", "ABB", "NEW_TOLLDOLLARS", "NEW_MODES", "COMPLETION_YEAR", "NOTES"]
            with arcpy.da.InsertCursor(hwyproj_coding_table, fields) as icursor:
                for row in check_3_dict_null:
                    icursor.insertRow((row["TIPID"], "3", row["REP_ABB"], 0, "0", row["COMPLETION_YEAR"], "added action code 3"))