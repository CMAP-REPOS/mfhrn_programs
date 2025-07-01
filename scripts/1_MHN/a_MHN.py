## MHN.py
## Author: npeterson
## updated by ccai (2025)

import os
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
    years_list = pd.read_csv(years_csv_path)["years"].to_list()

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

    def generate_base_year():
        print("let's have some soup")