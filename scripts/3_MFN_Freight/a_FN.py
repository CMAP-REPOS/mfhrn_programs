## a_FN.py

## Based on work by kcazzato
## Edited by ccai (2025)

import math
import os
import shutil
import sys
import arcpy
import pandas as pd
import time

class FreightNetwork:

    # constructor
    def __init__(self):

        # get paths
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
        
        # already exists
        out_folder = os.path.join(mfhrn_path, "output")
        mhn_out_folder = os.path.join(out_folder, "1_MHN")
        self.mfn_out_folder = os.path.join(out_folder, "3_MFN_Freight")

        self.mhn_gdb = os.path.join(mhn_out_folder, "MHN_all.gdb")
        self.mfhn_gdb = os.path.join(self.mfn_out_folder, "MFHN_all.gdb")

    # MAIN METHODS --------------------------------------------------------------------------------
    
    def generate_mfhn(self):

        print("Creating freight output folder...")

        # delete freight output folder + recreate it 
        mfn_out_folder = self.mfn_out_folder

        mhn_gdb = self.mhn_gdb
        mfhn_gdb = self.mfhn_gdb

        if os.path.isdir(mfn_out_folder) == True:
            shutil.rmtree(mfn_out_folder)

        os.mkdir(mfn_out_folder)

        # copy GDB
        self.copy_gdb_safe(mhn_gdb, mfhn_gdb)
        self.current_gdb = mfhn_gdb

        print("Freight output folder created.\n")

    def make_meso_override(self):

        print("Making meso override file...")

        arcpy.env.workspace = self.mfhn_gdb

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_all")
        base_hwylink = hwylink_list[0]

        arcpy.management.AddField(base_hwylink, "MESO_flag", "TEXT") # to make my life easier 

        # all links where MESO = 1
        where_clause = "MESO = 1"
        base_meso = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(base_hwylink, ["ABB"], where_clause)], 
            columns = ["ABB"]).ABB.to_list()

        # all links where replacing a link with MESO = 1
        where_clause = "ACTION_CODE = '2'"
        project_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor("hwyproj_applied", ["ABB", "REP_ABB"], where_clause)], 
            columns = ["ABB", "REP_ABB"])
        
        replace_meso = project_df[project_df.REP_ABB.isin(base_meso)].ABB.to_list()

        all_meso = set(base_meso) | set(replace_meso)

        fields = ["ABB", "MESO_flag"]
        with arcpy.da.UpdateCursor(base_hwylink, fields) as ucursor:
            for row in ucursor:

                abb = row[0]
                if abb in base_meso:
                    row[1] = "base_meso"
                if abb in replace_meso:
                    row[1] = "replace_meso"

                ucursor.updateRow(row)

        arcpy.management.MakeFeatureLayer(base_hwylink, "meso_links", "MESO_flag IS NOT NULL")
        arcpy.analysis.PairwiseBuffer("meso_links", "meso_buffer_draft", "10 Feet")
        arcpy.management.DeleteField("meso_buffer_draft", ["ABB", "MESO_flag"], method = "KEEP_FIELDS")

        arcpy.management.MakeFeatureLayer(base_hwylink, "non_meso_links", "MESO_flag IS NULL")
        arcpy.analysis.PairwiseBuffer("non_meso_links", "non_meso_buffer", "5 Feet")
        arcpy.analysis.PairwiseErase("meso_buffer_draft", "non_meso_buffer", "meso_buffer")

        arcpy.management.AddFields("meso_buffer", [["USE", "SHORT"], ["YEAR", "SHORT"]])
        arcpy.management.CalculateField("meso_buffer", "USE", "1")

        meso_override_folder = os.path.join(self.mfn_out_folder, "meso_override")
        os.mkdir(meso_override_folder)

        meso_override_shp = os.path.join(meso_override_folder, "meso_override.shp")
        arcpy.management.CopyFeatures("meso_buffer", meso_override_shp)
        print("Meso override file made.\n")

    # HELPER METHODS ------------------------------------------------------------------------------

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

# TESTING -----------------------------------------------------------------------------------------

# main function for testing 
if __name__ == "__main__":

    FN = FreightNetwork()
    FN.generate_mfhn()
    FN.make_meso_override()