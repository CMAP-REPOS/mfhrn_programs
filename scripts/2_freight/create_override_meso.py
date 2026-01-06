## u_create_override_meso.py
## Author: ccai (2025)

import os
import sys
import arcpy
import shutil
import pandas as pd

print("Creating meso override file...")

sys_path = sys.argv[0]
abs_path = os.path.abspath(sys_path)
mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

arcpy.env.workspace = "memory"

in_folder = os.path.join(mfhrn_path, "input")
mhn_in_folder = os.path.join(in_folder, "1_travel")
mhn_in_gdb = os.path.join(mhn_in_folder, "MHN.gdb")

hwylink_fc = os.path.join(mhn_in_gdb, "hwynet/hwynet_arc")
base_hwylink = "base_hwylink"

arcpy.management.CopyFeatures(hwylink_fc, base_hwylink)
arcpy.management.AddField(base_hwylink, "MESO_flag", "TEXT") # to make my life easier 

# all links where MESO = 1
# failsafe for centroid connectors
where_clause = "MESO = 1 AND TYPE1 <> '6'"
base_meso = pd.DataFrame(
    data = [row for row in arcpy.da.SearchCursor(base_hwylink, ["ABB"], where_clause)], 
    columns = ["ABB"]).ABB.to_list()

# all links where POE 
link_df = pd.DataFrame(
    data = [row for row in arcpy.da.SearchCursor(base_hwylink, ["ANODE", "BNODE", "ABB"])], 
    columns = ["ANODE", "BNODE", "ABB"])

poe = [3634, 3636, 3639, 3640, 3641, 3642, 3643, 3644, 3647, 3648] 
poe_meso = link_df[link_df.ANODE.isin(poe) | link_df.BNODE.isin(poe)].ABB.to_list()

all_meso = set(base_meso) | set(poe_meso)

fields = ["ABB", "MESO_flag"]
with arcpy.da.UpdateCursor(base_hwylink, fields) as ucursor:
    for row in ucursor:

        abb = row[0]
        if abb in base_meso:
            row[1] = "base_meso"
        if abb in poe_meso:
            row[1] = "poe_meso"

        ucursor.updateRow(row)

arcpy.management.MakeFeatureLayer(base_hwylink, "meso_links", "MESO_flag IS NOT NULL")
arcpy.analysis.PairwiseBuffer("meso_links", "meso_buffer_draft", "10 Feet")
arcpy.management.DeleteField("meso_buffer_draft", ["ABB", "MESO_flag"], method = "KEEP_FIELDS")

arcpy.management.MakeFeatureLayer(base_hwylink, "non_meso_links", "MESO_flag IS NULL")
arcpy.analysis.PairwiseBuffer("non_meso_links", "non_meso_buffer", "5 Feet")
arcpy.analysis.PairwiseErase("meso_buffer_draft", "non_meso_buffer", "meso_buffer")

arcpy.management.AddFields("meso_buffer", [["USE", "SHORT"]])
arcpy.management.CalculateField("meso_buffer", "USE", "1")

out_folder = os.path.join(mfhrn_path, "output")

if os.path.isdir(out_folder) != True:
    os.mkdir(out_folder)

mfn_out_folder = os.path.join(out_folder, "2_freight")

if os.path.isdir(mfn_out_folder) == True:
    shutil.rmtree(mfn_out_folder)

os.mkdir(mfn_out_folder)

# find any issues with the override file
override_file_creation_errors = os.path.join(
    mfn_out_folder, 
    "override_file_creation_errors.txt")

arcpy.management.MakeFeatureLayer(base_hwylink, "all_links")
arcpy.management.MakeFeatureLayer("meso_buffer", "meso_buffer_layer")
arcpy.management.SelectLayerByLocation("all_links", "INTERSECT", "meso_buffer_layer")
arcpy.management.CopyFeatures("all_links", "override_meso_links")

arcpy.management.Delete("meso_links")
arcpy.management.Delete("non_meso_links")
arcpy.management.Delete("all_links")
arcpy.management.Delete("meso_buffer_layer")

override_meso = pd.DataFrame(
    data = [row for row in arcpy.da.SearchCursor("override_meso_links", ["ABB"])], 
    columns = ["ABB"]).ABB.to_list()
override_meso = set(override_meso)

error_file= open(override_file_creation_errors, "a")

# get meso links which weren't selected by the override file 
missed_meso = all_meso - override_meso
error_file.write(f"{len(missed_meso)} links are MESO but are not selected by the override.\n")
error_file.write(str(missed_meso) + "\n")

# get links which are not meso but were selected by the override file
extra_meso = override_meso - all_meso
error_file.write(f"{len(extra_meso)} links are not MESO but are selected by the override.\n")
error_file.write(str(extra_meso) + "\n")

error_file.close()

override_meso_folder = os.path.join(mfn_out_folder, "override_meso")
os.mkdir(override_meso_folder)

override_meso_shp = os.path.join(override_meso_folder, "override_meso.shp")
arcpy.management.CopyFeatures("meso_buffer", override_meso_shp)
print("Meso override file created.")