## FN.py

## Based on work by kcazzato
## Edited by ccai (2025)

import os
import shutil
import sys
import arcpy
import math
import pandas as pd
import networkx as nx

class FreightNetwork:

    # constructor
    def __init__(self):

        # get paths
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        in_folder = os.path.join(mfhrn_path, "input")
        self.mfn_in_folder = os.path.join(in_folder, "2_freight")
        self.mfn_in_gdb = os.path.join(self.mfn_in_folder, "MFN.gdb")

        # already exists
        out_folder = os.path.join(mfhrn_path, "output")
        mhn_out_folder = os.path.join(out_folder, "1_travel")
        self.mhn_all_gdb = os.path.join(mhn_out_folder, "MHN_all.gdb")

        self.mfn_out_folder = os.path.join(out_folder, "2_freight")
        self.mfhn_all_gdb = os.path.join(self.mfn_out_folder, "MFHN_all.gdb")

        # special nodes
        self.node_dict = {}
        self.node_dict["CMAP_centroid"] = [i for i in range(1, 133)]
        self.node_dict["CMAP_logistic"] = [i for i in range(133, 151)]
        self.node_dict["national_centroid"] = [i for i in range(151, 274)] + [310, 399]

        for num in [179, 180, 182]:
            self.node_dict["national_centroid"].remove(num)

        self.node_dict["poe"] = [3634, 3636, 3639, 3640, 3641, 3642, 3643, 3644, 3647, 3648] 

        years_csv_path = os.path.join(in_folder, "input_years.csv")
        self.years_list = pd.read_csv(years_csv_path)["year"].to_list()

    # MAIN METHODS --------------------------------------------------------------------------------
    
    # method that generates output MFHN
    def generate_mfhn(self):

        print("Creating freight output folder...")

        # search for mhn_all.gdb
        mhn_all_gdb = self.mhn_all_gdb

        if os.path.isdir(mhn_all_gdb) == False:
            sys.exit("You need to get MHN_all.gdb first! Crashing program.")

        # delete freight output folder + recreate it 
        mfn_out_folder = self.mfn_out_folder
        mfhn_all_gdb = self.mfhn_all_gdb

        if os.path.isdir(mfn_out_folder) == True:
            shutil.rmtree(mfn_out_folder)

        os.mkdir(mfn_out_folder)

        # copy GDB
        self.copy_gdb_safe(mhn_all_gdb, mfhn_all_gdb)

        print("Freight output folder created.\n")

    # method that checks feature classes in the MFN 
    def check_mfn_fcs(self): 

        print("Checking feature classes for errors...")

        mfn_in_gdb = self.mfn_in_gdb
        mfn_out_folder = self.mfn_out_folder

        errors = 0

        base_feature_class_errors = os.path.join(
            mfn_out_folder, 
            "base_feature_class_errors.txt")
        error_file= open(base_feature_class_errors, "a")

        centroid_fc = os.path.join(mfn_in_gdb, "Meso_Ext_Int_Centroids")
        logistic_fc = os.path.join(mfn_in_gdb, "Meso_Logistic_Nodes")
        mesozones_fc = os.path.join(mfn_in_gdb, "Meso_External_CMAP_merge")

        # CHECK CENTROID NODE FC
        centroid_node_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(centroid_fc, ["NODE_ID"])],
            columns = ["NODE_ID"]
        )

        centroid_node_counts = centroid_node_df.NODE_ID.value_counts()
        centroid_node_set = set(centroid_node_df.NODE_ID.to_list())
        valid_centroid = self.node_dict["CMAP_centroid"] + self.node_dict["national_centroid"]
        valid_centroid_set = set(valid_centroid)

        if (centroid_node_counts.max() > 1):
            bad_node_df = centroid_node_counts[centroid_node_counts > 1]
            error_file.write("These centroid nodes violate unique node ID constraint.\n")
            error_file.write(bad_node_df.to_string() + "\n\n")
            errors += 1 

        extra_centroids = centroid_node_set - valid_centroid_set
        missing_centroids = valid_centroid_set - centroid_node_set

        if (len(extra_centroids) > 0):
            error_file.write("These centroid nodes do not have valid node IDs: " + str(extra_centroids))
            error_file.write("\n\n")
            errors += 1

        if (len(missing_centroids) > 0):
            error_file.write("These centroid nodes are not in the feature class: " + str(missing_centroids))
            error_file.write("\n\n")
            errors += 1

        # CHECK LOGISTIC NODE FC
        logistic_node_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(logistic_fc, ["NODE_ID"])],
            columns = ["NODE_ID"]
        )

        logistic_node_counts = logistic_node_df.NODE_ID.value_counts()
        logistic_node_set = set(logistic_node_df.NODE_ID.to_list())
        valid_logistic = self.node_dict["CMAP_logistic"] 
        valid_logistic_set = set(valid_logistic)

        if (logistic_node_counts.max() > 1):
            bad_node_df = logistic_node_counts[logistic_node_counts > 1]
            error_file.write("These logistic nodes violate unique node ID constraint.\n")
            error_file.write(bad_node_df.to_string() + "\n\n")
            errors += 1 

        extra_logistic = logistic_node_set - valid_logistic_set
        missing_logistic = valid_logistic_set - logistic_node_set

        if (len(extra_logistic) > 0):
            error_file.write("These logistic nodes do not have valid node IDs: " + str(extra_logistic))
            error_file.write("\n\n")
            errors += 1

        if (len(missing_logistic) > 0):
            error_file.write("These logistic nodes are not in the feature class: " + str(missing_logistic))
            error_file.write("\n\n")
            errors += 1

        # CHECK MESOZONE FC
        mesozones_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(mesozones_fc, ["MESOZONE"])],
            columns = ["MESOZONE"]
        )

        mesozone_counts = mesozones_df.MESOZONE.value_counts()
        mesozone_set = set(mesozones_df.MESOZONE.to_list())
        valid_mesozone = self.node_dict["CMAP_centroid"] + self.node_dict["national_centroid"]
        valid_mesozone_set = set(valid_mesozone)

        if (mesozone_counts.max() > 1):
            bad_zone_df = mesozone_counts[mesozone_counts > 1]
            error_file.write("These mesozones violate unique zone number constraint.\n")
            error_file.write(bad_zone_df.to_string() + "\n\n")
            errors += 1 

        extra_mesozones = mesozone_set - valid_mesozone_set
        missing_mesozones = valid_mesozone_set - mesozone_set

        if (len(extra_mesozones) > 0):
            error_file.write("These mesozones do not have a valid zone number: " + str(extra_mesozones))
            error_file.write("\n\n")
            errors += 1

        if (len(missing_mesozones) > 0):
            error_file.write("These mesozones are not in the feature class: " + str(missing_mesozones))
            error_file.write("\n\n")
            errors += 1

        error_file.close()

        if errors > 0:
            sys.exit("Error(s) were detected in the feature class(es). Crashing program.")
        else:
            os.remove(base_feature_class_errors)
        
        print("Base feature classes checked for errors.\n")

    # method that creates override file
    def create_override_meso(self):

        print("Creating meso override file...")

        arcpy.env.workspace = self.mfhn_all_gdb

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_all")
        base_hwylink = hwylink_list[0]

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
        
        poe = self.node_dict["poe"]
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

        # find any issues with the override file
        override_file_creation_errors = os.path.join(
            self.mfn_out_folder, 
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

        override_meso_folder = os.path.join(self.mfn_out_folder, "override_meso")
        os.mkdir(override_meso_folder)

        override_meso_shp = os.path.join(override_meso_folder, "override_meso.shp")
        arcpy.management.CopyFeatures("meso_buffer", override_meso_shp)
        print("Meso override file created.\n")

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