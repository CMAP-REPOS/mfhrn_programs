## a_FN.py

## Based on work by kcazzato
## Edited by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd
import networkx as nx
import time

class FreightNetwork:

    # constructor
    def __init__(self):

        # get paths
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        in_folder = os.path.join(mfhrn_path, "input")
        self.mfn_in_folder = os.path.join(in_folder, "3_MFN_Freight")
        self.mfn_in_gdb = os.path.join(self.mfn_in_folder, "MFN.gdb")

        # already exists
        out_folder = os.path.join(mfhrn_path, "output")
        mhn_out_folder = os.path.join(out_folder, "1_MHN")
        self.mfn_out_folder = os.path.join(out_folder, "3_MFN_Freight")

        self.mhn_all_gdb = os.path.join(mhn_out_folder, "MHN_all.gdb")
        self.mfhn_all_gdb = os.path.join(self.mfn_out_folder, "MFHN_all.gdb")

        # special nodes
        self.node_dict = {}
        self.node_dict["CMAP_centroid"] = [i for i in range(1, 133)]
        self.node_dict["CMAP_logistic"] = [i for i in range(133, 151)]
        self.node_dict["national_centroid"] = [i for i in range(151, 274)] + [310, 399]
        self.node_dict["poe"] = [3634, 3636, 3639, 3640, 3641, 3642, 3643, 3644, 3647, 3648]

        years_csv_path = os.path.join(in_folder, "input_years.csv")
        self.years_list = pd.read_csv(years_csv_path)["years"].to_list()

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

        # all links where replacing a link with MESO = 1
        where_clause = "ACTION_CODE = '2'"
        project_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor("hwyproj_applied", ["ABB", "REP_ABB"], where_clause)], 
            columns = ["ABB", "REP_ABB"])
        
        replace_meso = project_df[project_df.REP_ABB.isin(base_meso)].ABB.to_list()

        # all links where POE 
        link_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(base_hwylink, ["ANODE", "BNODE", "ABB"])], 
            columns = ["ANODE", "BNODE", "ABB"])
        
        poe = self.node_dict["poe"]
        poe_meso = link_df[link_df.ANODE.isin(poe) | link_df.BNODE.isin(poe)].ABB.to_list()

        all_meso = set(base_meso) | set(replace_meso) | set(poe_meso)

        fields = ["ABB", "MESO_flag"]
        with arcpy.da.UpdateCursor(base_hwylink, fields) as ucursor:
            for row in ucursor:

                abb = row[0]
                if abb in base_meso:
                    row[1] = "base_meso"
                if abb in replace_meso:
                    row[1] = "replace_meso"
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

    # method that generates meso layers
    def generate_meso_layers(self):
        
        print("Generating meso layers...")

        self.copy_meso_info()
        self.create_special_node_fc()
        self.subset_to_meso()
        self.find_hanging_nodes()

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
            
    # helper method which copies meso information 
    def copy_meso_info(self):

        print("Copying meso information from MFN...")
        mfn_in_gdb = self.mfn_in_gdb
        mfhn_all_gdb = self.mfhn_all_gdb
        
        meso_fcs = ["Meso_Ext_Int_Centroids", 
                    "Meso_Logistic_Nodes", 
                    "Meso_External_CMAP_merge", 
                    "CMAP_Rail"]
        
        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "meso_info", spatial_reference = 26771)

        for fc in meso_fcs:

            input_fc = os.path.join(mfn_in_gdb, fc)
            output_fc = os.path.join(mfhn_all_gdb, "meso_info", fc)
            arcpy.management.CopyFeatures(input_fc, output_fc)

        print("Meso information copied.\n")
    
    # helper method which creates special node fc
    def create_special_node_fc(self):

        print("Creating special node feature class...")
        mfhn_all_gdb = self.mfhn_all_gdb

        centroids_fc = os.path.join(mfhn_all_gdb, "meso_info", "Meso_Ext_Int_Centroids")
        logistic_fc = os.path.join(mfhn_all_gdb, "meso_info", "Meso_Logistic_Nodes")
        special_fc = os.path.join(mfhn_all_gdb, "special_nodes")

        arcpy.management.DeleteField(centroids_fc, ["NODE_ID", "POINT_X", "POINT_Y", "MESOZONE"], "KEEP_FIELDS")
        arcpy.management.AddField(centroids_fc, "flag", "TEXT")

        arcpy.management.DeleteField(logistic_fc, ["NODE_ID", "POINT_X", "POINT_Y", "MESOZONE"], "KEEP_FIELDS")
        arcpy.management.AddField(logistic_fc, "flag", "TEXT")

        arcpy.management.Merge([centroids_fc, logistic_fc], special_fc) # combines centroids + logistic nodes

        with arcpy.da.UpdateCursor(special_fc, ["NODE_ID", "flag"]) as ucursor:
            for row in ucursor:

                node = row[0]

                centroid = self.node_dict["CMAP_centroid"]
                logistic = self.node_dict["CMAP_logistic"]

                if node in centroid:
                    row[1] = "CMAP_centroid"
                elif node in logistic:
                    row[1] = "CMAP_logistic"
                
                ucursor.updateRow(row)

        arcpy.management.MakeFeatureLayer(special_fc, "null_layer", "flag IS NULL")
        arcpy.management.DeleteRows("null_layer")
        print("Special node feature class created.\n")

    # helper method which subsets to meso 
    def subset_to_meso(self):

        print("Subsetting to meso links...")
        # what does it mean that you lose access?

        mfn_in_folder = self.mfn_in_folder
        mfhn_all_gdb = self.mfhn_all_gdb
        years_list = self.years_list

        override_shp = os.path.join(mfn_in_folder, "override_meso", "override_meso.shp")

        arcpy.env.workspace = self.mfhn_all_gdb
        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_all")

        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "hwylinks_meso", spatial_reference = 26771)

        for fc in hwylink_list:

            year = int(fc[-4:])
            if year in years_list:
                
                all_fc = os.path.join(mfhn_all_gdb, "hwylinks_all", fc)
                meso_fc = os.path.join(mfhn_all_gdb, "hwylinks_meso", fc + "_MESO")
                arcpy.management.CopyFeatures(all_fc, meso_fc)

                meso_layer = f"meso_layer_{year}"
                override_layer = f"override_layer_{year}"

                arcpy.management.MakeFeatureLayer(meso_fc, meso_layer)
                arcpy.management.MakeFeatureLayer(override_shp, override_layer, "USE = 1")
                arcpy.management.SelectLayerByLocation(meso_layer, "INTERSECT", override_layer, invert_spatial_relationship = "INVERT")
                arcpy.management.DeleteRows(meso_layer)

                # remove skeleton links

                skeleton_layer = f"skeleton_layer_{year}"
                arcpy.management.MakeFeatureLayer(meso_fc, skeleton_layer, "NEW_BASELINK = '0'")
                arcpy.management.DeleteRows(skeleton_layer)
        
        print("Meso links subsetted.\n")

    # helper method which creates graph from meso links
    def create_meso_graph(self, meso_links):
        
        G = nx.Graph()

        with arcpy.da.SearchCursor(meso_links, ["ANODE", "BNODE"]) as scursor:
            for row in scursor:

                G.add_edge(row[0], row[1])

        return G

    # helper method which finds hanging nodes
    def find_hanging_nodes(self):

        print("Finding hanging nodes...")

        mfhn_all_gdb = self.mfhn_all_gdb

        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "hanging_nodes", spatial_reference = 26771)
        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_meso")

        for fc in hwylink_list:

            year = fc[8:12]
            input_fc = "hwynode_all"
            output_fc = os.path.join(mfhn_all_gdb, "hanging_nodes", f"HANGING_{year}")

            arcpy.management.CopyFeatures(input_fc, output_fc)
            
            fc_path = os.path.join(mfhn_all_gdb, "hwylinks_meso", fc)
            G = self.create_meso_graph(fc_path)

            comps = [c for c in sorted(nx.connected_components(G), key=len, reverse=True)]

            hanging_nodes = []

            if len(comps) > 1:

                for i in range(1, len(comps)):
                    hanging_nodes.extend(comps[i])

            node_layer = f"node_layer_{year}"
            arcpy.management.MakeFeatureLayer(output_fc, node_layer)

            if len(hanging_nodes) > 0:
                hanging_nodes_string = ", ".join([str(node) for node in hanging_nodes])
                arcpy.management.SelectLayerByAttribute(node_layer, where_clause = f"NODE NOT IN ({hanging_nodes_string})")
                arcpy.management.DeleteRows(node_layer)
            else:
                arcpy.management.DeleteRows(node_layer)

            arcpy.management.AddField(fc_path, "HANGING", "TEXT")

            with arcpy.da.UpdateCursor(fc_path, ["ANODE", "BNODE", "HANGING"]) as ucursor:
                for row in ucursor:

                    anode = row[0]
                    bnode = row[1]

                    if anode in hanging_nodes or bnode in hanging_nodes:
                        row[2] = "Y"

                    ucursor.updateRow(row)

    # helper method which connects the special nodes 
    def connect_special_nodes(self):

        mfhn_all_gdb = self.mfhn_all_gdb
        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "conn_links", spatial_reference = 26771)

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_meso")

        for fc in hwylink_list:

            year = fc[8:12]
            input_fc = os.path.join(mfhn_all_gdb, "special_nodes")
            output_fc = os.path.join(mfhn_all_gdb, "hanging_nodes", f"HANGING_{year}")

# TESTING -----------------------------------------------------------------------------------------

# main function for testing 
if __name__ == "__main__":

    FN = FreightNetwork()
    FN.generate_mfhn()
    FN.generate_meso_layers()