## u_create_meso_layers.py
## a translation of process_futureLinks.R
## Author: kcazzato
## Translated + Updated by ccai (2025) 

import os
import shutil
import sys
import arcpy
import math
import pandas as pd
import networkx as nx
import time

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

    # method that creates meso layers
    def create_meso_layers(self):
        
        print("Creating meso layers...")

        arcpy.env.workspace = self.mfhn_all_gdb

        self.copy_meso_info()
        self.create_freight_node_fcs()
        self.subset_to_meso()
        self.find_hanging_nodes()
        self.connect_special_nodes()
        self.create_final_networks()

        print("Meso layers created.\n")

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

    # helper method which creates freight node fcs
    def create_freight_node_fcs(self):

        print("Creating freight node feature classes...")

        mfn_out_folder = self.mfn_out_folder
        mfhn_all_gdb = self.mfhn_all_gdb

        centroid_fc = os.path.join(mfhn_all_gdb, "meso_info", "Meso_Ext_Int_Centroids")
        logistic_fc = os.path.join(mfhn_all_gdb, "meso_info", "Meso_Logistic_Nodes")
        special_fc = os.path.join(mfhn_all_gdb, "special_nodes")

        arcpy.management.DeleteField(centroid_fc, ["NODE_ID", "POINT_X", "POINT_Y", "MESOZONE"], "KEEP_FIELDS")
        arcpy.management.AddField(centroid_fc, "flag", "TEXT")

        arcpy.management.DeleteField(logistic_fc, ["NODE_ID", "POINT_X", "POINT_Y", "MESOZONE"], "KEEP_FIELDS")
        arcpy.management.AddField(logistic_fc, "flag", "TEXT")

        arcpy.management.Merge([centroid_fc, logistic_fc], special_fc) # combines centroids + logistic nodes

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

        arcpy.management.Delete("null_layer")

        hwynode_all_fc = os.path.join(mfhn_all_gdb, "hwynode_all")
        mesozones_fc = os.path.join(mfhn_all_gdb, "meso_info", "Meso_External_CMAP_merge")

        hwynode_zone_fc = os.path.join(mfhn_all_gdb, "hwynode_zone")

        arcpy.analysis.SpatialJoin(hwynode_all_fc, mesozones_fc, hwynode_zone_fc)

        meso_join_errors = os.path.join(
            mfn_out_folder, 
            "meso_join_errors.txt")
        
        hwynode_zone_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode_zone_fc, ["Join_Count", "NODE"])], 
            columns = ["Join_Count", "NODE"])
        
        error_file= open(meso_join_errors, "a") # open error file, don't forget to close it!

        no_zone_list = hwynode_zone_df[hwynode_zone_df.Join_Count == 0].NODE.to_list()
        mult_zone_list = hwynode_zone_df[hwynode_zone_df.Join_Count > 1].NODE.to_list()

        error_file.write(f"{len(no_zone_list)} highway nodes do not have a corresponding mesozone.\n")
        error_file.write(str(no_zone_list) + "\n\n")
        error_file.write(f"{len(mult_zone_list)} highway nodes have multiple corresponding mesozones.\n")
        error_file.write(str(mult_zone_list) + "\n\n")

        error_file.close()

        print("Freight node feature classes created.\n")

    # helper method which subsets to meso 
    def subset_to_meso(self):

        print("Subsetting to meso links...")
        # what does it mean that you lose access?

        mfhn_all_gdb = self.mfhn_all_gdb
        years_list = self.years_list

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_all")

        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "hwylinks_meso", spatial_reference = 26771)

        for fc in hwylink_list:

            year = int(fc[-4:])
            if year in years_list:
                
                all_fc = os.path.join(mfhn_all_gdb, "hwylinks_all", fc)
                meso_fc = os.path.join(mfhn_all_gdb, "hwylinks_meso", fc + "_MESO")
                arcpy.management.CopyFeatures(all_fc, meso_fc)

                meso_layer = f"meso_layer_{year}"

                # only want meso = 1 and non-skeleton links
                arcpy.management.MakeFeatureLayer(meso_fc, meso_layer)
                where_clause = "MESO = 1 AND NEW_BASELINK = '1' AND TYPE1 <> '6'"
                arcpy.management.SelectLayerByAttribute(meso_layer, "NEW_SELECTION", where_clause, "INVERT")
                arcpy.management.DeleteRows(meso_layer)

                arcpy.management.Delete(meso_layer)

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

            arcpy.management.Delete(node_layer)

        print("Hanging nodes found.\n")

    # helper method which connects the special nodes 
    def connect_special_nodes(self):

        print("Connecting special nodes...")

        mfhn_all_gdb = self.mfhn_all_gdb
        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "conn_links", spatial_reference = 26771)

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_meso")

        for fc in hwylink_list:

            year = fc[8:12]
            special_fc = os.path.join(mfhn_all_gdb, "special_nodes")
            conn_fc = os.path.join(mfhn_all_gdb, "conn_links", f"conn_links_{year}")

            arcpy.management.CopyFeatures(special_fc, conn_fc)

            fc_path = os.path.join(mfhn_all_gdb, "hwylinks_meso", fc)

            conn_layer = f"conn_layer_{year}"
            arcpy.management.MakeFeatureLayer(conn_fc, conn_layer)

            link_layer = f"link_layer_{year}"
            where_clause = "HANGING IS NULL"
            arcpy.management.MakeFeatureLayer(fc_path, link_layer, where_clause)

            arcpy.analysis.Near(conn_layer, link_layer)
            arcpy.management.JoinField(conn_fc, "NEAR_FID", fc_path, "OBJECTID", ["ABB", "ANODE"])

            arcpy.management.Delete(conn_layer)
            arcpy.management.Delete(link_layer)

        print("Special nodes connected.\n")

    # helper method which creates the final networks
    def create_final_networks(self):
        
        print("Creating final meso network...")

        mfhn_all_gdb = self.mfhn_all_gdb
        
        fields = ["NODE_ID", "SHAPE@X", "SHAPE@Y", "MESOZONE"]
        freightnode_fc = os.path.join(mfhn_all_gdb, "special_nodes")
        freightnode_dict = {}

        with arcpy.da.SearchCursor(freightnode_fc, fields) as scursor:
            for row in scursor:

                node_dict = {"POINT_X": row[1], "POINT_Y": row[2], "MESOZONE": row[3]}
                freightnode_dict[row[0]] = node_dict

        fields = ["NODE", "SHAPE@X", "SHAPE@Y", "MESOZONE"]
        hwynode_fc = os.path.join(mfhn_all_gdb, "hwynode_zone")
        hwynode_dict = {}

        with arcpy.da.SearchCursor(hwynode_fc, fields) as scursor:
            for row in scursor:

                node_dict = {"POINT_X": row[1], "POINT_Y": row[2], "MESOZONE": row[3]}
                hwynode_dict[row[0]] = node_dict

        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "final_links", spatial_reference = 26771)
        arcpy.management.CreateFeatureDataset(mfhn_all_gdb, "final_nodes", spatial_reference = 26771)

        hwylink_list = arcpy.ListFeatureClasses(feature_dataset = "hwylinks_meso")

        for fc in hwylink_list:

            year = fc[8:12]

            final_link_workspace = os.path.join(mfhn_all_gdb, "final_links")
            final_link_fc = f"final_links_{year}" 
            arcpy.management.CreateFeatureclass(final_link_workspace, final_link_fc, "POLYLINE", spatial_reference = 26771)

            fc_path = os.path.join(mfhn_all_gdb, "hwylinks_meso", fc)
            final_link_fc = os.path.join(final_link_workspace, final_link_fc)

            arcpy.management.AddFields(final_link_fc, [["INODE", "LONG"], ["JNODE", "LONG"],
                                                  ["DIRECTIONS", "TEXT"], ["MILES", "DOUBLE"], 
                                                  ["LANES1", "SHORT"], ["LANES2", "SHORT"],
                                                  ["TYPE", "TEXT"], ["VDF", "SHORT"], 
                                                  ["MODES", "TEXT"]])
            
            s_fields = ["SHAPE@", "ANODE", "BNODE", "DIRECTIONS", "MILES", 
                        "THRULANES1", "THRULANES2", "HANGING"] 
            i_fields = ["SHAPE@", "INODE", "JNODE", "DIRECTIONS", "MILES", 
                        "LANES1", "LANES2", "TYPE", "VDF", "MODES"]
            
            # creating link class
            # copy the geometry of the highway links
            with arcpy.da.SearchCursor(fc_path, s_fields) as scursor:
                with arcpy.da.InsertCursor(final_link_fc, i_fields) as icursor:

                    for row in scursor:

                        if row[7] != "Y":
                            icursor.insertRow(
                                [row[0], row[1], row[2], row[3], row[4], row[5], row[6], "1", 10, "T"]
                            )

            # add the connector links
            conn_fc = os.path.join(mfhn_all_gdb, "conn_links", f"conn_links_{year}")

            # node id corresponds to freght nodes, anode corresponds to highway nodes 
            s_fields = ["NODE_ID", "ANODE"]

            centroid = self.node_dict["CMAP_centroid"]
            logistic = self.node_dict["CMAP_logistic"]
            
            with arcpy.da.SearchCursor(conn_fc, s_fields) as scursor:
                with arcpy.da.InsertCursor(final_link_fc, i_fields) as icursor:

                    for row in scursor:

                        node_id = row[0]
                        anode = row[1]

                        inode_x = freightnode_dict[node_id]["POINT_X"]
                        inode_y = freightnode_dict[node_id]["POINT_Y"]
                        jnode_x = hwynode_dict[anode]["POINT_X"]
                        jnode_y = hwynode_dict[anode]["POINT_Y"]

                        dist = (math.sqrt((jnode_x - inode_x)**2 + (jnode_y - inode_y)**2))/5280

                        geom_array = arcpy.Array(
                            [arcpy.Point(inode_x, inode_y), arcpy.Point(jnode_x, jnode_y)]
                            )
                        geom = arcpy.Polyline(geom_array, 26771)

                        if node_id in centroid:
                            icursor.insertRow([geom, node_id, anode, "2", dist, 2, 2, "4", 10, "T"])
                        elif node_id in logistic:
                            icursor.insertRow([geom, node_id, anode, "2", dist, 2, 2, "7", 10, "T"])

            # creating node class
            final_link_df = pd.DataFrame(
                data = [row for row in arcpy.da.SearchCursor(final_link_fc, ["INODE", "JNODE"])], 
                columns = ["INODE", "JNODE"])
            
            final_node_set = set(final_link_df.INODE.to_list()) | set(final_link_df.JNODE.to_list())

            final_node_workspace = os.path.join(mfhn_all_gdb, "final_nodes")
            final_node_fc = f"final_nodes_{year}" 
            arcpy.management.CreateFeatureclass(final_node_workspace, final_node_fc, "POINT", spatial_reference = 26771)

            final_node_fc = os.path.join(final_node_workspace, final_node_fc)

            arcpy.management.AddFields(final_node_fc, [["NODE", "LONG"], ["POINT_X", "DOUBLE"], 
                                                       ["POINT_Y", "DOUBLE"], ["MESOZONE", "LONG"]])

            i_fields = ["SHAPE@", "NODE", "POINT_X", "POINT_Y", "MESOZONE"]

            with arcpy.da.InsertCursor(final_node_fc, i_fields) as icursor:
                for node in final_node_set:

                    if node in centroid or node in logistic:

                        point_x = freightnode_dict[node]["POINT_X"]
                        point_y = freightnode_dict[node]["POINT_Y"]
                        mesozone = freightnode_dict[node]["MESOZONE"]

                        point = arcpy.Point(point_x, point_y)
                        geom = arcpy.PointGeometry(point, spatial_reference = 26771)

                        icursor.insertRow([geom, node, point_x, point_y, mesozone])

                    else:
                        
                        point_x = hwynode_dict[node]["POINT_X"]
                        point_y = hwynode_dict[node]["POINT_Y"]
                        mesozone = hwynode_dict[node]["MESOZONE"]

                        point = arcpy.Point(point_x, point_y)
                        geom = arcpy.PointGeometry(point, spatial_reference = 26771)

                        icursor.insertRow([geom, node, point_x, point_y, mesozone])

        print("Final meso network created.")

start_time = time.time()

FN = FreightNetwork()
FN.generate_mfhn()
FN.check_mfn_fcs()
FN.create_meso_layers()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")

print("Done")