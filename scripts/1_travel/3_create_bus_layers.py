## 3_create_bus_layers.py
## a translation of generate_transit_files.py (the first half)
# ## Author: npeterson
## Translated + Updated by ccai (2025)

import os
import sys
import math
import time
import shutil
import arcpy
import pandas as pd
import networkx as nx

pd.options.mode.chained_assignment = None

from modules.util_functions import create_directional_hwy_records

class BusNetwork:

    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        in_folder = os.path.join(mfhrn_path, "input")
        mhn_in_folder = os.path.join(in_folder, "1_travel")
        self.mhn_in_gdb = os.path.join(mhn_in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        # base + current definition
        self.bus_base = 2019
        self.bus_current = 2024

        # TODO: replace with input
        self.scenario_dict = {
            3: 2030
        }

        self.bn_out_folder = os.path.join(self.mhn_out_folder, "bus_network")

        # how similar bus runs have to be to be collapsed
        self.threshold = 0.85

        self.tod_dict = {
            1: {"description": "6 PM - 6 AM", # overnight
                "where_clause": "STARTHOUR >= 18 OR STARTHOUR < 6",
                "maxtime": 720,
                "hwy_tod": 1,
                "hdwy_mult": 4},
            2: {"description": "6 AM - 9 AM", # AM peak
                "where_clause": "STARTHOUR >= 6 AND STARTHOUR < 9",
                "maxtime": 180,
                "hwy_tod": 3,
                "hdwy_mult": 1},
            3: {"description": "9 AM - 4 PM", # midday
                "where_clause": "STARTHOUR >= 9 AND STARTHOUR < 16", 
                "maxtime": 420,
                "hwy_tod": 5,
                "hdwy_mult": 3},
            4: {"description": "4 PM - 6 PM", # PM peak
                "where_clause": "STARTHOUR >= 16 AND STARTHOUR < 18",
                "maxtime": 120,
                "hwy_tod": 7,
                "hdwy_mult": 1}
        }

        self.default_speed = 30

        self.node_dict = self.build_hwy_node_dict()
        self.link_dict = self.build_hwy_link_dict()

        self.error_file_1 = os.path.join(self.bn_out_folder, "error_file_1.txt")
        self.error_file_2 = os.path.join(self.bn_out_folder, "error_file_2.txt")

    # MAIN METHODS --------------------------------------------------------------------------------

    # method that creates the bus network folder
    def create_bn_folder(self):
        
        bn_out_folder = self.bn_out_folder

        if os.path.isdir(bn_out_folder) == True:
            shutil.rmtree(bn_out_folder)

        os.mkdir(bn_out_folder)

        print("Bus network output folder created.\n")

    # method that collapses routes
    def collapse_bus_routes(self):

        print("Collapsing routes by TOD...")

        mhn_in_gdb = self.mhn_in_gdb
        bn_out_folder = self.bn_out_folder

        cr_gdb_name = "collapsed_routes.gdb"
        arcpy.management.CreateFileGDB(bn_out_folder, cr_gdb_name)
        cr_gdb = os.path.join(bn_out_folder, cr_gdb_name)

        # copy fcs into gdb
        self.copy_bus_fcs("bus_base")
        self.copy_bus_fcs("bus_current")
        self.copy_bus_fcs("bus_future")

        itin_fields = ["TRANSIT_LINE", "ITIN_ORDER", "ITIN_A", "ITIN_B", "ABB",
                       "DWELL_CODE", "LINE_SERV_TIME", "TTF"]
        
        # get dfs + dict for bus base itin
        base_itin = os.path.join(mhn_in_gdb, "bus_base_itin")
        base_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(base_itin, itin_fields)],
            columns = itin_fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        base_itin_dict = {k: v.to_dict(orient='records') for k, v in base_itin_df.groupby("TRANSIT_LINE")}
        base_rf_dict = self.reformat_gtfs_feed(base_itin_dict)

        # get dfs + dicts for bus current itin
        current_itin = os.path.join(mhn_in_gdb, "bus_current_itin")
        current_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(current_itin, itin_fields)],
            columns = itin_fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        current_itin_dict = {k: v.to_dict(orient='records') for k, v in current_itin_df.groupby("TRANSIT_LINE")}
        current_rf_dict = self.reformat_gtfs_feed(current_itin_dict)

        # get dfs + dicts for bus future itin
        future_itin = os.path.join(mhn_in_gdb, "bus_future_itin")
        future_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(future_itin, itin_fields)],
            columns = itin_fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        future_itin_dict = {k: v.to_dict(orient='records') for k, v in future_itin_df.groupby("TRANSIT_LINE")}

        # make feature layers
        base_fc = os.path.join(cr_gdb, "bus_base")
        arcpy.management.MakeFeatureLayer(base_fc, "base_layer")
        current_fc = os.path.join(cr_gdb, "bus_current")
        arcpy.management.MakeFeatureLayer(current_fc, "current_layer")
        future_fc = os.path.join(cr_gdb, "bus_future")
        arcpy.management.MakeFeatureLayer(future_fc, "future_layer")

        error_file= open(self.error_file_1, "a")

        for tod in [1, 2, 3, 4]:

            print(f"Collapsing routes for TOD {tod}...")

            arcpy.management.CreateFeatureDataset(cr_gdb, f"TOD_{tod}", spatial_reference = 26771)

            # collapse gtfs routes
            self.find_rep_runs(tod= tod, which_gtfs="base", rf_dict = base_rf_dict)
            self.find_rep_runs(tod= tod, which_gtfs="current", rf_dict = current_rf_dict)

            # find rep itins
            self.find_rep_itins(
                tod= tod, which_bus = "base", itin_dict = base_itin_dict, error_file = error_file)
            self.find_rep_itins(
                tod= tod, which_bus = "current", itin_dict = current_itin_dict, error_file = error_file)
            
        col_future_fc = os.path.join(cr_gdb, f"col_future_0")
        arcpy.management.CopyFeatures("future_layer", col_future_fc)
        self.find_rep_itins(
            tod= 0, which_bus = "future", itin_dict = future_itin_dict, error_file = error_file)

        error_file.close()

        # copy park n ride table
        input_table = os.path.join(mhn_in_gdb, "parknride")
        arcpy.management.CreateFeatureclass(cr_gdb, "parknride", "POINT", spatial_reference = 26771)
        add_fields = [
            ["FACILITY", "TEXT"], ["NODE", "LONG"],
            ["COST", "SHORT"], ["SPACES", "SHORT"],
            ["ESTIMATE", "SHORT"], ["SCENARIO", "TEXT"]
        ]
        output_fc = os.path.join(cr_gdb, "parknride")
        arcpy.management.AddFields(output_fc, add_fields)

        sfields = ["FACILITY", "NODE", "COST", "SPACES", "ESTIMATE", "SCENARIO"]
        ifields = sfields + ["SHAPE@"]

        with arcpy.da.SearchCursor(input_table, sfields) as scursor:
            with arcpy.da.InsertCursor(output_fc, ifields) as icursor:

                for row in scursor:
                    
                    node = row[1]
                    geom = self.node_dict[node]["GEOM"]

                    row = list(row) + [geom]

                    icursor.insertRow(row)

        arcpy.management.Delete(base_fc)
        arcpy.management.Delete(current_fc)
        arcpy.management.Delete(future_fc)

        print("TOD routes collapsed.\n")

    # method that creates bus layers for each scenario
    def create_bus_layers(self):

        print("Creating bus layers...")

        scenario_dict = self.scenario_dict
        bn_out_folder = self.bn_out_folder
        node_dict = self.node_dict

        mhn_all_gdb = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        if os.path.exists(self.error_file_2):
            os.remove(self.error_file_2)

        error_file= open(self.error_file_2, "a")

        for scen in scenario_dict:

            year = scenario_dict[scen]

            scen_gdb_name = f"SCENARIO_{scen}.gdb"
            scen_gdb = os.path.join(bn_out_folder, scen_gdb_name)

            if arcpy.Exists(scen_gdb):
                arcpy.management.Delete(scen_gdb)

            # make the gdb
            arcpy.management.CreateFileGDB(bn_out_folder, scen_gdb_name)

            # copy the links over
            input_links = os.path.join(mhn_all_gdb, "hwylinks_all", f"HWYLINK_{year}")
            arcpy.management.MakeFeatureLayer(input_links, "input_links_layer", "NEW_BASELINK = '1'")
            output_links = os.path.join(scen_gdb, f"HWYLINK_{year}")
            arcpy.management.CopyFeatures("input_links_layer", output_links)

            arcpy.management.Delete("input_links_layer")

            scen_nodes = set(node_dict.keys())

            # for each tod
            for tod in [1, 2, 3, 4]:

                print(f"Creating bus layers for scenario {scen} TOD {tod}...")

                arcpy.management.CreateFeatureDataset(scen_gdb, f"TOD_{tod}", spatial_reference = 26771)

                # find highway links
                G = self.create_tod_hwy_networks(scen, tod)
                scen_nodes = scen_nodes & set(G.nodes)

                # find bus networks
                reroute_dict = self.create_tod_bus_runs(scen, tod)
                self.create_tod_bus_itins(scen, tod, reroute_dict, G, error_file)

            # find scenario park n ride nodes
            cr_gdb = os.path.join(bn_out_folder, f"collapsed_routes.gdb")

            input_nodes = os.path.join(cr_gdb, "parknride")
            arcpy.management.MakeFeatureLayer(input_nodes, "input_nodes_layer", f"SCENARIO LIKE '%{scen}%'")
            output_nodes = os.path.join(scen_gdb, "scen_parknride")
            arcpy.management.CopyFeatures("input_nodes_layer", output_nodes)

            arcpy.management.Delete("input_nodes_layer")

            error_file.write("\n")
            
            error_file.write(f"Errors in scenario {scen} park and ride nodes:\n")
            error_file.write(f"------------------------------------\n")

            with arcpy.da.UpdateCursor(output_nodes, ["NODE", "FACILITY", "SHAPE@"]) as ucursor:
                for row in ucursor:

                    facility = row[1]

                    if row[0] not in scen_nodes:
                        
                        replace_node = self.find_nearest_node(row[0], scen_nodes.copy(), zone = True)
                        if replace_node == None:

                            error_file.write(f"Node for {facility} could not be found/replaced. Removing facility.")
                            ucursor.deleteRow()

                        else:

                            replace_geom = node_dict[replace_node]["GEOM"]
                            ucursor.updateRow([replace_node, facility, replace_geom])

        error_file.close()

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that builds highway node geometry dict
    def build_hwy_node_dict(self):

        print("Building dictionary of all highway nodes...")

        mhn_in_gdb = self.mhn_in_gdb

        node_fields = ["NODE", "SHAPE@X", "SHAPE@Y", "zone17"]

        hwynode_fc = os.path.join(mhn_in_gdb, "hwynet", "hwynet_node")

        node_dict = {}

        with arcpy.da.SearchCursor(hwynode_fc, node_fields) as scursor:
            for row in scursor:

                node = row[0]
                point = arcpy.Point(row[1], row[2])
                geom = arcpy.PointGeometry(point, spatial_reference = 26771)
                zone = row[3]

                node_dict[node] = {"GEOM": geom, "ZONE": zone}

        print("Node dictionary built.")

        return node_dict
    
    # helper method that builds highway link geometry dict
    def build_hwy_link_dict(self):

        print("Building dictionary of all highway links...")

        mhn_in_gdb = self.mhn_in_gdb

        link_fields = ["ANODE", "BNODE", "ABB", "DIRECTIONS"]

        hwylink_fc = os.path.join(mhn_in_gdb, "hwynet", "hwynet_arc")
        hwylink_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwylink_fc, link_fields)], 
            columns = link_fields)
        
        hwylink_rev_df = pd.merge(hwylink_df, hwylink_df.copy(), 
                                  left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        hwylink_rev_set = set(hwylink_rev_df.ABB_x.to_list())

        link_fields = ["SHAPE@", "ANODE", "BNODE", "ABB", "MILES"]

        link_dict = {}

        with arcpy.da.SearchCursor(hwylink_fc, link_fields) as scursor:
            for row in scursor:

                geom = row[0]
                anode = row[1]
                bnode = row[2]
                abb = row[3]
                miles = row[4]

                multi_array = arcpy.Array()
                for part in geom:
                    part_array = arcpy.Array([point for point in part])
                    multi_array.append(part_array)
                    
                polyline = arcpy.Polyline(multi_array, spatial_reference = 26771)

                link_dict[(anode, bnode)] = {"ABB": abb, "GEOM": polyline, "MILES": miles}
                if abb not in hwylink_rev_set:
                    link_dict[(bnode, anode)] = {"ABB": abb, "GEOM": polyline, "MILES": miles}

        print("Link dictionary built.\n")

        return link_dict



    # helper method to copy fcs to collapse routes
    def copy_bus_fcs(self, fc_name):

        # copy current fc over
        input_fc = os.path.join(self.mhn_in_gdb, "hwynet", fc_name) 

        cr_gdb = os.path.join(self.bn_out_folder, "collapsed_routes.gdb")
        arcpy.management.CreateFeatureclass(cr_gdb, fc_name, template = input_fc, spatial_reference = 26771)
        output_fc = os.path.join(cr_gdb, fc_name)

        exclude_fields = ["OBJECTID", "Shape", "Shape_Length"]
        fields = [f.name for f in arcpy.ListFields(input_fc) if (f.name not in exclude_fields)]
        fields += ["SHAPE@"]

        with arcpy.da.SearchCursor(input_fc, fields) as scursor:
            with arcpy.da.InsertCursor(output_fc, fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

        if fc_name == "bus_future":
            return
        
        # fix issue with starting after midnight
        fields = ["START", "STARTHOUR"]

        with arcpy.da.UpdateCursor(output_fc, fields) as ucursor:
            for row in ucursor:

                if row[0] >= 86400:
                    row[0] = row[0] - 86400

                if row[1] >= 24:
                    row[1] = row[1] - 24

                ucursor.updateRow(row)

        arcpy.management.CalculateField(
            in_table = output_fc,
            field = "MODERTE",
            expression='!MODE! + "-" + !ROUTE_ID!', 
            expression_type="PYTHON3", 
            code_block="", 
            field_type="TEXT", 
            enforce_domains="NO_ENFORCE_DOMAINS"
        )

        arcpy.management.AddField(output_fc, "BUS_GROUP", "SHORT")
        arcpy.management.AddField(output_fc, "AVG_HEADWAY", "FLOAT")

    # helper method to reformat feed
    def reformat_gtfs_feed(self, itin_dict):

        reformat_dict = {}

        for tr_line in itin_dict:
            tr_itin = itin_dict[tr_line]
            tr_path = []

            for record in tr_itin:
                
                itin_a = record["ITIN_A"]
                itin_b = record["ITIN_B"]
                dw_code = record["DWELL_CODE"]

                record_string = f"{itin_a}-{itin_b}-{dw_code}"
                tr_path.append(record_string)

            reformat_dict[tr_line] = tr_path

        return reformat_dict
    
    # helper method that finds representative runs
    def find_rep_runs(self, tod, which_gtfs, rf_dict):

        where_clause = self.tod_dict[tod]["where_clause"]
        maxtime = self.tod_dict[tod]["maxtime"]

        bus_layer = f"{which_gtfs}_layer"

        arcpy.management.SelectLayerByAttribute(
            bus_layer, "NEW_SELECTION", where_clause)
        
        tod_fd = os.path.join(self.bn_out_folder, "collapsed_routes.gdb", f"TOD_{tod}")
        all_tod_fc = os.path.join(tod_fd, f"all_{which_gtfs}_{tod}")

        arcpy.management.CopyFeatures(bus_layer, all_tod_fc)

        # get lines and group by MODE- ROUTE_ID (MODERTE)
        lines_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(all_tod_fc, ["MODERTE", "TRANSIT_LINE"])], 
            columns = ["MODERTE", "TRANSIT_LINE"])

        # make sure it's the same order every time
        lines_df = lines_df.sort_values("TRANSIT_LINE")
        
        lines_dict = lines_df.groupby('MODERTE')['TRANSIT_LINE'].apply(list).to_dict()

        group = 0
        groups = {}

        # find routes similar enough to be collapsed
        for moderte in lines_dict:

            mr_lines = lines_dict[moderte]

            while True:

                group += 1
                base_run = mr_lines.pop(0)
                groups[base_run] = group

                # compare all other runs against base run
                for comp_run in mr_lines[:]: # make a copy to iterate safely

                    base_run_path = rf_dict[base_run]
                    comp_run_path = rf_dict[comp_run]

                    base_run_set = set(base_run_path)
                    comp_run_set = set(comp_run_path)
                    common_set = base_run_set & comp_run_set

                    base_run_num = len(base_run_set)
                    common_num = len(common_set)

                    if common_num/ base_run_num >= self.threshold:
                        groups[comp_run] = group
                        mr_lines.remove(comp_run)
                
                if len(mr_lines) == 0:
                    break

        with arcpy.da.UpdateCursor(all_tod_fc, ["TRANSIT_LINE", "BUS_GROUP"]) as ucursor:
            for row in ucursor:

                tr_line = row[0]
                row[1] = groups[tr_line]

                ucursor.updateRow(row)

        # find representative routes
        # get number of segments 
        num_seg_dict = {k: len(v) for k, v in rf_dict.items()}

        # get line df 
        lines_fields = ["BUS_GROUP", "TRANSIT_LINE", "START"]
        
        lines_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(all_tod_fc, lines_fields)], 
            columns = lines_fields)
        
        lines_df["NUM_SEGS"] = lines_df["TRANSIT_LINE"].apply(lambda x: num_seg_dict[x])
        
        # comes earliest in that time period (adjusts for TOD 1)
        lines_df["START"] = lines_df["START"].apply(lambda x: x + 86400 if x < 21600 else x)

        # calculate headway
        lines_df = lines_df.sort_values(["BUS_GROUP", "START"])
        lines_df["PREV_START"] = lines_df.groupby("BUS_GROUP")["START"].shift()
        subtract_df = lines_df[lines_df.PREV_START.notnull()]
        subtract_df["HEADWAY"] = (subtract_df["START"] - subtract_df["PREV_START"]) /60

        headway_dict = subtract_df.groupby("BUS_GROUP")["HEADWAY"].mean().to_dict()

        # longest, then starts earliest
        lines_df = lines_df.sort_values(["BUS_GROUP", "NUM_SEGS", "START"], 
                                        ascending = [True, False, True])
        col_routes = lines_df.groupby("BUS_GROUP").first()["TRANSIT_LINE"].to_list()

        col_tod_fc = os.path.join(tod_fd, f"col_{which_gtfs}_{tod}")
        arcpy.management.CopyFeatures(all_tod_fc, col_tod_fc)

        fields = ["TRANSIT_LINE", "BUS_GROUP", "AVG_HEADWAY"]

        with arcpy.da.UpdateCursor(col_tod_fc, fields) as ucursor:
            for row in ucursor:

                if row[0] not in col_routes:
                    ucursor.deleteRow()

                else:
                    bus_group = row[1]
                    
                    if bus_group in headway_dict:
                        row[2] = round(headway_dict[bus_group], 1)
                    else:
                        row[2] = maxtime

                    ucursor.updateRow(row)

    # helper method that finds representative itineraries
    def find_rep_itins(self, tod, which_bus, itin_dict, error_file):

        link_dict = self.link_dict

        error_file.write("\n")

        if which_bus in ["base", "current"]:
            error_file.write(f"Errors in bus {which_bus} TOD {tod}:\n")
            error_file.write(f"------------------------------------\n")
        else:
            error_file.write(f"Errors in bus {which_bus}:\n")
            error_file.write(f"------------------------------------\n")

        cr_gdb = os.path.join(self.bn_out_folder, "collapsed_routes.gdb")

        tod_fd = os.path.join(cr_gdb, f"TOD_{tod}")
        itin_fc_name = f"itin_{which_bus}_{tod}"

        itin_fc = None

        if which_bus in ["base", "current"]:
            itin_fc = os.path.join(tod_fd, itin_fc_name)
            arcpy.management.CreateFeatureclass(tod_fd, itin_fc_name, "POLYLINE")
        else:
            itin_fc = os.path.join(cr_gdb, itin_fc_name)
            arcpy.management.CreateFeatureclass(cr_gdb, itin_fc_name, "POLYLINE", spatial_reference = 26771)

        add_fields = [
            ["TRANSIT_LINE", "TEXT"], ["ITIN_ORDER", "SHORT"],
            ["ITIN_A", "LONG"], ["ITIN_B", "LONG"],
            ["ABB", "TEXT"], ["DWELL_CODE", "TEXT"], 
            ["LINE_SERV_TIME", "FLOAT"], ["TTF", "FLOAT"],
            ["NOTES", "TEXT"]
        ]

        arcpy.management.AddFields(itin_fc, add_fields)

        # get rep routes 
        rep_fc = None

        if which_bus in ["base", "current"]:
            rep_fc = os.path.join(tod_fd, f"col_{which_bus}_{tod}")
        else:
            rep_fc = os.path.join(cr_gdb, f"col_{which_bus}_{tod}")

        rep_routes = [row[0] for row in arcpy.da.SearchCursor(rep_fc, ["TRANSIT_LINE"])]

        # get rep itins
        fields = ["SHAPE@", "TRANSIT_LINE", "ITIN_ORDER", 
                  "ITIN_A", "ITIN_B", "ABB", 
                  "DWELL_CODE", "LINE_SERV_TIME", "TTF", "NOTES"]
        with arcpy.da.InsertCursor(itin_fc, fields) as icursor:
            for tr_line in itin_dict:

                if tr_line not in rep_routes:
                    continue

                itin = itin_dict[tr_line]

                itin_order = 0
                for i in range(0, len(itin)):

                    record = itin[i]
                    itin_order += 1

                    itin_a = record["ITIN_A"]
                    itin_b = record["ITIN_B"]
                    abb = record["ABB"]
                    dwc = record["DWELL_CODE"]
                    lst = record["LINE_SERV_TIME"]
                    ttf = record["TTF"]

                    if (itin_a, itin_b) in link_dict:
                        geom = link_dict[(itin_a, itin_b)]["GEOM"]
                        notes = None
                    else:
                        geom = None
                        notes = "False Link"
                        error_file.write(f"{tr_line} - false link between {itin_a}, {itin_b}\n")
                    
                    row = [geom, tr_line, itin_order, itin_a, itin_b,
                           abb, dwc, lst, ttf, notes]
                    icursor.insertRow(row)

                    if i == len(itin) - 1:
                        continue

                    # check for itinerary gaps
                    next_record = itin[i+1]
                    next_a = next_record["ITIN_A"]

                    if itin_b != next_a:

                        itin_order+= 1
                        row = [None, tr_line, itin_order, itin_b, next_a,
                               # abb, dwc, lst, ttf, notes
                               None, 1, 0, 1, "Itin Gap"] 
                        
                        error_file.write(f"{tr_line} - itinerary gap between {itin_b}, {next_a}\n")
                        icursor.insertRow(row)



    # helper method which makes tod highway networks 
    def create_tod_hwy_networks(self, scen, tod):

        bn_out_folder = self.bn_out_folder
        link_dict = self.link_dict

        scen_gdb = os.path.join(bn_out_folder, f"SCENARIO_{scen}.gdb")

        tod_fd = os.path.join(scen_gdb, f"TOD_{tod}")
        arcpy.management.CreateFeatureclass(tod_fd, f"HWYLINK_{tod}", "POLYLINE")
        hwylink_tod_fc = os.path.join(tod_fd, f"HWYLINK_{tod}")

        add_fields = [
            ["ANODE", "LONG"], ["BNODE", "LONG"], 
            ["ABB", "TEXT"], ["MILES", "DOUBLE"], ["THRULANES", "SHORT"], 
            ["TYPE", "TEXT"] 
        ]

        arcpy.management.AddFields(hwylink_tod_fc, add_fields)

        fields = ["SHAPE@", "ANODE", "BNODE", "ABB", 
                  "MILES", "THRULANES", "TYPE"]
        
        year = self.scenario_dict[scen]
        hwylink_fc = os.path.join(scen_gdb, f"HWYLINK_{year}")

        hwylink_records = create_directional_hwy_records(hwylink_fc, 
                                                         where_clause = "NEW_BASELINK = '1'")
        hwylink_df = pd.DataFrame(hwylink_records)

        # The highway TOD that the bus TOD corresponds to
        hwy_tod = self.tod_dict[tod]["hwy_tod"]

        ampm_links = []
        if hwy_tod == 1:
            ampm_links += ["1", "3", "4"]
        elif hwy_tod == 3:
            ampm_links += ["1", "2", "5"]
        elif hwy_tod == 5:
            ampm_links += ["1", "2", "4"]
        elif hwy_tod == 7:
            ampm_links += ["1", "3", "5"]

        hwylink_tod_df = hwylink_df[(hwylink_df.AMPM.isin(ampm_links)) &
                                    (hwylink_df.TYPE != "6")] # no centroids allowed
        hwylink_tod_dict = hwylink_tod_df.set_index(["INODE", "JNODE"]).to_dict("index")

        G = nx.DiGraph()

        with arcpy.da.InsertCursor(hwylink_tod_fc, fields) as icursor:
            for link in hwylink_tod_dict:

                anode = link[0]
                bnode = link[1]

                abb = link_dict[(anode, bnode)]["ABB"]
                geom = link_dict[(anode, bnode)]["GEOM"]

                # miles
                miles = hwylink_tod_dict[link]["MILES"]

                G.add_edge(anode, bnode, weight = miles)

                # calculate # of lanes
                lanes = hwylink_tod_dict[link]["THRULANES"]
                parklanes = hwylink_tod_dict[link]["PARKLANES"]
                parkres = hwylink_tod_dict[link]["PARKRES"]

                if str(hwy_tod) in parkres:
                    lanes += parklanes
                    parklanes = 0

                # vdf
                vdf = hwylink_tod_dict[link]["TYPE"]

                row = [geom, anode, bnode, abb, miles, lanes, vdf]
                icursor.insertRow(row)

        return G
    
    # helper method which makes tod bus runs
    def create_tod_bus_runs(self, scen, tod):

        bn_out_folder = self.bn_out_folder

        cr_gdb = os.path.join(bn_out_folder, f"collapsed_routes.gdb")
        scen_gdb = os.path.join(bn_out_folder, f"SCENARIO_{scen}.gdb")

        maxtime = self.tod_dict[tod]["maxtime"]
        hdwy_mult = self.tod_dict[tod]["hdwy_mult"]

        which_gtfs = "base"
        if scen > 1:
            which_gtfs = "current"

        in_fc = os.path.join(cr_gdb, f"TOD_{tod}", f"col_{which_gtfs}_{tod}")
        # REP GTFS FC
        rep_gtfs_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"col_{which_gtfs}_{tod}") 
        arcpy.management.CopyFeatures(in_fc, rep_gtfs_fc)

        # REP FUTURE FC
        in_fc = os.path.join(cr_gdb, "col_future_0")
        where_clause = f"SCENARIO LIKE '%{scen}%'"
        arcpy.management.MakeFeatureLayer(in_fc, "in_layer", where_clause)
        where_clause = f"TOD = '0' Or TOD LIKE '%{tod}%'"
        arcpy.management.SelectLayerByAttribute("in_layer", "SUBSET_SELECTION", where_clause)

        rep_future_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"col_future_{tod}")

        arcpy.management.CopyFeatures("in_layer", rep_future_fc)
        arcpy.management.Delete("in_layer")

        # process project coding
        tod_fd = os.path.join(scen_gdb, f"TOD_{tod}")
        arcpy.management.CreateFeatureclass(tod_fd, f"scen_line_{tod}", "POLYLINE")
        rep_scen_fc = os.path.join(tod_fd, f"scen_line_{tod}")

        add_fields = [
            ["TRANSIT_LINE", "TEXT"], ["DESCRIPTION", "TEXT"],
            ["MODE", "TEXT"], ["VEHICLE_TYPE", "TEXT"], ["HEADWAY", "FLOAT"], 
            ["SPEED", "SHORT"], ["MODERTE", "TEXT"], ["NOTES", "TEXT"]
        ]

        arcpy.management.AddFields(rep_scen_fc, add_fields)

        fields = ["TRANSIT_LINE", "REPLACE", "REROUTE"]
        rep_future_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(rep_future_fc, fields)], 
            columns = fields
        )

        # find added runs
        add_df = rep_future_df[~(rep_future_df.REPLACE.str.contains("-")) &
                               ~(rep_future_df.REROUTE.str.contains("-"))]
        add_list = add_df.TRANSIT_LINE.to_list()

        # find replaced runs
        replace_df = rep_future_df[(rep_future_df.REPLACE.str.contains("-"))]
        replace_dict = replace_df.set_index("TRANSIT_LINE")["REPLACE"].to_dict()
        replace_dict = {k: v.split(":") for k, v in replace_dict.items()}

        replace_modertes = []
        for moderte_list in replace_dict.values():
            replace_modertes += moderte_list

        replace_modertes = set(replace_modertes)

        # find rerouted runs
        reroute_df = rep_future_df[(rep_future_df.REROUTE.str.contains("-"))]
        inv_reroute_dict = reroute_df.set_index("TRANSIT_LINE")["REROUTE"].to_dict()
        inv_reroute_dict = {k: v.split(":") for k, v in inv_reroute_dict.items()}

        reroute_dict = {}

        for tr_line in inv_reroute_dict:
            for moderte in inv_reroute_dict[tr_line]:

                if moderte not in reroute_dict:
                    reroute_dict[moderte] = []

                reroute_dict[moderte].append(tr_line)

        # transfer existing runs over
        sfields = ["SHAPE@", "TRANSIT_LINE", "DESCRIPTION", "MODE",
                   "VEHICLE_TYPE", "AVG_HEADWAY", "SPEED", "MODERTE"]
        ifields = ["SHAPE@", "TRANSIT_LINE", "DESCRIPTION", "MODE",
                   "VEHICLE_TYPE", "HEADWAY", "SPEED", "MODERTE"]
        
        mode_hdwys = {}
        replace_hdwys = []

        with arcpy.da.SearchCursor(rep_gtfs_fc, sfields) as scursor:
            with arcpy.da.InsertCursor(rep_scen_fc, ifields) as icursor:

                for row in scursor:

                    tr_line = row[1]
                    desc = row[2]
                    mode = row[3]
                    veh_type = row[4]
                    hdwy = row[5]
                    speed = row[6]
                    moderte = row[7]

                    # if replaced, don't transfer
                    if moderte in replace_modertes:
                        replace_hdwys.append((moderte, hdwy))
                        continue

                    if mode not in mode_hdwys:
                        mode_hdwys[mode] = []

                    mode_hdwys[mode].append(hdwy)

                    icursor.insertRow(
                        [row[0], tr_line, desc, mode, veh_type, hdwy, speed, moderte]
                    )

        mode_hdwys = {k: sum(v)/len(v) for k, v in mode_hdwys.items()}

        # transfer in added + replaced runs
        sfields = ["SHAPE@", "TRANSIT_LINE", "DESCRIPTION", "MODE",
                   "VEHICLE_TYPE", "HEADWAY", "SPEED"]
        
        with arcpy.da.SearchCursor(rep_future_fc, sfields) as scursor:
            with arcpy.da.InsertCursor(rep_scen_fc, ifields) as icursor:

                for row in scursor:

                    tr_line = row[1]

                    if tr_line not in add_list and tr_line not in replace_dict:
                        continue

                    desc = row[2]
                    mode = row[3]
                    veh_type = row[4]
                    hdwy = row[5] # uhhh here we go
                    speed = row[6]

                    coded_hdwy = hdwy * hdwy_mult
                    replaced_hdwy = 0

                    if tr_line in replace_dict:
                        
                        replace_modertes = replace_dict[tr_line]
                        replaced_hdwy = self.find_replaced_headway(
                            replace_modertes, replace_hdwys)
                        
                    mode_hdwy = mode_hdwys[mode]
                    last_hdwy = 90 # last chance headway

                    final_hdwy = 0 # calculate final headway

                    if coded_hdwy != 0:
                        if replaced_hdwy != 0:
                            final_hdwy = min(coded_hdwy, replaced_hdwy)
                        else:
                            final_hdwy = coded_hdwy
                    else:
                        if replaced_hdwy != 0:
                            final_hdwy = replaced_hdwy
                        else:
                            final_hdwy = max(mode_hdwy, last_hdwy)

                    final_hdwy = min(final_hdwy, maxtime)

                    icursor.insertRow(
                        [row[0], tr_line, desc, mode, veh_type, final_hdwy, speed, None]
                    )

        arcpy.management.Delete(rep_gtfs_fc)
        arcpy.management.Delete(rep_future_fc)

        return reroute_dict
    
    # helper method that finds the replaced headway
    def find_replaced_headway(self, replace_modertes, replace_hdwys):

        headways = []

        for pair in replace_hdwys:

            if pair[0] in replace_modertes:
                headways.append(pair[1])

        if len(headways) > 0:
            return min(headways)
        else:
            return 0
        
    # helper method which makes tod bus itineraries
    def create_tod_bus_itins(self, scen, tod, reroute_dict, G, 
                             error_file):

        bn_out_folder = self.bn_out_folder

        cr_gdb = os.path.join(bn_out_folder, f"collapsed_routes.gdb")
        scen_gdb = os.path.join(bn_out_folder, f"SCENARIO_{scen}.gdb")

        which_gtfs = "base"
        if scen > 1:
            which_gtfs = "current"

        # get itins as dicts
        tod_fd = os.path.join(cr_gdb, f"TOD_{tod}")
        itin_gtfs_fc = os.path.join(tod_fd, f"itin_{which_gtfs}_{tod}")
        itin_future_fc = os.path.join(cr_gdb, f"itin_future_0")

        error_file.write("\n")

        error_file.write(f"Errors in scenario {scen} TOD {tod} transit lines:\n")
        error_file.write(f"------------------------------------\n")

        exclude_fields = ["OBJECTID", "Shape", "Shape_Length"]
        fields = [f.name for f in arcpy.ListFields(itin_gtfs_fc) if (f.name not in exclude_fields)]

        itin_gtfs_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(itin_gtfs_fc, fields)],
            columns = fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        itin_gtfs_dict = {k: v.to_dict(orient='records') for k, v in itin_gtfs_df.groupby("TRANSIT_LINE")}

        itin_future_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(itin_future_fc, fields)],
            columns = fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        itin_future_dict = {k: v.to_dict(orient='records') for k, v in itin_future_df.groupby("TRANSIT_LINE")}

        # all the lines for the scenario
        tod_fd = os.path.join(scen_gdb, f"TOD_{tod}")
        rep_scen_fc = os.path.join(tod_fd, f"scen_line_{tod}")

        transit_lines = {row[0]: row[1] for row in arcpy.da.SearchCursor(rep_scen_fc, ["TRANSIT_LINE", "MODERTE"])}

        # create itin 
        arcpy.management.CreateFeatureclass(tod_fd, f"scen_itin_{tod}", "POLYLINE", itin_gtfs_fc)
        rep_itin_fc = os.path.join(tod_fd, f"scen_itin_{tod}")

        fields = ["SHAPE@", "TRANSIT_LINE", "ITIN_ORDER", 
                  "ITIN_A", "ITIN_B", "ABB", 
                  "DWELL_CODE", "LINE_SERV_TIME", "TTF", "NOTES"]
        
        with arcpy.da.InsertCursor(rep_itin_fc, fields) as icursor:

            for transit_line in transit_lines:

                moderte = transit_lines[transit_line]
                line_itin = None

                if transit_line in itin_gtfs_dict:
                    line_itin = itin_gtfs_dict[transit_line]

                elif transit_line in itin_future_dict:
                    line_itin = itin_future_dict[transit_line]

                final_itin = self.make_final_line_itin(transit_line, line_itin,
                                                       moderte, reroute_dict, itin_future_dict, 
                                                       G, error_file)
                
                for record in final_itin:

                    itin_order = record["ITIN_ORDER"]
                    itin_a = record["ITIN_A"]
                    itin_b = record["ITIN_B"]
                    abb = record["ABB"]

                    dwc = record["DWELL_CODE"]
                    lst = record["LINE_SERV_TIME"]
                    ttf = record["TTF"]
                    notes = record["NOTES"]

                    geom = self.link_dict[(itin_a, itin_b)]["GEOM"]

                    icursor.insertRow(
                        [geom, transit_line, itin_order, 
                         itin_a, itin_b, abb, 
                         dwc, lst, ttf, notes]
                    )

    # helper method that makes final line itin
    def make_final_line_itin(self, transit_line, line_itin, moderte, 
                             reroute_dict, itin_future_dict, G, error_file):

        # first- reroute
        anodes = [record["ITIN_A"] for record in line_itin]
        bnodes = [record["ITIN_B"] for record in line_itin]

        # attempt to reroute
        if moderte in reroute_dict:

            reroute = 0
            reroute_lines = reroute_dict[moderte]

            for reroute_line in reroute_lines:

                reroute_itin = itin_future_dict[reroute_line]
                start_node = reroute_itin[0]["ITIN_A"]
                end_node = reroute_itin[-1]["ITIN_B"]

                if start_node in anodes and end_node in bnodes:

                    start_index = anodes.index(start_node)
                    end_index = bnodes.index(end_node)

                    if start_index <= end_index:

                        first_part = line_itin[: start_index]
                        reroute_part = reroute_itin
                        last_part = line_itin[end_index + 1:]

                        line_itin = first_part + reroute_part + last_part
                        reroute = 1

            if reroute == 0:
                error_file.write(f"WARNING: Could not reroute {transit_line} ({moderte})\n")

        # make sure first + last node are secured 
        available_nodes = set(G.nodes())
        first_node = anodes[0]

        if first_node not in available_nodes:

            replace_node = self.find_nearest_node(first_node, available_nodes)
            if replace_node == None:
                error_file.write(f"ERROR: First node of {transit_line} could not be found/replaced. Removing line.\n")
                return []
            
            else:
                line_itin[0]["ITIN_A"] = replace_node

        last_node = bnodes[-1]
        if last_node not in available_nodes:

            replace_node = self.find_nearest_node(last_node, available_nodes)

            if replace_node == None:
                error_file.write(f"ERROR: Last node of {transit_line} could not be found/replaced. Removing line.\n")
                return []
            
            else:
                line_itin[-1]["ITIN_B"] = replace_node

        # make final itinerary
        final_itin = []

        i = 0 # counter that loops through original itin
        itin_order = 0

        while i < len(line_itin):

            record = line_itin[i]
            itin_a = record["ITIN_A"]
            itin_b = record["ITIN_B"]

            # segment is in network
            if G.has_edge(itin_a, itin_b):
                itin_order += 1
                record["ITIN_ORDER"] = itin_order
                final_itin.append(record)

                i+= 1
            # segment is not in network
            else:

                i += 1
                dwc = record["DWELL_CODE"]
                ttf = record["TTF"]

                # get consecutive missing segments
                while i < len(line_itin):

                    recordx = line_itin[i]
                    itin_ax = recordx["ITIN_A"]
                    itin_bx = recordx["ITIN_B"]
                    dwcx = recordx["DWELL_CODE"]
                    ttfx = recordx["TTF"]

                    if G.has_edge(itin_ax, itin_bx):
                        break

                    else:
                        
                        itin_b = itin_bx
                        dwc = dwcx
                        ttf = ttfx
                        i += 1

                # is there a path
                if nx.has_path(G, itin_a, itin_b):
                    
                    # find shortest path
                    path = nx.shortest_path(G, itin_a, itin_b)

                    for j in range(0, len(path) - 1):

                        record = {}

                        itin_order += 1
                        record["ITIN_ORDER"] = itin_order
                        record["ITIN_A"] = path[j]
                        record["ITIN_B"] = path[j+1]
                        record["ABB"] = self.link_dict[(path[j], path[j+1])]["ABB"]

                        # assume stop
                        dwcj = 0

                        # if mode is E or Q - change to non-stop
                        if transit_line[0] in ["e", "q"]:
                            dwcj = 1

                        # if last in segment - use original dwc
                        if j == len(path) - 2:
                            dwcj = dwc

                        record["DWELL_CODE"] = dwcj

                        miles = self.link_dict[(path[j], path[j+1])]["MILES"]
                        record["LINE_SERV_TIME"] = max(miles * (60/ self.default_speed), 0.1)

                        record["TTF"] = ttf
                        record["NOTES"] = "Shortest Path"

                        final_itin.append(record)

                else:
                    error_file.write(f"ERROR: Shortest path could not be calculated for {transit_line}. Removing line.\n")
                    return []
        
        if len(final_itin) == 0:
            error_file.write(f"ERROR: Zero segments in {transit_line}. Removing line.")

        return final_itin
    
    # find nearest node
    def find_nearest_node(self, orig_node, available_nodes, zone = False):

        node_dict = self.node_dict

        # this node straight up does not exist
        if orig_node not in node_dict:
            return None
        
        orig_zone = node_dict[orig_node]["ZONE"]
    
        # park n ride - requires it to be the same zone
        if zone == True:

            for node in available_nodes.copy():
                if node_dict[node]["ZONE"] != orig_zone:
                    available_nodes.remove(node)

        if len(available_nodes) == 0:
            return None
        
        orig_node_loc = node_dict[orig_node]["GEOM"]

        available_nodes = list(available_nodes)

        nearest_node = available_nodes[0]
        comp_node_loc = node_dict[nearest_node]["GEOM"]

        nearest_dist = orig_node_loc.distanceTo(comp_node_loc)

        for node in available_nodes:

            comp_node_loc = node_dict[node]["GEOM"]

            dist = orig_node_loc.distanceTo(comp_node_loc)

            if dist < nearest_dist:

                nearest_dist = dist
                nearest_node = node

        return nearest_node

start_time = time.time()

BN = BusNetwork()
# BN.create_bn_folder()
# BN.collapse_bus_routes()
BN.create_bus_layers()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")