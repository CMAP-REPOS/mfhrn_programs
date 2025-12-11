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
            # 1: 2019,
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

        # copy fcs into memory
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


start_time = time.time()

BN = BusNetwork()
BN.create_bn_folder()
BN.collapse_bus_routes()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")

print("Done")