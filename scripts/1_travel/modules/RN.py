## RN.py

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import csv
import arcpy
import pandas as pd
import networkx as nx

pd.options.mode.chained_assignment = None

from .HN import HighwayNetwork

class RailNetwork(HighwayNetwork):

    def __init__(self):

        super().__init__()

        # scenario dict
        self.scenario_dict = {
            # 1: 2019,
            3: 2030
        }
        
        self.mrn_in_gdb = os.path.join(self.mhn_in_folder, "MRN.gdb")
        self.rn_out_folder = os.path.join(self.mhn_out_folder, "rail_network")

        self.tod_dict = {
            1: {"description": "6 PM - 6 AM", # overnight
                "where_clause": "STARTHOUR >= 18 OR STARTHOUR < 6",
                "maxtime": 720},
            2: {"description": "6 AM - 9 AM", # AM peak
                "where_clause": "STARTHOUR >= 6 AND STARTHOUR < 9",
                "maxtime": 180},
            3: {"description": "9 AM - 4 PM", # midday
                "where_clause": "STARTHOUR >= 9 AND STARTHOUR < 16", 
                "maxtime": 420},
            4: {"description": "4 PM - 6 PM", # PM peak
                "where_clause": "STARTHOUR >= 16 AND STARTHOUR < 18",
                "maxtime": 120}
        }

        # how similar rail runs have to be to be collapsed
        self.threshold = 0.85

        self.geom_dict = self.build_geom_dict()

        self.c_graph = nx.DiGraph() # eventually create graph for cta
        self.m_graph = nx.DiGraph() # eventually create graph for metra

    # MAIN METHODS --------------------------------------------------------------------------------

    # method that creates the rail network folder
    def create_rn_folder(self):
        
        rn_out_folder = self.rn_out_folder

        if os.path.isdir(rn_out_folder) == True:
            shutil.rmtree(rn_out_folder)

        os.mkdir(rn_out_folder)

        print("Rail network output folder created.\n")

    # method that collapses routes
    def collapse_rail_routes(self):

        print("Collapsing routes by TOD...")

        mrn_in_gdb = self.mrn_in_gdb
        rn_out_folder = self.rn_out_folder

        cr_gdb_name = "collapsed_routes.gdb"
        arcpy.management.CreateFileGDB(rn_out_folder, cr_gdb_name)
        cr_gdb = os.path.join(rn_out_folder, cr_gdb_name)

        self.current_gdb = cr_gdb

        self.copy_rail_fcs("all_runs_base")
        self.copy_rail_fcs("all_runs")
        self.copy_rail_fcs("future")

        # get directional links
        self.create_rail_network()

        # get dfs + dict for rail base itin
        base_itin = os.path.join(mrn_in_gdb, "all_runs_base_itin")
        base_itin_fields = [f.name for f in arcpy.ListFields(base_itin) if (f.name != "OBJECTID")]
        base_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(base_itin, base_itin_fields)],
            columns = base_itin_fields
        ).sort_values(["TR_LINE", "IT_ORDER"])

        base_itin_dict = {k: v.to_dict(orient='records') for k, v in base_itin_df.groupby("TR_LINE")}
        base_rf_dict = self.reformat_gtfs_feed(base_itin_dict)

        # get dfs + dict for rail current itin
        current_itin = os.path.join(mrn_in_gdb, "all_runs_itin")
        current_itin_fields = [f.name for f in arcpy.ListFields(current_itin) if (f.name != "OBJECTID")]
        current_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(current_itin, current_itin_fields)],
            columns = current_itin_fields
        ).sort_values(["TR_LINE", "IT_ORDER"])

        current_itin_dict = {k: v.to_dict(orient='records') for k, v in current_itin_df.groupby("TR_LINE")}
        current_rf_dict = self.reformat_gtfs_feed(current_itin_dict)

        # get dfs + dict for rail future itin
        future_itin = os.path.join(mrn_in_gdb, "future_itin")
        future_itin_fields = [f.name for f in arcpy.ListFields(future_itin) if (f.name != "OBJECTID")]
        future_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(future_itin, future_itin_fields)],
            columns = future_itin_fields
        ).sort_values(["TR_LINE", "IT_ORDER"])

        future_itin_dict = {k: v.to_dict(orient='records') for k, v in future_itin_df.groupby("TR_LINE")}

        # make feature layers
        base_fc = os.path.join(cr_gdb, "rail_base")
        arcpy.management.MakeFeatureLayer(base_fc, "base_layer")
        current_fc = os.path.join(cr_gdb, "rail_current")
        arcpy.management.MakeFeatureLayer(current_fc, "current_layer")
        future_fc = os.path.join(cr_gdb, "rail_future")
        arcpy.management.MakeFeatureLayer(future_fc, "future_layer")

        for tod in [1, 2, 3, 4]:

            print(f"Collapsing routes for TOD {tod}...")

            arcpy.management.CreateFeatureDataset(cr_gdb, f"TOD_{tod}", spatial_reference = 26771)

            # collapse gtfs routes (cta only)
            self.find_rep_runs(tod= tod, which_gtfs="base", rf_dict = base_rf_dict)
            self.find_rep_runs(tod= tod, which_gtfs="current", rf_dict = current_rf_dict)

            self.find_rep_itins(tod= tod, which_rail = "base", itin_dict = base_itin_dict)
            self.find_rep_itins(tod= tod, which_rail = "current", itin_dict = current_itin_dict)


    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that builds geometry dict
    def build_geom_dict(self):

        print("Building geometry dictionary of all rail links...")

        mrn_in_gdb = self.mrn_in_gdb

        raillink_fc = os.path.join(mrn_in_gdb, "railnet", "railnet_arc")
        fields = ["SHAPE@", "ANODE", "BNODE", "DIRECTIONS"]

        geom_dict = {}

        with arcpy.da.SearchCursor(raillink_fc, fields) as scursor:
            for row in scursor:

                geom = row[0]
                anode = row[1]
                bnode = row[2]
                dirs = row[3]

                multi_array = arcpy.Array()
                for part in geom:
                    part_array = arcpy.Array([point for point in part])
                    multi_array.append(part_array)
                    
                polyline = arcpy.Polyline(multi_array, spatial_reference = 26771)

                geom_dict[(anode, bnode)] = {"GEOM": polyline}

                if dirs == 2:
                    geom_dict[(bnode, anode)] = {"GEOM": polyline}

        print("Geometry dictionary built.\n")

        return geom_dict
    
    # helper method to copy fcs to collapse routes
    def copy_rail_fcs(self, fc_name):

        # copy current fc over
        input_fc = os.path.join(self.mrn_in_gdb, "railnet", fc_name) 

        if fc_name == "all_runs_base":
            new_fc_name = "rail_base"
        elif fc_name == "all_runs":
            new_fc_name = "rail_current"
        else:
            new_fc_name = "rail_future"

        arcpy.management.CreateFeatureclass(self.current_gdb, new_fc_name, template = input_fc, spatial_reference = 26771)
        output_fc = os.path.join(self.current_gdb, new_fc_name)

        exclude_fields = ["OBJECTID", "Shape", "Shape_Length"]
        fields = [f.name for f in arcpy.ListFields(input_fc) if (f.name not in exclude_fields)]
        fields += ["SHAPE@"]

        with arcpy.da.SearchCursor(input_fc, fields) as scursor:
            with arcpy.da.InsertCursor(output_fc, fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

        if fc_name == "future":
            return
        
        # alter field - strthour to starthour 
        arcpy.management.AlterField(
            output_fc, "STRTHOUR", new_field_name= "STARTHOUR", new_field_alias = "STARTHOUR")
        
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
            field = "MR_ID",
            expression='!TR_LINE![0:3] + "-" + !ROUTE_ID!', 
            expression_type="PYTHON3", 
            code_block="", 
            field_type="TEXT", 
            enforce_domains="NO_ENFORCE_DOMAINS"
        )
        
        arcpy.management.AddField(output_fc, "RAIL_GROUP", "SHORT")
        arcpy.management.AddField(output_fc, "AVG_HEADWAY", "FLOAT")

    # helper method that creates rail network
    def create_rail_network(self):

        mrn_in_gdb = self.mrn_in_gdb

        raillink_fc = os.path.join(mrn_in_gdb, "railnet", "railnet_arc")

        arcpy.management.CreateFeatureclass(self.current_gdb, "RAILLINK", "POLYLINE")
        directional_fc = os.path.join(self.current_gdb, "RAILLINK")

        add_fields = [["ANODE", "LONG"], ["BNODE", "LONG"], 
                      ["MILES", "DOUBLE"], ["MODES", "TEXT"]]

        arcpy.management.AddFields(directional_fc, add_fields)

        sfields = ["SHAPE@", "ANODE", "BNODE", "DIRECTIONS", 
                   "MODES1", "MODES2", "MILES"]
        
        ifields = ["SHAPE@", "ANODE", "BNODE", "MODES", "MILES"]
        with arcpy.da.SearchCursor(raillink_fc, sfields) as scursor:
            with arcpy.da.InsertCursor(directional_fc, ifields) as icursor:

                for row in scursor:

                    geom = row[0]
                    anode = row[1]
                    bnode = row[2]
                    dirs = row[3]
                    modes1 = row[4]
                    modes2 = row[5]
                    miles = row[6]

                    icursor.insertRow(
                        [geom, anode, bnode, modes1, miles]
                    )

                    if "C" in modes1:
                        self.c_graph.add_edge(anode, bnode, weight = miles)
                    if "M" in modes1:
                        self.m_graph.add_edge(anode, bnode, weight = miles)

                    if dirs == 2:

                        icursor.insertRow(
                            [geom, bnode, anode, modes2, miles]
                        )

                        if "C" in modes2:
                            self.c_graph.add_edge(bnode, anode, weight = miles)
                        if "M" in modes2:
                            self.m_graph.add_edge(bnode, anode, weight = miles)

    # helper method to reformat feed
    def reformat_gtfs_feed(self, itin_dict):

        reformat_dict = {}

        for tr_line in itin_dict:
            tr_itin = itin_dict[tr_line]
            tr_path = []

            for record in tr_itin:
                
                itin_a = record["ITIN_A"]
                itin_b = record["ITIN_B"]
                dw_code = record["DW_CODE"]

                record_string = f"{itin_a}-{itin_b}-{dw_code}"
                tr_path.append(record_string)

            reformat_dict[tr_line] = tr_path

        return reformat_dict
    
    # helper method that finds representative runs
    def find_rep_runs(self, tod, which_gtfs, rf_dict):

        where_clause = self.tod_dict[tod]["where_clause"]
        maxtime = self.tod_dict[tod]["maxtime"]

        rail_layer = f"{which_gtfs}_layer"

        arcpy.management.SelectLayerByAttribute(
            rail_layer, "NEW_SELECTION", where_clause)
        
        tod_fd = os.path.join(self.current_gdb, f"TOD_{tod}")
        all_tod_fc = os.path.join(tod_fd, f"all_{which_gtfs}_{tod}")

        arcpy.management.CopyFeatures(rail_layer, all_tod_fc)

        # get lines and group by MODE- ROUTE_ID (MR_ID)
        fields = ["MODE", "MR_ID", "TR_LINE"]
        lines_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(all_tod_fc, fields)], 
            columns = fields)
        
        lines_df = lines_df[lines_df.MODE == "C"]

        lines_dict = lines_df.groupby('MR_ID')['TR_LINE'].apply(list).to_dict()

        group = 0
        groups = {}

        # find routes similar enough to be collapsed
        for mr_id in lines_dict:

            mr_lines = lines_dict[mr_id]

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

        with arcpy.da.UpdateCursor(all_tod_fc, ["TR_LINE", "RAIL_GROUP"]) as ucursor:
            for row in ucursor:

                tr_line = row[0]

                if tr_line in groups:
                    row[1] = groups[tr_line]

                else:
                    group += 1
                    row[1] = group

                ucursor.updateRow(row)

        # find representative routes
        # get number of segments 
        num_seg_dict = {k: len(v) for k, v in rf_dict.items()}

        # get line df 
        lines_fields = ["RAIL_GROUP", "TR_LINE", "START"]
        
        lines_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(all_tod_fc, lines_fields)], 
            columns = lines_fields)
        
        lines_df["NUM_SEGS"] = lines_df["TR_LINE"].apply(lambda x: num_seg_dict[x])

        # comes earliest in that time period (adjusts for TOD 1)
        lines_df["START"] = lines_df["START"].apply(lambda x: x + 86400 if x < 21600 else x)

        # calculate headway
        lines_df = lines_df.sort_values(["RAIL_GROUP", "START"])
        lines_df["PREV_START"] = lines_df.groupby("RAIL_GROUP")["START"].shift()
        subtract_df = lines_df[lines_df.PREV_START.notnull()]
        subtract_df["HEADWAY"] = (subtract_df["START"] - subtract_df["PREV_START"]) /60

        headway_dict = subtract_df.groupby("RAIL_GROUP")["HEADWAY"].mean().to_dict()

        # longest, then starts earliest
        lines_df = lines_df.sort_values(["RAIL_GROUP", "NUM_SEGS", "START"], 
                                        ascending = [True, False, True])
        rep_routes = lines_df.groupby("RAIL_GROUP").first()["TR_LINE"].to_list()

        rep_tod_fc = os.path.join(tod_fd, f"rep_{which_gtfs}_{tod}")
        arcpy.management.CopyFeatures(all_tod_fc, rep_tod_fc)

        fields = ["TR_LINE", "RAIL_GROUP", "AVG_HEADWAY"]

        with arcpy.da.UpdateCursor(rep_tod_fc, fields) as ucursor:
            for row in ucursor:

                if row[0] not in rep_routes:
                    ucursor.deleteRow()

                else:
                    bus_group = row[1]
                    
                    if bus_group in headway_dict:
                        row[2] = round(headway_dict[bus_group], 1)
                    else:
                        row[2] = maxtime

                    ucursor.updateRow(row)

    # helper method that finds representative itins
    def find_rep_itins(self, tod, which_rail, itin_dict):

        geom_dict = self.geom_dict

        tod_fd = os.path.join(self.current_gdb, f"TOD_{tod}")
        itin_tod_fc = os.path.join(tod_fd, f"itin_{which_rail}_{tod}")
        
        # prepare feature class
        arcpy.management.CreateFeatureclass(
            tod_fd, f"itin_{which_rail}_{tod}", "POLYLINE", spatial_reference = 26771)
        
        add_fields = [
            ["TR_LINE", "TEXT"], ["IT_ORDER", "SHORT"],
            ["ITIN_A", "LONG"], ["ITIN_B", "LONG"],
            ["LAYOVER", "SHORT"], ["DW_CODE", "SHORT"], 
            ["ZN_FARE", "FLOAT"], ["TRV_TIME", "FLOAT"], 
            ["NOTES", "TEXT"]
        ]

        arcpy.management.AddFields(itin_tod_fc, add_fields)

        # get rep routes 
        rep_tod_fc = os.path.join(tod_fd, f"rep_{which_rail}_{tod}")
        rep_routes = {row[0]: row[1] for row in 
                      arcpy.da.SearchCursor(rep_tod_fc, ["TR_LINE", "MODE"])}
        
        # get rep itins
        fields = ["SHAPE@", "TR_LINE", "IT_ORDER", 
                  "ITIN_A", "ITIN_B", "LAYOVER", "DW_CODE",
                  "ZN_FARE", "TRV_TIME", "NOTES"]
        
        with arcpy.da.InsertCursor(itin_tod_fc, fields) as icursor:
            for tr_line in itin_dict:

                if tr_line not in rep_routes:
                    continue

                itin = itin_dict[tr_line]

                # if action code 2-7 ... just hope it works LOL?
                if tr_line[3:5] == "**":
                    
                    for record in itin:

                        itin_order = record["IT_ORDER"]
                        itin_a = record["ITIN_A"]
                        itin_b = record["ITIN_B"]
                        layover = record["LAYOVER"]
                        dwc = record["DW_CODE"]
                        zfare = record["ZN_FARE"]
                        lst = record["TRV_TIME"]

                else:

                    mode = rep_routes[tr_line]
                    graph = None
                    if mode == "C":
                        graph = self.c_graph
                    if mode == "M":
                        graph = self.m_graph

                    itin_order = 0
                    for i in range(0, len(itin)):

                        record = itin[i]
                        itin_order += 1

                        itin_a = record["ITIN_A"]
                        itin_b = record["ITIN_B"]
                        layover = record["LAYOVER"]
                        dwc = record["DW_CODE"]
                        zfare = record["ZN_FARE"]
                        lst = record["TRV_TIME"]

                        if graph.has_edge(itin_a, itin_b):
                            geom = geom_dict[(itin_a, itin_b)]["GEOM"]
                            notes = None
                        else:
                            geom = None
                            notes = "Itin Gap"
                        
                        row = [geom, tr_line, itin_order, itin_a, itin_b,
                            layover, dwc, zfare, lst, notes]
                        icursor.insertRow(row)

                        if i == len(itin) - 1:
                            continue

                        # check for itinerary gaps
                        next_record = itin[i+1]
                        next_a = next_record["ITIN_A"]

                        if itin_b != next_a:

                            itin_order+= 1
                            row = [None, tr_line, itin_order, itin_b, next_a,
                                # layover, dwc, zfare, lst, notes
                                0, 1, 0, 0, "Itin Gap"] 
                            icursor.insertRow(row)