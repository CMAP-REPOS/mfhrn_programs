## BHN.py

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
from .ETN import EmmeTravelNetwork

class BusHighwayNetwork(HighwayNetwork):

    def __init__(self):
        super().__init__()

        # base + current definition
        self.bus_base = 2019
        self.bus_current = 2024

        # scenario dict
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
        
        self.ETN = EmmeTravelNetwork()

        self.geom_dict = self.build_geom_dict()

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

        # copy fcs
        self.current_gdb = cr_gdb

        self.copy_bus_fcs("bus_base")
        self.copy_bus_fcs("bus_current")
        self.copy_bus_fcs("bus_future")

        # copy park n ride table
        input_table = os.path.join(mhn_in_gdb, "parknride")
        arcpy.management.CreateTable(cr_gdb, "parknride", template = input_table)
        output_table = os.path.join(cr_gdb, "parknride")

        fields = [f.name for f in arcpy.ListFields(input_table) if (f.name != "OBJECTID")]

        with arcpy.da.SearchCursor(input_table, fields) as scursor:
            with arcpy.da.InsertCursor(output_table, fields) as icursor:

                for row in scursor:
                    icursor.insertRow(row)

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

        for tod in [1, 2, 3, 4]:

            print(f"Collapsing routes for TOD {tod}...")

            arcpy.management.CreateFeatureDataset(cr_gdb, f"TOD_{tod}", spatial_reference = 26771)

            # collapse gtfs routes
            self.find_rep_runs(tod= tod, which_gtfs="base", rf_dict = base_rf_dict)
            self.find_rep_runs(tod= tod, which_gtfs="current", rf_dict = current_rf_dict)

            self.find_rep_itins(tod= tod, which_bus = "base", itin_dict = base_itin_dict)
            self.find_rep_itins(tod= tod, which_bus = "current", itin_dict = current_itin_dict)

            arcpy.management.SelectLayerByAttribute(
                "future_layer", "NEW_SELECTION", f"TOD = '0' Or TOD LIKE '%{tod}%'"
            )
            rep_future_fc = os.path.join(cr_gdb, f"TOD_{tod}", f"rep_future_{tod}")
            arcpy.management.CopyFeatures("future_layer", rep_future_fc)

            self.find_rep_itins(tod= tod, which_bus = "future", itin_dict = future_itin_dict)

        print("TOD routes collapsed.\n")

    # method that creates bus layers for each scenario
    def create_bus_layers(self):

        print("Creating bus layers...")

        scenario_dict = self.scenario_dict
        bn_out_folder = self.bn_out_folder

        mhn_all_gdb = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        # first, make bus gdbs
        for scen in scenario_dict:

            year = scenario_dict[scen]

            # copy the links over
            scen_gdb_name = f"SCENARIO_{scen}.gdb"
            scen_gdb = os.path.join(bn_out_folder, scen_gdb_name)

            if arcpy.Exists(scen_gdb):
                arcpy.management.Delete(scen_gdb)

            arcpy.management.CreateFileGDB(bn_out_folder, scen_gdb_name)

            input_links = os.path.join(mhn_all_gdb, "hwylinks_all", f"HWYLINK_{year}")
            arcpy.management.MakeFeatureLayer(input_links, "input_links_layer", "NEW_BASELINK = '1'")
            output_links = os.path.join(scen_gdb, f"HWYLINK_{year}")
            arcpy.management.CopyFeatures("input_links_layer", output_links)

            arcpy.management.Delete("input_links_layer")

            # for each tod
            for tod in [1, 2, 3, 4]:

                print(f"Creating bus layers for scenario {scen} TOD {tod}...")

                arcpy.management.CreateFeatureDataset(scen_gdb, f"TOD_{tod}", spatial_reference = 26771)

                # find highway links
                G = self.create_tod_hwy_networks(scen, tod)

                # find bus networks
                reroute_dict = self.create_tod_bus_runs(scen, tod)
                self.create_tod_bus_itins(scen, tod, reroute_dict, G)

        # Use TOD 3 highways for AM transit

    # HELPER METHODS ------------------------------------------------------------------------------

    # helper method that builds node geometry dict


    # helper method that builds link geometry dict
    def build_geom_dict(self):

        print("Building geometry dictionary of all highway links...")

        mhn_in_gdb = self.mhn_in_gdb

        link_fields = ["ANODE", "BNODE", "ABB", "DIRECTIONS"]

        hwylink_fc = os.path.join(mhn_in_gdb, "hwynet", "hwynet_arc")
        hwylink_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwylink_fc, link_fields)], 
            columns = link_fields)

        hwylink_rev_df = pd.merge(hwylink_df, hwylink_df.copy(), 
                                  left_on = ["ANODE", "BNODE"], right_on = ["BNODE", "ANODE"])
        hwylink_rev_set = set(hwylink_rev_df.ABB_x.to_list())

        fields = ["SHAPE@", "ANODE", "BNODE", "ABB", "MILES"]

        geom_dict = {}

        with arcpy.da.SearchCursor(hwylink_fc, fields) as scursor:
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

                geom_dict[(anode, bnode)] = {"ABB": abb, "GEOM": polyline, "MILES": miles}
                if abb not in hwylink_rev_set:
                    geom_dict[(bnode, anode)] = {"ABB": abb, "GEOM": polyline, "MILES": miles}

        print("Geometry dictionary built.\n")

        return geom_dict

    # helper method to copy fcs to collapse routes
    def copy_bus_fcs(self, fc_name):

        # copy current fc over
        input_fc = os.path.join(self.mhn_in_gdb, "hwynet", fc_name) 
        arcpy.management.CreateFeatureclass(self.current_gdb, fc_name, template = input_fc, spatial_reference = 26771)
        output_fc = os.path.join(self.current_gdb, fc_name)

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
            field = "MR_ID",
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
        
        tod_fd = os.path.join(self.current_gdb, f"TOD_{tod}")
        all_tod_fc = os.path.join(tod_fd, f"all_{which_gtfs}_{tod}")

        arcpy.management.CopyFeatures(bus_layer, all_tod_fc)

        # get lines and group by MODE- ROUTE_ID (MR_ID)
        lines_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(all_tod_fc, ["MR_ID", "TRANSIT_LINE"])], 
            columns = ["MR_ID", "TRANSIT_LINE"])
        
        lines_dict = lines_df.groupby('MR_ID')['TRANSIT_LINE'].apply(list).to_dict()

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
        rep_routes = lines_df.groupby("BUS_GROUP").first()["TRANSIT_LINE"].to_list()

        rep_tod_fc = os.path.join(tod_fd, f"rep_{which_gtfs}_{tod}")
        arcpy.management.CopyFeatures(all_tod_fc, rep_tod_fc)

        fields = ["TRANSIT_LINE", "BUS_GROUP", "AVG_HEADWAY"]

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

    # helper method that finds representative itineraries
    def find_rep_itins(self, tod, which_bus, itin_dict):

        geom_dict = self.geom_dict

        tod_fd = os.path.join(self.current_gdb, f"TOD_{tod}")
        itin_tod_fc = os.path.join(tod_fd, f"itin_{which_bus}_{tod}")
        
        # prepare feature class
        arcpy.management.CreateFeatureclass(
            tod_fd, f"itin_{which_bus}_{tod}", "POLYLINE")
        
        add_fields = [
            ["TRANSIT_LINE", "TEXT"], ["ITIN_ORDER", "SHORT"],
            ["ITIN_A", "LONG"], ["ITIN_B", "LONG"],
            ["ABB", "TEXT"], ["DWELL_CODE", "TEXT"], 
            ["LINE_SERV_TIME", "FLOAT"], ["TTF", "FLOAT"],
            ["NOTES", "TEXT"]
        ]

        arcpy.management.AddFields(itin_tod_fc, add_fields)

        # get rep routes 
        rep_tod_fc = os.path.join(tod_fd, f"rep_{which_bus}_{tod}")
        rep_routes = [row[0] for row in arcpy.da.SearchCursor(rep_tod_fc, ["TRANSIT_LINE"])]
        
        # get rep itins
        fields = ["SHAPE@", "TRANSIT_LINE", "ITIN_ORDER", 
                  "ITIN_A", "ITIN_B", "ABB", 
                  "DWELL_CODE", "LINE_SERV_TIME", "TTF", "NOTES"]
        with arcpy.da.InsertCursor(itin_tod_fc, fields) as icursor:
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

                    if (itin_a, itin_b) in geom_dict:
                        geom = geom_dict[(itin_a, itin_b)]["GEOM"]
                        notes = None
                    else:
                        geom = None
                        notes = "Itin Gap"
                    
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
                        icursor.insertRow(row)





    # helper method which makes tod highway networks 
    def create_tod_hwy_networks(self, scen, tod):

        bn_out_folder = self.bn_out_folder
        geom_dict = self.geom_dict

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

        ETN = self.ETN
        hwylink_records = ETN.create_directional_records(hwylink_fc)
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

        hwylink_tod = hwylink_df[(hwylink_df.ampm.isin(ampm_links)) &
                                 (hwylink_df.type != "6")] # no centroids allowed
        hwylink_dict = hwylink_tod.set_index(["inode", "jnode"]).to_dict("index")

        G = nx.DiGraph()

        with arcpy.da.InsertCursor(hwylink_tod_fc, fields) as icursor:
            for link in hwylink_dict:

                anode = link[0]
                bnode = link[1]

                abb = geom_dict[(anode, bnode)]["ABB"]
                geom = geom_dict[(anode, bnode)]["GEOM"]

                # miles
                miles = hwylink_dict[link]["miles"]

                G.add_edge(anode, bnode, weight = miles)

                # calculate # of lanes
                lanes = hwylink_dict[link]["lanes"]
                parklanes = hwylink_dict[link]["parklanes"]
                parkres = hwylink_dict[link]["parkres"]

                if str(hwy_tod) in parkres:
                    lanes += parklanes
                    parklanes = 0

                # vdf
                type = hwylink_dict[link]["type"]

                row = [geom, anode, bnode, abb, miles, lanes, type]
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

        in_fc = os.path.join(cr_gdb, f"TOD_{tod}", f"rep_{which_gtfs}_{tod}")
        # REP GTFS FC
        rep_gtfs_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"rep_{which_gtfs}_{tod}") 
        arcpy.management.CopyFeatures(in_fc, rep_gtfs_fc)

        # REP FUTURE FC
        in_fc = os.path.join(cr_gdb, f"TOD_{tod}", f"rep_future_{tod}")
        where_clause = f"SCENARIO LIKE '%{scen}%'"
        arcpy.management.MakeFeatureLayer(in_fc, "in_layer", where_clause)
        
        rep_future_fc = os.path.join(scen_gdb, f"TOD_{tod}", f"rep_future_{tod}")

        arcpy.management.CopyFeatures("in_layer", rep_future_fc)
        arcpy.management.Delete("in_layer")

        # process project coding
        tod_fd = os.path.join(scen_gdb, f"TOD_{tod}")
        arcpy.management.CreateFeatureclass(tod_fd, f"rep_scen_{tod}", "POLYLINE")
        rep_scen_fc = os.path.join(tod_fd, f"rep_scen_{tod}")

        add_fields = [
            ["TRANSIT_LINE", "TEXT"], ["DESCRIPTION", "TEXT"],
            ["MODE", "TEXT"], ["VEHICLE_TYPE", "TEXT"], ["HEADWAY", "FLOAT"], 
            ["SPEED", "SHORT"], ["MR_ID", "TEXT"], ["NOTES", "TEXT"]
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

        replace_mrids = []
        for mrid_list in replace_dict.values():

            replace_mrids += mrid_list

        replace_mrids = set(replace_mrids)

        # find rerouted runs
        reroute_df = rep_future_df[(rep_future_df.REROUTE.str.contains("-"))]
        inv_reroute_dict = reroute_df.set_index("TRANSIT_LINE")["REROUTE"].to_dict()
        inv_reroute_dict = {k: v.split(":") for k, v in inv_reroute_dict.items()}

        reroute_dict = {}

        for tr_line in inv_reroute_dict:
            for mr_id in inv_reroute_dict[tr_line]:

                if mr_id not in reroute_dict:
                    reroute_dict[mr_id] = []
                
                reroute_dict[mr_id].append(tr_line)
                
        # transfer existing runs over
        sfields = ["SHAPE@", "TRANSIT_LINE", "DESCRIPTION", "MODE",
                   "VEHICLE_TYPE", "AVG_HEADWAY", "SPEED", "MR_ID"]
        ifields = ["SHAPE@", "TRANSIT_LINE", "DESCRIPTION", "MODE",
                   "VEHICLE_TYPE", "HEADWAY", "SPEED", "MR_ID"]
        
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
                    mr_id = row[7]

                    # if replaced, don't transfer
                    if mr_id in replace_mrids:
                        replace_hdwys.append((mr_id, hdwy))
                        continue

                    if mode not in mode_hdwys:
                        mode_hdwys[mode] = []

                    mode_hdwys[mode].append(hdwy)

                    icursor.insertRow(
                        [row[0], tr_line, desc, mode, veh_type, hdwy, speed, mr_id]
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
                        
                        replace_mrids = replace_dict[tr_line]
                        replaced_hdwy = self.find_replaced_headway(
                            replace_mrids, replace_hdwys)
                        
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

        return reroute_dict

    # helper method that finds the replaced headway
    def find_replaced_headway(self, replace_mrids, replace_hdwys):

        headways = []

        for pair in replace_hdwys:

            if pair[0] in replace_mrids:
                headways.append(pair[1])

        if len(headways) > 0:
            return min(headways)
        else:
            return 0

    # helper method which makes tod bus itineraries
    def create_tod_bus_itins(self, scen, tod, reroute_dict, G):

        bn_out_folder = self.bn_out_folder

        cr_gdb = os.path.join(bn_out_folder, f"collapsed_routes.gdb")
        scen_gdb = os.path.join(bn_out_folder, f"SCENARIO_{scen}.gdb")

        which_gtfs = "base"
        if scen > 1:
            which_gtfs = "current"

        # get itins as dicts
        tod_fd = os.path.join(cr_gdb, f"TOD_{tod}")
        itin_gtfs_fc = os.path.join(tod_fd, f"itin_{which_gtfs}_{tod}")
        itin_future_fc = os.path.join(tod_fd, f"itin_future_{tod}")

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
        rep_scen_fc = os.path.join(tod_fd, f"rep_scen_{tod}")

        transit_lines = {row[0]: row[1] for row in arcpy.da.SearchCursor(rep_scen_fc, ["TRANSIT_LINE", "MR_ID"])}

        # create itin 
        arcpy.management.CreateFeatureclass(tod_fd, f"rep_itin_{tod}", "POLYLINE", itin_gtfs_fc)
        rep_itin_fc = os.path.join(tod_fd, f"rep_itin_{tod}")

        fields = ["SHAPE@", "TRANSIT_LINE", "ITIN_ORDER", 
                  "ITIN_A", "ITIN_B", "ABB", 
                  "DWELL_CODE", "LINE_SERV_TIME", "TTF", "NOTES"]
        
        with arcpy.da.InsertCursor(rep_itin_fc, fields) as icursor:

            for transit_line in transit_lines:

                mr_id = transit_lines[transit_line]
                line_itin = None

                if transit_line in itin_gtfs_dict:
                    line_itin = itin_gtfs_dict[transit_line]

                elif transit_line in itin_future_dict:
                    line_itin = itin_future_dict[transit_line]

                final_itin = self.make_final_line_itin(transit_line, line_itin,
                                           mr_id, reroute_dict, itin_future_dict, G)

                for record in final_itin:

                    itin_order = record["ITIN_ORDER"]
                    itin_a = record["ITIN_A"]
                    itin_b = record["ITIN_B"]
                    abb = record["ABB"]

                    dwc = record["DWELL_CODE"]
                    lst = record["LINE_SERV_TIME"]
                    ttf = record["TTF"]
                    notes = record["NOTES"]

                    geom = self.geom_dict[(itin_a, itin_b)]["GEOM"]

                    icursor.insertRow(
                        [geom, transit_line, itin_order, 
                         itin_a, itin_b, abb, 
                         dwc, lst, ttf, notes]
                    )

    # helper method that makes final line itin
    def make_final_line_itin(self, transit_line, line_itin, mr_id, 
                              reroute_dict, itin_future_dict, G):

        # first- reroute
        anodes = [record["ITIN_A"] for record in line_itin]
        bnodes = [record["ITIN_B"] for record in line_itin]

        # attempt to reroute
        if mr_id in reroute_dict:

            reroute = 0
            reroute_lines = reroute_dict[mr_id]

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

            # if reroute == 0:
            #     print("Can't reroute " + transit_line)

        # check if first and last nodes are in graph
        if anodes[0] not in G:
            return []
        if bnodes[-1] not in G:
            return []

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
                        record["ABB"] = self.geom_dict[(path[j], path[j+1])]["ABB"]

                        # assume stop
                        dwcj = 0

                        # if mode is E or Q - change to non-stop
                        if transit_line[0] in ["e", "q"]:
                            dwcj = 1

                        if j == len(path) - 2:
                            dwcj = dwc

                        record["DWELL_CODE"] = dwcj

                        miles = self.geom_dict[(path[j], path[j+1])]["MILES"]
                        record["LINE_SERV_TIME"] = max(miles * (60/ self.default_speed), 0.1)

                        record["TTF"] = ttf
                        record["NOTES"] = "Shortest Path"

                        final_itin.append(record)

                else:
                    return []
        
        return final_itin
    
    # find nearest node
