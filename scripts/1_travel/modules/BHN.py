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

        self.headway_dict = {
            20: 600, 21: 600, 22: 600, 23: 600, 0: 600, 1: 600, 2: 600, 3: 600, 4: 600, 5: 600, 
            6: 60, 
            7: 120, 8: 120, 
            9: 60, 
            10: 240, 11: 240, 12: 240, 13: 240, 
            14: 120, 15: 120, 
            16: 120, 17: 120, 
            18: 120, 19: 120}
        
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

        # get dfs + dict for bus base itin
        base_itin = os.path.join(mhn_in_gdb, "bus_base_itin")
        base_itin_fields = [f.name for f in arcpy.ListFields(base_itin) if (f.name != "OBJECTID")]
        base_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(base_itin, base_itin_fields)],
            columns = base_itin_fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        base_itin_dict = {k: v.to_dict(orient='records') for k, v in base_itin_df.groupby("TRANSIT_LINE")}
        base_rf_dict = self.reformat_gtfs_feed(base_itin_dict)

        # get dfs + dicts for bus current itin
        current_itin = os.path.join(mhn_in_gdb, "bus_current_itin")
        current_itin_fields = [f.name for f in arcpy.ListFields(current_itin) if (f.name != "OBJECTID")]
        current_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(current_itin, current_itin_fields)],
            columns = current_itin_fields
        ).sort_values(["TRANSIT_LINE", "ITIN_ORDER"])

        current_itin_dict = {k: v.to_dict(orient='records') for k, v in current_itin_df.groupby("TRANSIT_LINE")}
        current_rf_dict = self.reformat_gtfs_feed(current_itin_dict)

        # get dfs + dicts for bus future itin
        future_itin = os.path.join(mhn_in_gdb, "bus_future_itin")
        future_itin_fields = [f.name for f in arcpy.ListFields(future_itin) if (f.name != "OBJECTID")]
        future_itin_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(future_itin, future_itin_fields)],
            columns = future_itin_fields
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
                self.create_tod_bus_itins(scen, tod, reroute_dict)

        # Use TOD 3 highways for AM transit

    # method that imports gtfs lines and segments
    # NOTE - NOT COMPLETE!!
    def import_gtfs_bus_routes(self, which_gtfs):

        print(f"Importing {which_gtfs} GTFS lines and segments...")

        import_lines = os.path.join(
            self.mhn_in_folder, f"import_bus_lines_{which_gtfs}.csv")
        import_segments = os.path.join(
            self.mhn_in_folder, f"import_bus_segments_{which_gtfs}.csv")
        
        lines_df = self.read_csv_to_df(import_lines)
        segments_df = self.read_csv_to_df(import_segments)

        line_ids = set(lines_df.transit_line.to_list())
        segment_ids = set(lines_df.transit_line.to_list())

        if (len(line_ids - segment_ids) > 0):
            sys.exit("Lines exist without corresponding segments.")
        if (len(segment_ids - line_ids) > 0):
            sys.exit("Segments exist without corresponding lines.")

        # lines - transit line must be unique
        pk_lines = lines_df.transit_line.value_counts()

        if (pk_lines.max() > 1):
            sys.exit("Lines cannot be uniquely identified by transit_line.")

        # segments - transit_line/itin_order combinations must be unique
        pk_segments_df = segments_df.groupby(["transit_line", "itin_order"]).size().reset_index()
        pk_segments_df = pk_segments_df.rename(columns = {0: "group_size"})
        
        if (pk_segments_df["group_size"].max() > 1):
            sys.exit("Segments cannot be uniquely identified by transit_line + itin_order.")

        hwylink_fc = os.path.join(self.current_gdb, "hwynet", "hwynet_arc")
        bus_lines_fc = os.path.join(self.current_gdb, "hwynet", f"bus_{which_gtfs}")
        bus_itin_table = os.path.join(self.current_gdb, f"bus_{which_gtfs}_itin")

        # delete all rows
        arcpy.management.TruncateTable(bus_lines_fc)
        arcpy.management.TruncateTable(bus_itin_table)

        # import transit lines
        lines_records = lines_df.sort_values("transit_line").to_dict("records")

        not_fields = ["Shape", "Shape_Length", "OBJECTID"]
        line_fields = ["SHAPE@"] + [f.name for f in arcpy.ListFields(bus_lines_fc) if (f.name not in not_fields)]
        lf_dict = {field: index for index, field in enumerate(line_fields)}
        
        with arcpy.da.InsertCursor(bus_lines_fc, line_fields) as icursor:
            for record in lines_records:

                row = [None] * len(line_fields)
                row[lf_dict["TRANSIT_LINE"]] = record["transit_line"]
                
                # put together description
                longname = record["longname"]
                route_id = longname.split()[0].split("-")[0]
                desc = route_id + " " + longname.split(maxsplit = 2)[2]
                desc = desc[0:50]

                row[lf_dict["DESCRIPTION"]] = desc
                row[lf_dict["MODE"]] = record["mode"]
                row[lf_dict["VEHICLE_TYPE"]] = record["vehicle_type"]

                row[lf_dict["SPEED"]] = 15
                row[lf_dict["ROUTE_ID"]] = route_id
                row[lf_dict["DIRECTION"]] = record["direction"]

                row[lf_dict["START"]] = int(float(record["start"])) * 60
                starthour = int(float(record["starthour"]))
                row[lf_dict["STARTHOUR"]] = starthour

                row[lf_dict["HEADWAY"]] = self.headway_dict[starthour]

                row[lf_dict["FEEDLINE"]] = record["feedline"]

                icursor.insertRow(row)

        # import transit segments
        segments_df["itin_order"] = segments_df["itin_order"].astype(int)
        segments_sorted_df = segments_df.sort_values(["transit_line", "itin_order"])
        segments_records = segments_sorted_df.to_dict("records")

        # make hwylink dict
        hwylink_dict = {}
        with arcpy.da.SearchCursor(hwylink_fc, ["ANODE", "BNODE", "ABB"]) as scursor:
            for row in scursor:

                anode = row[0]
                bnode = row[1]
                abb = row[2]

                hwylink_dict[(anode, bnode)] = abb
                hwylink_dict[(bnode, anode)] = abb

        segments_fields = [f.name for f in arcpy.ListFields(bus_itin_table) if (f.name != "OBJECTID")]
        sf_dict = {field: index for index, field in enumerate(segments_fields)}

        with arcpy.da.InsertCursor(bus_itin_table, segments_fields) as icursor:
            for record in segments_records:

                row = [None] * len(segments_fields)

                row[sf_dict["TRANSIT_LINE"]] = record["transit_line"]
                row[sf_dict["ITIN_ORDER"]] = record["itin_order"]

                itin_a = int(record["itin_a"])
                row[sf_dict["ITIN_A"]] = itin_a
                itin_b = int(record["itin_b"])
                row[sf_dict["ITIN_B"]] = itin_b

                if (itin_a, itin_b) in hwylink_dict:
                    abb = hwylink_dict[(itin_a, itin_b)]
                    row[sf_dict["ABB"]] = abb

                icursor.insertRow(row)

        print(f"{which_gtfs} GTFS lines and segments imported.")

    # method that resolve the highway geometry
    # NOTE - NOT COMPLETE!!
    def resolve_hwy_geometry(self):

        # we can... we can fix this later... 

        in_path = os.path.join(self.mhn_in_folder, "MHN.gdb")
        out_path = os.path.join(self.mhn_out_folder, f"MHN_{self.base_year}.gdb")

        in_fd = os.path.join(in_path, "hwynet")
        in_link_fc = os.path.join(in_fd, f"hwynet_arc")

        out_fd = os.path.join(out_path, f"hwynet")
        out_node_fc = os.path.join(out_fd, f"hwynet_node")
        out_link_fc = os.path.join(out_fd, f"hwynet_arc")

        # find deleted links and drop 
        in_abbs = set([row[0] for row in arcpy.da.SearchCursor(in_link_fc, "ABB")])
        out_abbs = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "ABB")])
        
        del_abbs = in_abbs - out_abbs

        remaining_anodes = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "ANODE")])
        remaining_bnodes = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "BNODE")])
        remaining_nodes = remaining_anodes | remaining_bnodes
        
        # remove nodes which do not exist anymore
        with arcpy.da.UpdateCursor(out_node_fc, "NODE") as ucursor:
            for row in ucursor:

                if row[0] not in remaining_nodes:
                    ucursor.deleteRow()

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

        fields = ["SHAPE@", "ANODE", "BNODE", "ABB"]

        geom_dict = {}

        with arcpy.da.SearchCursor(hwylink_fc, fields) as scursor:
            for row in scursor:

                geom = row[0]
                anode = row[1]
                bnode = row[2]
                abb = row[3]

                multi_array = arcpy.Array()
                for part in geom:
                    part_array = arcpy.Array([point for point in part])
                    multi_array.append(part_array)
                    
                polyline = arcpy.Polyline(multi_array, spatial_reference = 26771)

                geom_dict[(anode, bnode)] = {"ABB": abb, "GEOM": polyline}
                if abb not in hwylink_rev_set:
                    geom_dict[(bnode, anode)] = {"ABB": abb, "GEOM": polyline}

        print("Geometry dictionary built.\n")

        return geom_dict

    # helper method to read csv into df
    def read_csv_to_df(self, file_path):
        '''
        Function that reads Nick's GTFS tables without error -- 
        (usually reads the LINESTRING as separate columns because of the commas)
        parameter: 
            - file_path - path to the csv file (string)
        '''
        prepped_line_info = []
        with open(file_path, 'r', encoding = "utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            body_rows = [row for row in reader]
            for row in body_rows:
                index_linestring = next((i for i, item in enumerate(row) if 'LINESTRING' in item), None)
                linestring_content = row[index_linestring:]
                linestring_element = ','.join(linestring_content)
                row = row[:index_linestring]
                row.append(linestring_element)
                prepped_line_info.append(row)
        table = pd.DataFrame(prepped_line_info, columns=header)

        return table
    
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
            ["ABB", "TEXT"], ["LAYOVER", "SHORT"],
            ["DWELL_CODE", "TEXT"], ["ZONE_FARE", "SHORT"],
            ["LINE_SERV_TIME", "FLOAT"], ["TTF", "FLOAT"],
            ["NOTES", "TEXT"]
        ]

        arcpy.management.AddFields(itin_tod_fc, add_fields)

        # get rep routes 
        rep_tod_fc = os.path.join(tod_fd, f"rep_{which_bus}_{tod}")
        rep_routes = [row[0] for row in arcpy.da.SearchCursor(rep_tod_fc, ["TRANSIT_LINE"])]
        
        # get rep itins
        fields = ["SHAPE@", "TRANSIT_LINE", "ITIN_ORDER", 
                  "ITIN_A", "ITIN_B", "ABB", "LAYOVER", "DWELL_CODE",
                  "ZONE_FARE", "LINE_SERV_TIME", "TTF", "NOTES"]
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
                    layover = record["LAYOVER"]
                    dwc = record["DWELL_CODE"]
                    zfare = record["ZONE_FARE"]
                    lst = record["LINE_SERV_TIME"]
                    ttf = record["TTF"]

                    if (itin_a, itin_b) in geom_dict:
                        geom = geom_dict[(itin_a, itin_b)]["GEOM"]
                        notes = None
                    else:
                        geom = None
                        notes = "Itin Gap"
                    
                    row = [geom, tr_line, itin_order, itin_a, itin_b,
                           abb, layover, dwc, zfare, lst, ttf, notes]
                    icursor.insertRow(row)

                    if i == len(itin) - 1:
                        continue

                    # check for itinerary gaps
                    next_record = itin[i+1]
                    next_a = next_record["ITIN_A"]

                    if itin_b != next_a:

                        itin_order+= 1
                        row = [None, tr_line, itin_order, itin_b, next_a,
                               # abb, layover, dwc, zfare, lst, ttf, notes
                               None, 0, 1, 0, 0, 1, "Itin Gap"] 
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

        in_fc = os.path.join(cr_gdb, f"TOD_{tod}", f"rep_future_{tod}")
        where_clause = f"SCENARIO LIKE '%{scen}%'"
        arcpy.management.MakeFeatureLayer(in_fc, "in_layer", where_clause)

        # REP FUTURE FC
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

    # helper method which makes tod bus itineraries
    def create_tod_bus_itins(self, scen, tod, reroute_dict):

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
                  "ITIN_A", "ITIN_B"]
        
        print(reroute_dict)
        
        with arcpy.da.InsertCursor(rep_itin_fc, fields) as icursor:

            for transit_line in transit_lines:

                mr_id = transit_lines[transit_line]
                line_itin = None

                if transit_line in itin_gtfs_dict:
                    line_itin = itin_gtfs_dict[transit_line]

                elif transit_line in itin_future_dict:
                    line_itin = itin_future_dict[transit_line]

                for record in line_itin:

                    itin_order = record["ITIN_ORDER"]
                    itin_a = record["ITIN_A"]
                    itin_b = record["ITIN_B"]

                    geom = None
                    if (itin_a, itin_b) in self.geom_dict:
                        geom = self.geom_dict[(itin_a, itin_b)]["GEOM"]

                    icursor.insertRow(
                        [geom, transit_line, itin_order, itin_a, itin_b]
                    )

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
        
    # helper method that makes usable line itin
    def make_usable_line_itin(self, line_itin, mr_id):

        # first- reroute
        anodes = [record["ITIN_A"] for record in line_itin]
        bnodes = [record["ITIN_B"] for record in line_itin]

