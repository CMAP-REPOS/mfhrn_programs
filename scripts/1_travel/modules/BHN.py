## BHN.py

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import csv
import arcpy
import pandas as pd

pd.options.mode.chained_assignment = None

from .HN import HighwayNetwork

class BusHighwayNetwork(HighwayNetwork):

    def __init__(self):
        super().__init__()

        self.bn_out_folder = os.path.join(self.mhn_out_folder, "bus_network")

        # how similar bus runs have to be to be collapsed
        self.threshold = 0.85

        self.tod_dict = {
            1: {"description": "6 PM - 6 AM",
                "where_clause": "STARTHOUR >= 18 OR STARTHOUR < 6",
                "maxtime": 720},
            2: {"description": "6 AM - 9 AM",
                "where_clause": "STARTHOUR >= 6 AND STARTHOUR < 9",
                "maxtime": 180},
            3: {"description": "9 AM - 4 PM",
                "where_clause": "STARTHOUR >= 9 AND STARTHOUR < 16", 
                "maxtime": 420},
            4: {"description": "4 PM - 6 PM",
                "where_clause": "STARTHOUR >= 16 AND STARTHOUR < 18",
                "maxtime": 120}
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

        # create TOD feature datasets
        arcpy.management.CreateFeatureDataset(cr_gdb, "TOD_1", spatial_reference = 26771)
        arcpy.management.CreateFeatureDataset(cr_gdb, "TOD_2", spatial_reference = 26771)
        arcpy.management.CreateFeatureDataset(cr_gdb, "TOD_3", spatial_reference = 26771)
        arcpy.management.CreateFeatureDataset(cr_gdb, "TOD_4", spatial_reference = 26771)

        # make feature layers
        base_fc = os.path.join(cr_gdb, "bus_base")
        arcpy.management.MakeFeatureLayer(base_fc, "base_layer")
        current_fc = os.path.join(cr_gdb, "bus_current")
        arcpy.management.MakeFeatureLayer(current_fc, "current_layer")
        future_fc = os.path.join(cr_gdb, "bus_future")
        arcpy.management.MakeFeatureLayer(future_fc, "future_layer")

        # make geom dict
        geom_dict = self.build_geom_dict()

        # TOD 1

        # collapse gtfs routes
        self.find_rep_runs(tod= 1, which_gtfs="base", rf_dict = base_rf_dict)
        self.find_rep_runs(tod= 1, which_gtfs="current", rf_dict = current_rf_dict)

        self.find_rep_itins(tod= 1, which_bus = "base", 
                            itin_dict = base_itin_dict, geom_dict = geom_dict)
        self.find_rep_itins(tod= 1, which_bus = "current", 
                            itin_dict = current_itin_dict, geom_dict = geom_dict)

        # arcpy.management.SelectLayerByAttribute(
        #     "future_layer", "NEW_SELECTION", "TOD = '0' Or TOD LIKE '%1%'"
        # )

        # future_fc_1 = os.path.join(tod_1_fd, "bus_future_1")
        # arcpy.management.CopyFeatures("future_layer", future_fc_1)

        print("TOD routes collapsed.\n")

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

    # helper method that builds geometry dict
    def build_geom_dict(self):

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

                geom_dict[(anode, bnode)] = polyline
                if abb not in hwylink_rev_set:
                    geom_dict[(bnode, anode)] = polyline

        return geom_dict
    
    # helper method that finds representative itineraries
    def find_rep_itins(self, tod, which_bus, itin_dict, geom_dict):

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
        rep_routes = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(rep_tod_fc, ["TRANSIT_LINE"])], 
            columns = ["TRANSIT_LINE"]).TRANSIT_LINE.to_list()
        
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
                        geom = geom_dict[(itin_a, itin_b)]
                        notes = None
                    else:
                        geom = None
                        notes = "Itin Gap"
                    
                    row = [geom, tr_line, itin_order, itin_a, itin_b,
                           abb, layover, dwc, zfare, lst, ttf, notes]
                    icursor.insertRow(row)

                    if i == len(itin) - 1:
                        continue

                    next_record = itin[i+1]
                    next_a = next_record["ITIN_A"]

                    if itin_b != next_a:

                        itin_order+= 1
                        row = [None, tr_line, itin_order, itin_b, next_a,
                               None, None, None, None, None, None, "Itin Gap"]
                        icursor.insertRow(row)