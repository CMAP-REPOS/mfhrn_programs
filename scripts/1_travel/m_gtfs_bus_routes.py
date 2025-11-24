## m_gtfs_bus_routes.py
## serves the same function as import_gtfs_bus_routes.py
## based on work by npeterson & tkoleary
## Translated + Updated by ccai (2025)

from modules.BHN import BusHighwayNetwork

import os
import sys
import arcpy
import argparse
import math
import time
import csv
import pandas as pd

# if __name__ == "__main__":

#     start_time = time.time()
#     parser = argparse.ArgumentParser()
#     parser.add_argument("gtfs", help = "choose base or current")
#     args = parser.parse_args()

#     BHN = BusHighwayNetwork()

#     BHN.create_base_hwy()
#     # BHN.check_hwy_fcs()
#     BHN.import_gtfs_bus_routes(args.gtfs)

#     end_time = time.time()
#     total_time = round(end_time - start_time)
#     minutes = math.floor(total_time / 60)
#     seconds = total_time % 60

#     print(f"{minutes}m {seconds}s to execute.")

#     print("Done")


headway_dict = {
    20: 600, 21: 600, 22: 600, 23: 600, 0: 600, 1: 600, 2: 600, 3: 600, 4: 600, 5: 600, 
    6: 60, 
    7: 120, 8: 120, 
    9: 60, 
    10: 240, 11: 240, 12: 240, 13: 240, 
    14: 120, 15: 120, 
    16: 120, 17: 120, 
    18: 120, 19: 120}

# helper method to read csv into df
def read_csv_to_df(file_path):
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


# method that imports gtfs lines and segments
# NOTE - NOT COMPLETE!!
def import_gtfs_bus_routes(mhn_in_folder, current_gdb, headway_dict, which_gtfs):

    print(f"Importing {which_gtfs} GTFS lines and segments...")

    import_lines = os.path.join(
        mhn_in_folder, f"import_bus_lines_{which_gtfs}.csv")
    import_segments = os.path.join(
        mhn_in_folder, f"import_bus_segments_{which_gtfs}.csv")
    
    lines_df = read_csv_to_df(import_lines)
    segments_df = read_csv_to_df(import_segments)

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

    hwylink_fc = os.path.join(current_gdb, "hwynet", "hwynet_arc")
    bus_lines_fc = os.path.join(current_gdb, "hwynet", f"bus_{which_gtfs}")
    bus_itin_table = os.path.join(current_gdb, f"bus_{which_gtfs}_itin")

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

            row[lf_dict["HEADWAY"]] = headway_dict[starthour]

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