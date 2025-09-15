## GeneralNetwork.py
## provides functions for working with networks

## Author: ccai (2025)

import arcpy
import os

def resolve_geometry(in_path, out_path, network):

    in_fd = os.path.join(in_path, f"{network}net")
    in_link_fc = os.path.join(in_fd, f"{network}net_arc")

    out_fd = os.path.join(out_path, f"{network}net")
    out_node_fc = os.path.join(out_fd, f"{network}net_node")
    out_link_fc = os.path.join(out_fd, f"{network}net_arc")

    # find deleted links and drop 
    in_abbs = set([row[0] for row in arcpy.da.SearchCursor(in_link_fc, "ABB")])
    out_abbs = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "ABB")])

    del_abbs = in_abbs - out_abbs

    print(len(del_abbs))
    
    remaining_anodes = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "ANODE")])
    remaining_bnodes = set([row[0] for row in arcpy.da.SearchCursor(out_link_fc, "BNODE")])
    remaining_nodes = remaining_anodes | remaining_bnodes

    with arcpy.da.UpdateCursor(out_node_fc, "NODE") as ucursor:
        for row in ucursor:

            if row[0] not in remaining_nodes:
                ucursor.deleteRow()

    print(len(remaining_nodes))