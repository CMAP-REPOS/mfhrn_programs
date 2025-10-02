## BHN.py

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

from .HN import HighwayNetwork

class BusHighwayNetwork(HighwayNetwork):

    def __init__(self):
        super().__init__()

    def resolve_hwy_geometry(self):

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