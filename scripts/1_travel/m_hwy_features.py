## m_hwy_features.py
## a translation of incorporate_edits.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.BHN import BusHighwayNetwork

import os
import arcpy
import argparse
import math
import time

# if __name__ == "__main__":

#     start_time = time.time()
#     parser = argparse.ArgumentParser()
#     parser.add_argument("-f", "--final", help="finalize highway project coding",
#                         action="store_true")
#     args = parser.parse_args()

#     BHN = BusHighwayNetwork()

#     # have to change manually
#     BHN.current_gdb = os.path.join(BHN.mhn_out_folder, f"MHN_{BHN.base_year}.gdb")
#     BHN.built_gdbs.append(BHN.current_gdb)

#     if not args.final:
#         BHN.resolve_hwy_geometry()
#         BHN.check_hwy_fcs()
#     else:
#         BHN.finalize_hwy_data()

#     end_time = time.time()
#     total_time = round(end_time - start_time)
#     minutes = math.floor(total_time / 60)
#     seconds = total_time % 60

#     print(f"{minutes}m {seconds}s to execute.")

#     print("Done")



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