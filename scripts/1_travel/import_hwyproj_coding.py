## import_hwyproj_coding.py
## a translation of import_highway_projects.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.HN import HighwayNetwork

import argparse
import math
import time

start_time = time.time()

# import highway project coding
HN = HighwayNetwork()
HN.create_base_hwy()
HN.check_hwy_fcs()
HN.import_hwyproj_coding()
HN.check_hwyproj_coding_table()
HN.finalize_hwy_data()
HN.add_rcs()

end_time = time.time()
total_time = round(end_time - start_time)
minutes = math.floor(total_time / 60)
seconds = total_time % 60

print(f"{minutes}m {seconds}s to execute.")

print("Done")