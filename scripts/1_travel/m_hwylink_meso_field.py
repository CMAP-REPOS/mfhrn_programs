## m_hwyproj_coding.py
## a translation of import_highway_projects.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.XHN import ExtraHighwayNetwork

import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    XHN = ExtraHighwayNetwork()
    XHN.create_base_hwy()
    XHN.edit_hwylink_meso()
    XHN.check_hwy_fcs()
    XHN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")