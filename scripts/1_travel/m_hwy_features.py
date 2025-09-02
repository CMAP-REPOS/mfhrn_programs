## m_hwy_features.py
## a translation of incorporate_edits.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.HN import HighwayNetwork

import os
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    HN = HighwayNetwork()

    # have to change manually
    HN.current_gdb = os.path.join(HN.mhn_out_folder, f"MHN_{HN.base_year}.gdb")
    HN.built_gdbs.append(HN.current_gdb)

    HN.check_hwy_fcs()
    HN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")