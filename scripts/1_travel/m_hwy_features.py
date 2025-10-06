## m_hwy_features.py
## a translation of incorporate_edits.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.BHN import BusHighwayNetwork

import os
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--final", help="finalize highway project coding",
                        action="store_true")
    args = parser.parse_args()

    BHN = BusHighwayNetwork()

    # have to change manually
    BHN.current_gdb = os.path.join(BHN.mhn_out_folder, f"MHN_{BHN.base_year}.gdb")
    BHN.built_gdbs.append(BHN.current_gdb)

    if not args.final:
        BHN.resolve_hwy_geometry()
        BHN.check_hwy_fcs()
    else:
        BHN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")