## u_export_future_hwys.py
## a translation of export_future_network.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.HN import HighwayNetwork

import os
import sys
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--subset", help="subset to certain projects",
                        action="store_true")
    args = parser.parse_args()

    # check if subset = True
    if args.subset:
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
        subset_path = os.path.join(mfhrn_path, "input", "1_travel", "subset_hwy_projects.csv")

        if not os.path.exists(subset_path):
            sys.exit("Please provide a csv of the projects to subset to as subset_hwy_projects.csv.")

    # build highway networks
    HN = HighwayNetwork()
    HN.generate_base_year()
    HN.check_hwy_fcs()
    HN.check_hwyproj_coding_table()
    HN.build_future_hwys(subset = args.subset)

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")