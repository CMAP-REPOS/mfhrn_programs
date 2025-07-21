## u_export_future_networks.py
## a translation of export_future_network.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from a_HN import HighwayNetwork

import sys
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("version", help = "please choose whether this is draft or final")
    args = parser.parse_args()
    version = args.version

    if version != "draft" and version != "final":
        sys.exit("error: you must choose whether the version is 'draft' or 'final'")

    HN = HighwayNetwork()
    print(f"The original base year is {HN.base_year}.")
    HN.generate_base_year()
    HN.check_hwy_fcs()
    HN.check_hwy_project_table()
    HN.build_future_hwys()
    if version == "final":
        HN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")