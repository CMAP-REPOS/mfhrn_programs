## m_hwyproj_coding.py
## a translation of import_highway_projects.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from modules.HN import HighwayNetwork

import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--final", help="finalize highway project coding",
                        action="store_true")
    args = parser.parse_args()

    # import highway project coding
    HN = HighwayNetwork()
    HN.create_base_year()
    HN.check_hwy_fcs()
    HN.import_hwyproj_coding()
    HN.check_hwyproj_coding_table()
    if args.final:
        HN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")