## m_highway_projects.py
## a translation of import_highway_projects.py
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
    parser.add_argument("-f", "--final", help="finalize highway project coding",
                        action="store_true")
    args = parser.parse_args()

    # check for import file
    sys_path = sys.argv[0]
    abs_path = os.path.abspath(sys_path)
    mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
    import_path = os.path.join(mfhrn_path, "input", "1_MHN", "import_hwy_project_coding.xlsx")

    if not os.path.exists(import_path):
        sys.exit("Please provide a file of the project coding to import as import_hwy_project_coding.xlsx.")

    # import highway project coding
    HN = HighwayNetwork()
    HN.generate_base_year()
    HN.check_hwy_fcs()
    HN.import_hwy_project_coding()
    HN.check_hwy_project_table()
    if args.final:
        HN.finalize_hwy_data()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")