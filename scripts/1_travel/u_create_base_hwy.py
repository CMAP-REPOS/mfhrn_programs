## u_create_base_year.py
## Author: ccai (2025)

from modules.HN import HighwayNetwork

import math
import time

if __name__ == "__main__":

    start_time = time.time()

    HN = HighwayNetwork()
    HN.create_base_hwy()
    HN.check_hwy_fcs()
    HN.check_hwyproj_coding_table()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")