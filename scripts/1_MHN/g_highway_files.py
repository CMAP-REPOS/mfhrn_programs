## g_highway_files.py
## a translation of generate_highway_files.py
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

    # generate highway files
    HN = HighwayNetwork()
    HN.generate_hwy_files()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")