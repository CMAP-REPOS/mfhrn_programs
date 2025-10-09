## m_gtfs_bus_routes.py
## serves the same function as import_gtfs_bus_routes.py
## based on work by npeterson & tkoleary
## Translated + Updated by ccai (2025)

from modules.BHN import BusHighwayNetwork

import os
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()
    parser = argparse.ArgumentParser()
    parser.add_argument("gtfs", help = "choose base or current")
    args = parser.parse_args()

    BHN = BusHighwayNetwork()

    BHN.create_base_hwy()
    # BHN.check_hwy_fcs()
    BHN.import_gtfs_bus_routes(args.gtfs)

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")