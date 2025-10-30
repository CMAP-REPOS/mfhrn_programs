## u_create_bus_layers.py

from modules.BHN import BusHighwayNetwork

import os
import sys
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    BHN = BusHighwayNetwork()
    # BHN.create_bn_folder()
    geom_dict = BHN.build_geom_dict()
    # BHN.collapse_bus_routes(geom_dict)
    BHN.create_bus_layers(geom_dict)

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")