## u_create_bus_layers.py

from modules.BHN import BusHighwayNetwork
from modules.RN import RailNetwork

import os
import sys
import argparse
import math
import time

if __name__ == "__main__":

    start_time = time.time()

    # create bus networks
    BHN = BusHighwayNetwork()
    # BHN.create_bn_folder()
    # BHN.collapse_bus_routes()
    BHN.create_bus_layers()

    # create rail networks
    # RN = RailNetwork()
    # RN.create_rn_folder()
    # RN.collapse_rail_routes()

    end_time = time.time()
    total_time = round(end_time - start_time)
    minutes = math.floor(total_time / 60)
    seconds = total_time % 60

    print(f"{minutes}m {seconds}s to execute.")

    print("Done")