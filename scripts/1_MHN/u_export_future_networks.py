## u_export_future_networks.py
## a translation of export_future_network.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from a1_HN import HighwayNetwork

import sys
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("version", help = "please choose whether this is draft or final")
    args = parser.parse_args()
    version = args.version

    if version != "draft" and version != "final":
        sys.exit("error: you must choose whether the version is 'draft' or 'final'")

    HN = HighwayNetwork()
    print(f"The original base year is {HN.base_year}.")
    HN.generate_base_year()

    print("Done")