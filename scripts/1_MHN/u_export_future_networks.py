## u_export_future_networks.py
## a translation of export_future_network.py
## Author: npeterson
## Translated + Updated by ccai (2025)

from a_MHN import MasterHighwayNetwork

import sys
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("version", help = "please choose whether this is draft or final")
    args = parser.parse_args()
    version = args.version

    if version != "draft" and version != "final":
        sys.exit("error: you must choose whether the version is 'draft' or 'final'")

    MHN = MasterHighwayNetwork()

    print(f"The original base year is {MHN.base_year}.")
    MHN.generate_base_year()
    MHN.clean_base_project_table()

    print("Done")