## ETN.py
## a combination of:
## 1) MHN.py
## 2) generate_highway_files_2.sas

## Author: npeterson
## Translated by ccai (2025)

import os
import sys
import shutil
import arcpy
import pandas as pd
from datetime import date

class EmmeTransitNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_travel")

        in_folder = os.path.join(mfhrn_path, "input")
        years_csv_path = os.path.join(in_folder, "input_years.csv")

        self.years_list = pd.read_csv(years_csv_path)["year"].to_list()
        self.years_dict = pd.read_csv(years_csv_path).set_index("year")["scenario"].to_dict()

    # MAIN METHODS --------------------------------------------------------------------------------

    # method that generates files for input into EMME
    def generate_hwy_files(self):

        print("Generating highway files...")

        years_list = self.years_list
        years_dict = self.years_dict

        arcpy.env.workspace = os.path.join(self.mhn_out_folder, "MHN_all.gdb")

        emme_hwy_folder = os.path.join(self.mhn_out_folder, "highway")

        if os.path.isdir(emme_hwy_folder) == True:
            shutil.rmtree(emme_hwy_folder)
        os.mkdir(emme_hwy_folder)

        for year in years_list:

            scenario = years_dict[year]

            emme_scen_folder = os.path.join(emme_hwy_folder, str(scenario))
            os.mkdir(emme_scen_folder)

        print("Highway files generated.\n")