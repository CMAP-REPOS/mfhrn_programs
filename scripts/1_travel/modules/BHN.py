## BHN.py

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

from . import GeneralNetwork as gn
from .HN import HighwayNetwork

class BusHighwayNetwork(HighwayNetwork):

    def __init__(self):
        super().__init__()

    def resolve_hwy_geometry(self):

        in_path = os.path.join(self.mhn_in_folder, "MHN.gdb")
        out_path = os.path.join(self.mhn_out_folder, f"MHN_{self.base_year}.gdb")

        gn.resolve_geometry(in_path, out_path, "hwy")