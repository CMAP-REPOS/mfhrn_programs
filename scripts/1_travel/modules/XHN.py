## XHN.py

## Author: npeterson
## Translated by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

from .BHN import BusHighwayNetwork

class ExtraHighwayNetwork(BusHighwayNetwork):

    def __init__(self):
        super().__init__()

    def edit_hwylink_meso(self):

        print("Editing hwylink meso field...")

        mhn_in_folder = self.mhn_in_folder
        override_shp = os.path.join(mhn_in_folder, "override_meso", "override_meso.shp")

        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")

        arcpy.management.MakeFeatureLayer(hwylink_fc, "hwylink_layer")
        arcpy.management.MakeFeatureLayer(override_shp, "override_layer", "USE = 1")
        arcpy.management.SelectLayerByLocation("hwylink_layer", "INTERSECT", "override_layer")
        arcpy.management.CalculateField("hwylink_layer", "MESO", "1")
        arcpy.management.SelectLayerByLocation("hwylink_layer", "INTERSECT", "override_layer", 
                                               invert_spatial_relationship = "INVERT")
        arcpy.management.CalculateField("hwylink_layer", "MESO", "0")

        print("Hwylink meso field edited.\n")