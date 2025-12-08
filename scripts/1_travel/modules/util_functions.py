# util_functions.py
# utility functions which may be reused in multiple scripts
# author: ccai

import os
import arcpy

def create_directional_hwy_records(hwylink_fc, where_clause):

    link_fields = [f.name for f in arcpy.ListFields(hwylink_fc) if (f.type!="Geometry")]
    lf_dict = {field: index for index, field in enumerate(link_fields)}

    hwylink_records = []

    with arcpy.da.SearchCursor(hwylink_fc, link_fields, where_clause) as scursor:
        for row in scursor:
            
            dirs = row[lf_dict["DIRECTIONS"]]

            common_attr_dict = {
                "SIGIC": row[lf_dict["SIGIC"]],
                "CLTL": row[lf_dict["CLTL"]],
                "RRGRADECROSS": row[lf_dict["RRGRADECROSS"]],
                "TOLLDOLLARS": row[lf_dict["TOLLDOLLARS"]],
                "MODES": row[lf_dict["MODES"]],
                "VCLEARANCE": row[lf_dict["VCLEARANCE"]],
                "CHIBLVD" : row[lf_dict["CHIBLVD"]],
                "MILES": row[lf_dict["MILES"]],
            }

            if "PROJECT" in lf_dict:
                common_attr_dict["PROJECT"] = row[lf_dict["PROJECT"]]

            attr_dict = {
                "INODE" : row[lf_dict["ANODE"]],
                "JNODE" : row[lf_dict["BNODE"]],
                "TYPE": row[lf_dict["TYPE1"]],
                "AMPM": row[lf_dict["AMPM1"]],
                "POSTEDSPEED": row[lf_dict["POSTEDSPEED1"]],
                "THRULANES": row[lf_dict["THRULANES1"]],
                "THRULANEWIDTH": row[lf_dict["THRULANEWIDTH1"]],
                "PARKLANES": row[lf_dict["PARKLANES1"]],
                "PARKRES": row[lf_dict["PARKRES1"]]
            } | common_attr_dict

            hwylink_records.append(attr_dict)

            if dirs == "2":

                # parkres is coded separately
                rev_attr_dict = {
                    "INODE" : row[lf_dict["BNODE"]],
                    "JNODE" : row[lf_dict["ANODE"]],
                    "TYPE": row[lf_dict["TYPE1"]],
                    "AMPM": row[lf_dict["AMPM1"]],
                    "POSTEDSPEED": row[lf_dict["POSTEDSPEED1"]],
                    "THRULANES": row[lf_dict["THRULANES1"]],
                    "THRULANEWIDTH": row[lf_dict["THRULANEWIDTH1"]],
                    "PARKLANES": row[lf_dict["PARKLANES1"]],
                    "PARKRES": row[lf_dict["PARKRES2"]]
                } | common_attr_dict

                hwylink_records.append(rev_attr_dict)

            elif dirs == "3":

                # everything is coded separately
                rev_attr_dict = {
                    "INODE" : row[lf_dict["BNODE"]],
                    "JNODE" : row[lf_dict["ANODE"]],
                    "TYPE": row[lf_dict["TYPE2"]],
                    "AMPM": row[lf_dict["AMPM2"]],
                    "POSTEDSPEED": row[lf_dict["POSTEDSPEED2"]],
                    "THRULANES": row[lf_dict["THRULANES2"]],
                    "THRULANEWIDTH": row[lf_dict["THRULANEWIDTH2"]],
                    "PARKLANES": row[lf_dict["PARKLANES2"]],
                    "PARKRES": row[lf_dict["PARKRES2"]]
                } | common_attr_dict

                hwylink_records.append(rev_attr_dict)

    return hwylink_records