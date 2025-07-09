## a1_HN.py 
## combination of:
## 1) MHN.py, 
## 2) process_highway_coding.sas,
## 3) coding_overlap.sas,
## 4) generate_highway_files_2.sas, and
## 5) import_highway_projects_2.sas

## Author: npeterson
## Translation by by ccai (2025)

import os
import shutil
import sys
import arcpy
import pandas as pd

class HighwayNetwork:

    # constructor
    def __init__(self):

        # get paths 
        sys_path = sys.argv[0]
        abs_path = os.path.abspath(sys_path)
        mfhrn_path = os.path.dirname(os.path.dirname(os.path.dirname(abs_path)))
        
        self.in_folder = os.path.join(mfhrn_path, "input")
        self.in_gdb = os.path.join(self.in_folder, "MHN.gdb")

        self.mhn_out_folder = os.path.join(mfhrn_path, "output", "1_MHN")
        
        years_csv_path = os.path.join(self.in_folder, "years.csv")
        self.years_list = pd.read_csv(years_csv_path)["years"].to_list()

        # highway files - names of feature classes + tables in MHN
        self.hwy_files = [
            "hwynet/hwynet_arc",
            "hwynet/hwynet_node",
            "hwynet/hwyproj",
            "hwyproj_coding"
            ]
        
        # bus files - names of feature classes + tables in MHN
        # TODO - THERE ARE JUNK FILES IN HERE!! 
        self.bus_files = [
            "hwynet/bus_base",
            "hwynet/bus_current",
            "hwynet/bus_future",
            "bus_base_itin",
            "bus_base_itin_old",
            "bus_base_itin_new",
            "bus_current_itin",
            "bus_current_itin_old",
            "bus_current_itin_new",
            "bus_future_itin",
            "bus_future_itin_old",
            "bus_future_itin_new",
            "parknride" # not totally sure what this is. 
        ]
        
        # relationship classes in MHN 
        self.rel_classes = [
            "rel_arcs_to_bus_base_itin",
            "rel_arcs_to_bus_current_itin",
            "rel_arcs_to_bus_future_itin",
            "rel_arcs_to_hwyproj_coding",
            "rel_bus_base_to_itin",
            "rel_bus_current_to_itin",
            "rel_bus_future_to_itin",
            "rel_hwyproj_to_coding",
            "rel_nodes_to_parknride"
        ]
        
        self.current_gdb = self.in_gdb # as right now we're in the input folder 

        self.hwylink_df = None
        self.hwynode_df = None
        self.hwyproj_df = None
        self.hwyproj_years_df = None

        self.get_hwy_dfs()

        self.base_year = min(self.hwyproj_years_df.COMPLETION_YEAR.to_list()) - 1

    # helper function to get dfs 
    def get_hwy_dfs(self):

        hwylink = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        hwylink_fields = [f.name for f in arcpy.ListFields(hwylink) if f.type!="Geometry"]
        self.hwylink_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwylink, hwylink_fields)], 
            columns = hwylink_fields)
        
        hwynode = os.path.join(self.current_gdb, "hwynet/hwynet_node")
        hwynode_fields = [f.name for f in arcpy.ListFields(hwynode) if f.type != "Geometry"]
        self.hwynode_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwynode, hwynode_fields)], 
            columns = hwynode_fields)

        hwyproj = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_fields = [f.name for f in arcpy.ListFields(hwyproj)]
        self.hwyproj_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwyproj, hwyproj_fields)], 
            columns = hwyproj_fields)
        
        hwyproj_years = os.path.join(self.current_gdb, "hwynet/hwyproj")
        hwyproj_years_fields = ["TIPID", "COMPLETION_YEAR"]
        self.hwyproj_years_df = pd.DataFrame(
            data = [row for row in arcpy.da.SearchCursor(hwyproj_years, hwyproj_years_fields)],
            columns = hwyproj_years_fields)
        
    # helper function to delete relationship classes
    def del_rcs(self):

        for rc in self.rel_classes:
            rc_path = os.path.join(self.current_gdb, rc)
            arcpy.management.Delete(rc_path)

    # helper function to delete bus files
    def del_bus(self):

        for bus_file in self.bus_files:
            bus_path = os.path.join(self.current_gdb, bus_file)
            arcpy.management.Delete(bus_path)
    
    # function that generates base year gdb
    def generate_base_year(self):

        # delete output folder + recreate it 
        mhn_out_folder = self.mhn_out_folder
        out_folder = os.path.dirname(mhn_out_folder)
        in_gdb = self.in_gdb
        base_year = self.base_year 

        if os.path.isdir(out_folder) == True:
            shutil.rmtree(out_folder)
        
        os.mkdir(out_folder)
        os.mkdir(mhn_out_folder)

        # copy GDB
        out_gdb = os.path.join(mhn_out_folder, f"MHN_{base_year}.gdb")
        arcpy.management.Copy(in_gdb, out_gdb)
        self.current_gdb = out_gdb # update the HN's current gdb 

        self.del_rcs()
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")
        hwyproj_year_fc = os.path.join(self.current_gdb, "hwynet/hwyproj")

        # to make my life easier - add a field with REP-ABB to the project df
        arcpy.management.AddField(hwyproj_coding_table, "REP_ABB", "TEXT") # to make my life easier 
        arcpy.management.CalculateField(
            in_table= hwyproj_coding_table,
            field="REP_ABB", expression="rep_abb(!REP_ANODE!, !REP_BNODE!)",
            expression_type="PYTHON3",
            code_block="""def rep_abb(rep_anode, rep_bnode):
            return str(rep_anode) + "-" + str(rep_bnode) + "-1" """,
            field_type="TEXT",
            enforce_domains="NO_ENFORCE_DOMAINS")
        
        arcpy.management.JoinField(hwyproj_coding_table, "TIPID", hwyproj_year_fc, "TIPID")
        arcpy.management.AddField(hwyproj_coding_table, "USE", "SHORT")
        arcpy.management.CalculateField(hwyproj_coding_table, "USE", "1")
        arcpy.management.DeleteField(
            hwyproj_coding_table, 
            ["TIPID_1", "MCP_ID", "RSP_ID", "Shape_Length"])
        arcpy.management.AddField(hwyproj_coding_table, "NOTES", "TEXT")

        # add fields to review updates (a la Volpe)
        hwylink_fc = os.path.join(self.current_gdb, "hwynet/hwynet_arc")
        arcpy.management.AddFields(hwylink_fc, [["PROJECT", "TEXT"], ["DESCRIPTION", "TEXT"]])

        self.get_hwy_dfs() # update the HN's current dfs 

        print("Base year copied and prepared for modification.")

    # def check_base_link_fc(self):

    # function that checks the project table
    def check_base_project_table(self):
        
        mhn_out_folder = self.mhn_out_folder
        base_year = self.base_year

        base_project_table_warnings = os.path.join(
            mhn_out_folder, 
            "base_project_table_warnings.txt")
        error_file= open(base_project_table_warnings, "a") # open error file, don't forget to close it!

        # create dfs now to be compared to later
        hwylink_df = self.hwylink_df
        toll1_list = hwylink_df[(hwylink_df.TYPE1 == "7") & (hwylink_df.POSTEDSPEED1 == 0)].ABB.to_list()
        toll2_list = hwylink_df[(hwylink_df.TYPE2 == "7") & (hwylink_df.POSTEDSPEED2 == 0)].ABB.to_list()

        # projects with year 9999 are not well maintained 
        # only check integrity if != 9999

        hwyproj_df = self.hwyproj_df[self.hwyproj_df.COMPLETION_YEAR != 9999]
        hwyproj_coding_table = os.path.join(self.current_gdb, "hwyproj_coding")

        # check that no TIPID + ABB is duplicated. 
        hwyproj_group_df = hwyproj_df.groupby(["TIPID", "ABB"]).size().reset_index()
        hwyproj_group_df = hwyproj_group_df.rename(columns = {0: "size"})
        hwyproj_dup_dict = hwyproj_group_df[hwyproj_group_df["size"] > 1].to_dict("records")

        dup_set = set()
        for dup in hwyproj_dup_dict: 
            dup_set.add((dup["TIPID"], dup["ABB"]))

        # don't use the duplicates
        fields = ["TIPID", "ABB", "USE", "NOTES"]
        dup_fail = 0
        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, "COMPLETION_YEAR <> 9999") as ucursor:
            for row in ucursor:
                if (row[0], row[1]) in dup_set:
                    row[2] = 0
                    row[3] = "Duplicate TIPID-ABB combination. Must be unique."
                    dup_fail+=1
                    ucursor.updateRow(row)
        
        if dup_fail != 0:
            error_file.write(f"{dup_fail} rows failed the duplicate check and had USE set to 0. Check output gdb.\n")
        else:
            error_file.write("No rows failed the duplicate check.\n")

        # check row by row 
        # don't have to check the validity of already discarded rows
        # just count the rows with mistakes

        row_fail = 0
        fields = [f.name for f in arcpy.ListFields(hwyproj_coding_table) if f.name != "OBJECTID"]
        where_clause = "COMPLETION_YEAR <> 9999 AND USE = 1"
        with arcpy.da.UpdateCursor(hwyproj_coding_table, fields, where_clause) as ucursor:
            for row in ucursor:
                tipid = row[0] 
                abb = row[21]
                # use = row[26], notes = row[27]

                # for some reason these are saved as text fields but they should only contain numbers
                try: 
                    action_code = int(row[1])
                    new_directions = int(row[2])
                    new_type1 = int(row[3]); new_type2 = int(row[4])
                    new_ampm1 = int(row[5]); new_ampm2 = int(row[6])
                    new_modes = int(row[19])
                except:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Non-numeric values in fields where it should be numeric."
                    ucursor.updateRow(row)
                    continue

                new_postedspeed1 = row[7]; new_postedspeed2 = row[8]
                new_thrulanes1 = row[9]; new_thrulanes2 = row[10]
                new_thrulanewidth1 = row[11]; new_thrulanewidth2 = row[12]
                add_parklanes1 = row[13]; add_parklanes2 = row[14]
                add_sigic = row[15]
                add_cltl = row[16]
                add_rrgradecross = row[17]
                new_tolldollars = row[18]
                rep_abb = row[24]

                # check that TIPID is valid 
                tipids = self.hwyproj_years_df.TIPID.to_list()
                if tipid not in tipids: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "TIPID is not a legitimate project."
                    ucursor.updateRow(row)
                    continue

                # check that ABB is valid
                abbs = self.hwylink_df.ABB.to_list()
                if abb not in abbs:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "ABB is not an actual link."
                    ucursor.updateRow(row)
                    continue

                # check that values are within range 
                if action_code not in [1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Action code is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_directions not in [0, 1, 2, 3]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Directions flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_type1 not in [0, 1, 2, 3, 4, 5, 6, 7, 8]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Type 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_type2 not in [0, 1, 2, 3, 4, 5, 6, 7, 8]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Type 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_ampm1 not in [0, 1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "AMPM 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_ampm2 not in [0, 1, 2, 3, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "AMPM 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_postedspeed1 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Posted speed 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_postedspeed2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Posted speed 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanes1 < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Through lanes 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanes2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Through lanes 2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanewidth1 < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Through lanes width 1 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_thrulanewidth2 < 0:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Through lanes width2 flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_sigic not in [0, 1]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Sigic flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_cltl not in [-1, 0, 1]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Cltl flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if add_rrgradecross not in [-1, 0, 1]: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Railroad crossing flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_tolldollars < 0: 
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Toll dollar flag is not valid."
                    ucursor.updateRow(row)
                    continue
                if new_modes not in [0, 1, 2, 3, 4, 5]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Mode flag is not valid."
                    ucursor.updateRow(row)
                    continue

                # check that action codes are valid. 
                baselink = int(abb[-1])
                if baselink == 0 and action_code not in [2, 4]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Skeleton links cannot have action codes 1 or 3 applied to them."
                    ucursor.updateRow(row)
                    continue
                if baselink == 1 and action_code not in [1,3]:
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Regular links cannot have action codes 2 or 4 applied to them."
                    ucursor.updateRow(row)
                    continue

                # check that REP_ANODE + REP_BNODE are only associated with Action Code 2 
                if action_code in [1, 3, 4] and rep_abb != "0-0-1":
                    row_fail+=1
                    row[26] = 0
                    row[27] = "REP_ANODE + REP_BNODE are invalid if action code != 2."
                    ucursor.updateRow(row)
                    continue

                # check that action codes 2 + 3 don't have other attributes filled in 
                attributes = [new_directions, new_type1, new_type2, new_ampm1, new_ampm2,
                              new_postedspeed1, new_postedspeed2, new_thrulanes1, new_thrulanes2, 
                              new_thrulanewidth1, new_thrulanewidth2, add_parklanes1, add_parklanes2,
                              add_sigic, add_cltl, add_rrgradecross, new_tolldollars, new_modes]
                
                if action_code in [2, 3] and any(attribute != 0 for attribute in attributes):
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Action Codes 2 + 3 should not be associated with any other attributes."
                    ucursor.updateRow(row)
                    continue

                # action code 1 must have at least 1 attribute filled in. 
                if action_code == 1 and all(attribute == 0 for attribute in attributes):
                    row_fail+=1
                    row[26] = 0
                    row[27] = "Action Code 1 must make at least one modification."
                    ucursor.updateRow(row)
                    continue

                # if action code 2, REP_ABB must exist 
                if action_code == 2 and rep_abb not in abbs:
                    row_fail+=1
                    row[26] = 0 
                    row[27] = "Action code 2 must be associated with a valid REP_ABB."
                    ucursor.updateRow(row)
                    continue

                # if action code = 4, then required attributes must be filled
                req_fields = [new_directions, new_type1, new_ampm1, new_thrulanes1, new_thrulanewidth1, new_modes]
                if action_code == 4:
                    if any(field == 0 for field in req_fields):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Missing required attribute(s) on new link."
                        ucursor.updateRow(row)
                        continue
                    elif new_type1 != 7 and new_postedspeed1 == 0:
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Missing speed 1 on new link."
                        ucursor.updateRow(row)
                        continue

                # if action code = 1 or 4 and directions = 1 or 2
                # then all 2 fields should be 0 
                all_fields2 = [new_type2, new_ampm2, new_postedspeed2, new_thrulanes2, new_thrulanewidth2, add_parklanes2]

                if action_code in [1, 4] and new_directions in [1,2]:
                    if any(field2 != 0 for field2 in all_fields2):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = f"Unusable '2' attributes for this direction = {new_directions} link."
                        ucursor.updateRow(row)
                        continue
                    
                # if action code = 1 or 4 and directions = 3 
                # then new_type2, new_ampm2, new_thrulanes2, + new_thrulanes2 should not be 0 
                req_fields2 = [new_type2, new_ampm2, new_thrulanes2, new_thrulanewidth2]

                if action_code in [1, 4] and new_directions == 3:
                    if any(field2 == 0 for field2 in req_fields2):
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Missing '2' attributes for this direction = 3 link."
                        ucursor.updateRow(row)
                        continue 
                    elif new_type2 !=7 and new_postedspeed2 == 0:
                        row_fail+=1
                        row[26] = 0
                        row[27] = "Missing speed 2 on this direction = 3 link."
                        ucursor.updateRow(row)
                        continue

                # very specific situation with toll plazas
                if action_code == 1 and abb in toll1_list:
                    if new_type1 != 0 and new_postedspeed1 == 0: 
                        row_fail+=1
                        row[26] = 0 
                        row[27] = "Missing new posted speed 1 on a link which changed from a toll plaza."
                        ucursor.updateRow(row)
                        continue

                if action_code == 1 and abb in toll2_list:
                    if new_type2 != 0 and new_postedspeed2 == 0:
                        row_fail+=1
                        row[26] = 0
                        row[27] = "Missing new posted speed 2 on a link which changed from a toll plaza."
                        ucursor.updateRow(row)
                        continue
            
            # out of for loop
        
        # out of cursor
        if row_fail != 0:
            error_file.write(f"{row_fail} rows failed the individual row check and had USE set to 0. Check output gdb.\n")
        else:
            error_file.write("No rows failed the individual row check.\n")

        error_file.close()

    #     # make sure that every arc which is getting replaced 
    #     # is also deleted (3)

    #     rep_abbs_df = hwyproj_2050_df[hwyproj_2050_df.REP_ABB != "0-0-1"][["TIPID", "REP_ABB", "COMPLETION_YEAR"]].drop_duplicates()
    #     action_3_df = hwyproj_2050_df[hwyproj_2050_df.ACTION_CODE == "3"][["TIPID", "ABB", "COMPLETION_YEAR", "ACTION_CODE"]]
    #     check_3_df = pd.merge(rep_abbs_df, action_3_df, how = "left", left_on= ["TIPID", "REP_ABB", "COMPLETION_YEAR"], right_on = ["TIPID", "ABB", "COMPLETION_YEAR"])
    #     check_3_df_okay = check_3_df[check_3_df.ACTION_CODE.notnull()] # has a corresponding delete
    #     check_3_df_null = check_3_df[check_3_df.ACTION_CODE.isnull()] # does not have a corresponding delete 
        
    #     print(f"{len(check_3_df_okay)} out of {len(check_3_df)} of the replaced links were also deleted in that same TIPID + year")
    #     print(f"{len(check_3_df_null)} were not. Writing to base_project_table_notes.txt")
        
    #     base_project_table_notes = os.path.join(mhn_out_folder, "base_project_table_notes.txt")

    #     with open(base_project_table_notes, "a") as f:
    #         f.write("These replaced links were not also deleted.\n")
    #         f.write(check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_string())
    #         f.write("\n\n")

    #     check_3_dict_null = check_3_df_null[["TIPID", "REP_ABB", "COMPLETION_YEAR"]].to_dict("records")

    #     with arcpy.da.Editor(out_GDB):
    #         # every other field has a default of 0
    #         fields = ["TIPID", "ACTION_CODE", "ABB", "NEW_TOLLDOLLARS", "NEW_MODES", "COMPLETION_YEAR", "NOTES"]
    #         with arcpy.da.InsertCursor(hwyproj_coding_table, fields) as icursor:
    #             for row in check_3_dict_null:
    #                 icursor.insertRow((row["TIPID"], "3", row["REP_ABB"], 0, "0", row["COMPLETION_YEAR"], "added action code 3"))

# main function for testing 
if __name__ == "__main__":

    HN = HighwayNetwork()
    HN.generate_base_year()
    HN.check_base_project_table()

    print("Done")