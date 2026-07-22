# Aaron's Notes

*Author: Aaron Rumph*
*Updated: 07/02/26*

#### Author's Notes
*Notes tagged with (DIFF) represent differences in behavior from MHN tools.*

## Questions:
- Code assumes that TIPIDs are unique

## TODOs:
- Add docstrings for utility classes/functions/methods
- Swap hardcoded paths
- Remove ArcPy (particularly for 3rd tool)
    in favor of Pandas

## General Notes
- Tools and utilities lack documentation

## Tim's Equivalence Testing Notes:
If you navigate to "M:\proj1\tko\MHN\mfhrn_equivalence_test," you'll find several folders. Each folder represents a different MHN processing script. The containing data within the "before" and "after" gdbs illustrate how the given tool alters the MHN geodatabase.
0_incorporate_edits (Updates hwynet_arc and hwynet_node)
before_state0.gdb -- contains unprocessed network changes, I made several random roadway links at the western edge of the region, they look like a little clump of hair
after_state1.gdb 
1_import_hwyproj_coding (Updates hwyproj and hwyproj_coding)
(Need to reference the test xlsx, also within this folder. Updates the new roadways from the previous step with project coding, and a couple other random things.)
before_state1.gdb
after_state2.gdb
2_update_hwyproj_years (Updates hwyproj)
(Needs all three csv's: year, required, nocode. (these names are confusing-- you will also hear them referenced as the following: year\=\=conformed, required\=\=exempt, nocode==uncodeable)
(You also need a copy of the MRN to run this script. Please copy-paste the following gdb somewhere and use the copy, so we can keep the original safe: "M:\adb\MRN\dev\mrn.gdb")
before_state2.gdb
after_state3.gdb
3_output
highway (this folder will contain the testable changes)
linkshape
transit

## Per Tool Notes
Notes for each tool in MFHRN

### Export Future Network tool
- Relies on hardcoded path where input goes in "input/1_travel/subset_hwy_projects.csv"
- Odd way of getting file path?
    *Should just replace 
    "os.path.dirname(os.path.dirname(...))" with "Path(__file__)"*
- Times tool by default, should be opt-in flag (something like `--time`)
- (DIFF) MFRHN relies entirely on subset csv whereas MHN takes the following parameters:
    - mhn_gdb_path: (path) path to the MHN geodatabase `--mhn-gdb-path`
    - scen_list: (str) semicolon-delimitted list of scenario years (e.g. 100;200)
    - root_path: (path) folder path to write Emme transaction files to
    - create_tollsys_flag: (bool) whether to create tollsys.flag file for Emme `--create-tollsys`
    - abm_output: (bool) (DEPRECATED) whether to generate ABM outputs
    - rsp_eval: (bool) whether to focus only on RSP number (IF CHECKED THEN FOLLOWING FLAGS)
        - rsp_column: (str) column in hwyproj with RSP number
        - rsp_number: (str) rsp number denoting project to output
        - no_build_tipid_csv: (path) csv with TIPIDS for projects part of no build

- input_years.csv: 
    Replace with flag like `--scen-list`: e.g., '--scen-list 2019;2026;2030;2040'

