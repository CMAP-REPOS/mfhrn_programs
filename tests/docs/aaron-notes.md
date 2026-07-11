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

## General Notes
- Tools and utilities lack documentation

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
