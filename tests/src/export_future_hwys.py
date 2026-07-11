"""
This module contains tests for the 'export_future_hwy.py' tool in MFHRN.
"""

# SECTION: External dependencies
import pandas as pd
import os
from pathlib import Path
import subprocess

# SECTION: Internal dependencies

# SECTION: Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
TEST_DIR_PATH = os.path.join(PROJECT_ROOT, "tests")

# -- Inputs

# testing inputs paths (e.g. input_years.csv)
INPUTS_DIR_PATH = os.path.join(TEST_DIR_PATH, "inputs")
SHARED_INPUTS_DIR_PATH = os.path.join(INPUTS_DIR_PATH, "shared")
INPUT_YEARS_CSV_PATH = os.path.join(SHARED_INPUTS_DIR_PATH, "input_years.csv")

# source code paths
SCRIPTS_DIR_PATH = os.path.join(PROJECT_ROOT, "scripts")
TRAVEL_SCRIPTS_DIR_PATH = os.path.join(SCRIPTS_DIR_PATH, "1_travel")
TRAVEL_SCRIPTS_MODULE_DIR_PATH = os.path.join(SCRIPTS_DIR_PATH, "modules")
HN_FILE_PATH = os.path.join(TRAVEL_SCRIPTS_MODULE_DIR_PATH, "HN.py")

# main script file for this tool
EXPORT_FUTURE_HWY_PY_PATH = os.path.join(
    TRAVEL_SCRIPTS_DIR_PATH, "1_export_future_hwys.py"
)

# output files and dirs
OUTPUTS_DIR_PATH = os.path.join(PROJECT_ROOT, "output")
TRAVEL_OUTPUTS_DIR_PATH = os.path.join(OUTPUTS_DIR_PATH, "1_travel")


# SECTION: Functions


# NOTE: AR: Because export_future_hwys has to be run before
# running any of the other tools, this is a simple helper function
# to check whether or not it has been run
def _check_export_future_hwys_run():
    """
    Helper function: checks whether the export_future_hwys tool has
    been run or not by checking whether the corresponding output files
    have been created or not.

    The tool should create MHN_{year} for all years in `input_years.csv`
    """
    input_years = pd.read_csv(INPUT_YEARS_CSV_PATH)
    input_years = input_years["year"].to_list()

    mhn_all_gdb_path = os.path.join(TRAVEL_OUTPUTS_DIR_PATH, "MHN_all.gdb")
    if os.path.isdir(mhn_all_gdb_path):
        return

    for year in input_years:
        mhn_year_gdb_name = f"MHN_{year}.gdb"
        mhn_year_gdb_path = os.path.join(TRAVEL_OUTPUTS_DIR_PATH, mhn_year_gdb_name)
        if not os.path.isdir(mhn_year_gdb_path):
            raise Exception("You need to run the export_future_hwys_tool first!")


# SECTION: Tests


def test_highway_network_initialization():
    # TODO: Required inputs for TEST
    pass


def test_export_future_hwys():
    pass


def test_read_input_years():

    pass


def test_run_full_script_no_flags():
    command = f"python {EXPORT_FUTURE_HWY_PY_PATH}"
    command_result = subprocess.run(command).stdout
    print(command_result)


def test_run_full_script_subset_():
    pass


# SECTION: Main function
def main():
    # make sure that processes run from project root as CWD
    os.chdir(PROJECT_ROOT)

    # running test functions
    test_export_future_hwys()
    test_read_input_years()
    test_highway_network_initialization()
    test_run_full_script_no_flags()


if __name__ == "__main__":
    main()
