"""
This module contains tests for the 'generate_hwy_files' tool in MFHRN.
"""

# SECTION: External dependencies
import pandas as pd
import os
from pathlib import Path
import subprocess

# SECTION: Internal dependencies
from export_future_hwys import _check_export_future_hwys_run

# SECTION: Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
TEST_DIR_PATH = os.path.join(PROJECT_ROOT, "tests")

# -- Inputs

# testing inputs paths (e.g. input_years.csv)
INPUTS_DIR_PATH = os.path.join(TEST_DIR_PATH, "inputs")
SHARED_INPUTS_DIR_PATH = os.path.join(INPUTS_DIR_PATH, "shared")
INPUT_YEARS_CSV_PATH = os.path.join(SHARED_INPUTS_DIR_PATH, "input_years.csv")
GENERATE_HWY_FILES_INPUTS_DIR_PATH = os.path.join(INPUTS_DIR_PATH, "generate_hwy_files")

# Emme Known good files (given before state?)
waGOOD_EMME_HWY_FILE_DIR_PATH = os.path.join(
    GENERATE_HWY_FILES_INPUTS_DIR_PATH, "emme_hwy_files"
)

# source code paths
SCRIPTS_DIR_PATH = os.path.join(PROJECT_ROOT, "scripts")
TRAVEL_SCRIPTS_DIR_PATH = os.path.join(SCRIPTS_DIR_PATH, "1_travel")
TRAVEL_SCRIPTS_MODULE_DIR_PATH = os.path.join(SCRIPTS_DIR_PATH, "modules")
HN_FILE_PATH = os.path.join(TRAVEL_SCRIPTS_MODULE_DIR_PATH, "HN.py")

# main script file for this tool
GENERATE_HWY_FILES_PY_PATH = os.path.join(
    TRAVEL_SCRIPTS_DIR_PATH, "2_generate_hwy_files.py"
)

# output files and dirs
OUTPUTS_DIR_PATH = os.path.join(PROJECT_ROOT, "output")
TRAVEL_OUTPUTS_DIR_PATH = os.path.join(OUTPUTS_DIR_PATH, "1_travel")

# output emme hwy files
OUTPUT_EMME_FILES_PATH = os.path.join(TRAVEL_OUTPUTS_DIR_PATH, "highway")


# SECTION: Functions
def _check_can_run_tool():
    """
    Helper function: `generate_hwy_files` tool requires
    that `export_future_hwys` tool has been run.
    Simple wrapper function around `_check_export_future_hwys_run` function
    to make sure that the tests for `generate_hwy_files`
    can be run properly.
    """
    _check_export_future_hwys_run()


def test_run_full_script_no_flags():
    """
    Tests a full run of the script of completes succesfully
    """
    command = f"python {GENERATE_HWY_FILES_PY_PATH}"
    command_result = subprocess.run(command).stdout
    print(command_result)
    # TODO: assert __ == __
    for scenario in os.listdir(OUTPUT_EMME_FILES_PATH):
        print(scenario)


def test_run_full_script_subset_():
    pass


# SECTION: Main function
def main():
    # make sure that processes run from project root as CWD
    os.chdir(PROJECT_ROOT)

    # check that `export_future_hwys` has been run
    _check_can_run_tool()

    # running test functions
    # -- TESTS GO HERE
    test_run_full_script_no_flags()


if __name__ == "__main__":
    main()
