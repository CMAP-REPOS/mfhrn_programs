"""
This module contains tests for the 'export_future_hwy.py' tool in MFHRN.
"""

# SECTION: External dependencies
import pandas as pd
import os
from pathlib import Path
import subprocess

# SECTION: Internal dependencies

from _testing_config import ARCPY_ENV_PATH

# SECTION: Constants
PROJECT_ROOT = Path(__file__).parent.parent.parent
TEST_DIR_PATH = os.path.join(PROJECT_ROOT, "tests")
ARCPY_PYTHON_PATH = os.path.join(ARCPY_ENV_PATH, "python.exe")

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
CREATE_BUS_LAYERS_PY_PATH = os.path.join(
    TRAVEL_SCRIPTS_DIR_PATH, "3_create_bus_layers.py"
)

# output files and dirs
OUTPUTS_DIR_PATH = os.path.join(PROJECT_ROOT, "output")
TRAVEL_OUTPUTS_DIR_PATH = os.path.join(OUTPUTS_DIR_PATH, "1_travel")


# SECTION: Functions


# SECTION: Tests


def test_run_full_script_no_flags():
    command = [ARCPY_PYTHON_PATH, CREATE_BUS_LAYERS_PY_PATH]
    command_result = subprocess.run(command, check=True).stdout
    print(command_result)


# SECTION: Main function
def main():
    # make sure that processes run from project root as CWD
    os.chdir(PROJECT_ROOT)

    # running test functions
    test_run_full_script_no_flags()


if __name__ == "__main__":
    main()
