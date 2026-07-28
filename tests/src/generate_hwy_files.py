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
GENERATE_HWY_FILES_INPUTS_DIR_PATH = os.path.join(INPUTS_DIR_PATH, "generate_hwy_files")

# Emme Known good files (given before state?)
GOOD_EMME_HWY_FILE_DIR_PATH = os.path.join(
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
EMME_FILE_EXTENSIONS = ("l1", "l2", "n1", "n2")
HIGHWAY_TODS = range(0, 9)


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
    command = [ARCPY_PYTHON_PATH, GENERATE_HWY_FILES_PY_PATH]
    command_result = subprocess.run(command, check=True).stdout
    print(command_result)
    # TODO: assert __ == __
    for scenario in os.listdir(OUTPUT_EMME_FILES_PATH):
        print(scenario)


def test_emme_file_output_structure():
    """
    Checks that each scenario contains the expected EMME highway files
    """
    scenarios = pd.read_csv(INPUT_YEARS_CSV_PATH)["scenario"].to_list()

    for scenario in scenarios:
        scenario_output_path = Path(OUTPUT_EMME_FILES_PATH) / str(scenario)
        expected_file_paths = {
            scenario_output_path / f"{scenario}0{tod}.{extension}"
            for tod in HIGHWAY_TODS
            for extension in EMME_FILE_EXTENSIONS
        }
        expected_file_paths.add(scenario_output_path / "highway.linkshape")

        missing_files = [
            str(file_path)
            for file_path in expected_file_paths
            if not file_path.is_file()
        ]
        if missing_files:
            raise FileNotFoundError(
                "Missing expected EMME files:\n" + "\n".join(missing_files)
            )

        empty_files = [
            str(file_path)
            for file_path in expected_file_paths
            if file_path.stat().st_size == 0
        ]
        if empty_files:
            raise ValueError(
                "Empty EMME highway files detected:\n" + "\n".join(empty_files)
            )


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
    test_emme_file_output_structure()


if __name__ == "__main__":
    main()
