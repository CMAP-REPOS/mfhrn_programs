"""
This script allows anybody who needs to run tests
to easily setup and configure the testing environment
so that testers do not encounter environment issues
"""

"""
Author: Aaron Rumph
Updated: 07/02/26
"""

# SECTION: External dependencies

import argparse
import os
import subprocess
import logging
from pathlib import Path
import tomllib


logging.basicConfig(
    level=logging.INFO, format="%(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

# SECTION: Constants

_USER_HOME_DIR = Path.home()
_DEFAULT_ARCPY_PATH = os.path.join(
    _USER_HOME_DIR, "AppData", "Local", "ESRI", "conda", "envs", "arcpy"
)
_DEFAULT_SAS_PATH = r"C:\Program Files\SASHome\SASFoundation\9.4\sas.exe"
_TESTING_DIR_PATH = Path(__file__).parent
# the path where the config file will be written to
TESTING_CONFIG_PATH = os.path.join(_TESTING_DIR_PATH, "src", "_testing_config.py")


# SECTION: Functions


def parse_arguments():
    """
    Reads in and parses command line args for configuring testing of mfhrn
    """

    # Creating parser
    parser = argparse.ArgumentParser(description="Configuration for testing of mfrhn")

    # Arg 1: path to mhn_programs repo
    parser.add_argument(
        "--mhn-repo-path",
        dest="mhn_repo_path",
        type=str,
        help=(
            "Path to the 'mhn_programs' repo root containing the MHN tools source code"
        ),
        required=True,
    )

    # Arg 2: path to MHN GeoDatabase
    parser.add_argument(
        "--mhn-gdb-path",
        dest="mhn_gdb_path",
        type=str,
        help="Path to your MHN GeoDatabase",
        required=True,
    )

    # Arg 3: path to arcpy env to use for testing
    parser.add_argument(
        "--arcpy-env-path",
        dest="arcpy_env_path",
        type=str,
        help=(
            "Path to the ArcPy environment you plan to use for running tests"
            " (if different than default ArcPy env location)"
        ),
        default=f"{_DEFAULT_ARCPY_PATH}",
    )

    # Arg 4: path to SAS
    parser.add_argument(
        "--sas-path",
        dest="sas_path",
        type=str,
        help=(
            "Path to your SAS executable you plan to use for running tests"
            " (if different than default SAS location)"
        ),
        default=f"{_DEFAULT_SAS_PATH}",
    )

    # Arg 5: time tests by default
    parser.add_argument(
        "--time-by-default",
        dest="time_by_default",
        action="store_true",
        help="Whether to time all tests by default",
    )

    return parser.parse_args()


def check_mhn_git_branch(mhn_repo_path: str):
    """
    Checks the current branch for the MHN repo and gives user feedback if
    on incorrect branch.
    """
    try:
        # check which branch currently on
        check_branch_cmd = f"git -C {str(mhn_repo_path)} branch --show-current"
        current_branch = subprocess.run(
            check_branch_cmd,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        # CASE: not on main branch
        if current_branch != "main":
            switch_main_cmd = "git checkout main"
            raise EnvironmentError(
                f"You are not currently on the 'main' branch of mhn_programs."
                f" Please switch using {switch_main_cmd}"
            )
        else:
            logging.debug("Your MHN repo is on the correct branch")

    except subprocess.CalledProcessError:
        # CASE: not a git dirrectory
        raise ValueError(
            f"The inputted MHN repo path is not a Git repo."
            f" Are you sure this is the correct path to the MHN repo: {mhn_repo_path}"
        )


def write_test_config_file(
    mhn_repo_path: str,
    mhn_gdb_path: str,
    arcpy_env_path: str,
    sas_path: str,
    time_by_default: bool,
):
    """
    Writes config options for testing to a python file called
    '_testing_config.py' to be used by tests to reduce number of env problems
    """
    # NOTE: AR: This function is incredibly goofily formatted
    # because of the docstrings and writing to Python file code
    # but it works!

    # docstring for config file
    _config_docstring = """'''
This module contains user set config information for testing.
Specifically, it provides the path for:
SAS, ArcPy, the MHN Repo, the MHN GeoDatabase, and
whether or not to time tests by default.
'''
"""

    # text to write to config (python) file so can be used in tests
    config_text = f"""{_config_docstring}

MHN_REPO_PATH = r"{mhn_repo_path}"
MHN_GDB_PATH = r"{mhn_gdb_path}"
ARCPY_ENV_PATH = r"{arcpy_env_path}"
SAS_PATH = r"{sas_path}"
TIME_BY_DEFAULT = {time_by_default}
    """
    with open(TESTING_CONFIG_PATH, "w", encoding="utf-8") as config_file:
        config_file.write(config_text)


# SECTION: Main logic


def main():
    """
    Main function for testing config tool
    """
    # -- Read in args
    config_args = parse_arguments()

    mhn_repo_path = config_args.mhn_repo_path
    mhn_gdb_path = config_args.mhn_gdb_path
    arcpy_env_path = config_args.arcpy_env_path
    sas_path = config_args.sas_path
    time_by_default = config_args.time_by_default

    # -- Validate MHN repo
    if not os.path.isdir(mhn_repo_path):
        raise ValueError(f"The inputted MHN repo path does not exist: {mhn_repo_path}")
    # git validation (correct branch, is git repo)
    check_mhn_git_branch(mhn_repo_path)

    # TODO: -- Validate MHN GDB

    # TODO: -- Validate ArcPy env

    # TODO: -- Validate SAS

    # -- Write config options to file
    write_test_config_file(
        mhn_repo_path=mhn_repo_path,
        mhn_gdb_path=mhn_gdb_path,
        arcpy_env_path=arcpy_env_path,
        sas_path=sas_path,
        time_by_default=time_by_default,
    )


if __name__ == "__main__":
    main()
