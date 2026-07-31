"""
This script is the entry point for testing the tools
in the MFHRN repo. If you want to run all tests,
you can run the following command from the project root:

$ python tests\\src\\main.py
"""

"""
Author: Aaron Rumph
Updated: 07/02/2026
Notes: N/A
"""

# SECTION: External dependencies
import logging
import argparse
import sys


# SECTION: Internal dependencies
import generate_hwy_files
import export_future_hwys
from export_future_hwys import test_run_full_script_no_flags
import import_hwyproj_coding
import input_preparation.base_mhn as base_mhn
import create_bus_layers
import generate_transit_files


# AR: This function defintion goes here just so that the import of config
# can be at the top
def load_testing_config():
    """
    Helper function: tries to import the testing config file,
    and if it is not found, tells user what to do.
    """
    try:
        import _testing_config
        from _testing_config import (
            MHN_REPO_PATH,
            MHN_GDB_PATH,
            ARCPY_ENV_PATH,
            SAS_PATH,
            TIME_BY_DEFAULT,
        )
    except ModuleNotFoundError:
        error_msg = """
        Could not find '_testing_config.py' in 'tests\\'.
        Please run the config tool by running:

        $ python tests\\config.py

        For help running the tool, run:

        $ python tests\\config.py --help
        """
        raise ModuleNotFoundError(error_msg)


# SECTION: Constants


# SECTION: Functions
def parse_arguments():
    """
    Reads in and parses command line args for configuring testing of mfhrn
    """

    # Creating parser
    parser = argparse.ArgumentParser(description="Configuration for test running")

    # Arg 0: bypass Y/n for running script with no args:
    parser.add_argument(
        "-y",
        dest="bypass_args",
        action="store_true",
        help=(
            "Whether to bypass 'no arguments provided' check"
            " (will run all tests except `import_hwyproj_coding`)"
        ),
    )
    # Arg 1: Whether to run all tests
    parser.add_argument(
        "--all",
        dest="run_all",
        action="store_true",
        help="Whether to run all tests (including `import_hwyproj_coding`)",
    )

    # Arg 2:
    parser.add_argument(
        "--export-future-hwys",
        dest="export_future_hwys",
        action="store_true",
        help="Whether to run the tests for `export_future_hwys` tool",
    )

    # Arg 3: path to arcpy env to use for testing
    parser.add_argument(
        "--generate_hwy_files",
        dest="generate_hwy_files",
        action="store_true",
        help="Whether to run the tests for `generate_hwy_files` tool",
    )

    # Arg 4:
    parser.add_argument(
        "--create-bus-layers",
        dest="create_bus_layers",
        action="store_true",
        help="Whether to run the tests for `create-bus-layers` tool",
    )

    # Arg 5:
    parser.add_argument(
        "--import-hwyproj-coding",
        dest="import_hwyproj_coding",
        action="store_true",
        help=(
            "Whether to run the tests for `import-hwyproj-coding` tool."
            " (WARNING: If you run this tool, and you do not have the correct"
            " `import_hwyproj_coding.xlsx` table in the inputs"
            " dir, you will get a very odd error."
        ),
    )

    # Arg 6:
    parser.add_argument(
        "--generate-transit-files",
        dest="generate_transit_files",
        action="store_true",
        help="Whether to run the tests for `generate_transit_files` tool",
    )

    # Arg 7:
    parser.add_argument(
        "--all-but-hwyproj-coding",
        dest="all_but_hwyproj_coding",
        action="store_true",
        help="Whether to run all tests except for `import-hwyproj-coding` tool",
    )

    return parser.parse_args()


def main():
    # load args
    config_args = parse_arguments()

    # if no args provided checking with user that want to run all tests
    # (except import_hwyproj_coding)
    if len(sys.argv) == 1:
        run_with_no_args = input(
            "You did not provide any arguments. Would you like to run all tests"
            " except `import_hwyproj_coding`? (default behavior) [Y/n]: "
        )
        if run_with_no_args.lower() not in ["yes", "y"]:
            print("Ok, exiting. Use --help to see arguments")
            sys.exit(1)
        else:
            print("Ok! Will run tests for all tools except `import_hwyproj_coding`")
            print("-" * 80)
            config_args.all_but_hwyproj_coding = True

    # NOTE: AR: because Cindy's scripts require files to be in specific
    # places, I added this function to make it so that you don't
    # have to copy files by hand into the right place to run the tests (at least the mfhrn only ones)

    # Also, these always runs to make sure that you have the correct MHN location
    print("Putting base MHN in correct locations")
    print("-" * 80)
    base_mhn.main()

    print("Importing testing config file")
    print("-" * 80)
    load_testing_config()

    # check if `--all` or `all-but-hwyproj-coding` used
    # and set other arguments accordingly
    if config_args.run_all:
        print("Running all tests")
        print("-" * 80)
        for arg, _ in vars(config_args).items():
            if arg != "run_all" and arg != "all_but_hwyproj_coding":
                setattr(config_args, arg, True)

    # since --all-but-hwyproj-coding called after all, can safely set run_all
    # and all others args to True except import_hwyproj_coding
    if config_args.all_but_hwyproj_coding:
        print("Running all tests except `import_hwyproj_coding`")
        print("-" * 80)
        for arg, _ in vars(config_args).items():
            if arg != "import_hwyproj_coding":
                setattr(config_args, arg, True)

    # now can go through and run the correct tests based on args
    if config_args.export_future_hwys:
        print("Running `export_future_hwys` tool")
        print("-" * 80)
        try:
            export_future_hwys.main()
        except Exception as e:
            print(f"`export_future_hwys` tool failed: {e}")
        else:
            print("`export_future_hwys` tool succeeded!")
        finally:
            print("\n")

    if config_args.generate_hwy_files:
        print("Running `generate_hwy_files` tool")
        print("-" * 80)
        try:
            generate_hwy_files.main()
        except Exception as e:
            print(f"`generate_hwy_files` tool failed: {e}")
        else:
            print("`generate_hwy_files` tool succeeded!")
        finally:
            print("\n")

    if config_args.create_bus_layers:
        print("Running `create_bus_layers` tool")
        print("-" * 80)
        try:
            create_bus_layers.main()
        except Exception as e:
            print(f"`create_bus_layers` tool failed: {e}")
        else:
            print("`create_bus_layers` tool succeeded!")
        finally:
            print("\n")

    if config_args.import_hwyproj_coding:
        print("Running `import_hwyproj_coding` tool")
        print("-" * 80)
        try:
            create_bus_layers.main()
        except Exception as e:
            print(f"`import_hwyproj_coding` tool failed: {e}")
        else:
            print("`import_hwyproj_coding` tool succeeded!")
        finally:
            print("\n")

    if config_args.generate_transit_files:
        print("Running `generate_transit_files` tool")
        print("-" * 80)
        try:
            generate_transit_files.main()
        except Exception as e:
            print(f"`generate_transit_files` tool failed: {e}")
        else:
            print("`generate_transit_files` tool succeeded!")
        finally:
            print("\n")


# SECTION: Main
if __name__ == "__main__":
    main()
