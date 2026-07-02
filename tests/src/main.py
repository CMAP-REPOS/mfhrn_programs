"""
This script is the entry point for testing the tools
in the MFHRN repo. If you want to run all tests,
you can run the following command from the project root
(replace slashes with backslashes on Windows):

$ pytest tests\\src\\main.py
"""

"""
Author: Aaron Rumph
Updated: 07/02/2026
Notes: N/A
"""

# SECTION: External dependencies


# SECTION: Internal dependencies


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


load_testing_config()

# SECTION: Constants

# SECTION: Functions
