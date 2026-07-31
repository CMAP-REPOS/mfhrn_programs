"""
This script prepares the base MHN geodatabase used by the MFHRN tests.
"""

"""
Author: Aaron Rumph
Updated: 07/24/2026
"""

# SECTION: External dependencies

from pathlib import Path
import arcpy
import os
import shutil
import subprocess
import time

# SECTION: Internal dependencies
from _testing_config import ARCPY_ENV_PATH, SAS_PATH, MHN_REPO_PATH


# SECTION: Constants

PROJECT_ROOT = Path(__file__).parent.parent.parent
INPUT_TRAVEL_DIR_PATH = os.path.join(PROJECT_ROOT, "input", "1_travel")
INPUT_HWYPROJ_CODING_XLSX = os.path.join(
    INPUT_TRAVEL_DIR_PATH, "import_hwyproj_coding.xlsx"
)
INPUT_MHN_GDB_PATH = os.path.join(INPUT_TRAVEL_DIR_PATH, "MHN.gdb")
TEST_INPUT_DIR_PATH = os.path.join(PROJECT_ROOT, "tests", "inputs", "pipeline")
TEST_OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, "tests", "outputs", "pipeline")
DUPLICATE_TIPIDS = {"10000115", "10030008"}
ARCPY_PYTHON_PATH = os.path.join(ARCPY_ENV_PATH, "python.exe")

# NOTE: AR: Because of hardcoded paths in both MFHRN and MHN, there is a lot of
# very fragile code that requires input data to be in specific places. As such,
# this script contains some odd choices in terms of where data lives for tests
# as well as moving that data around a lot.
# For the most part, the pipeline test's input dir closely matches the network
# in terms of layout, but files are copied locally to reduce amount of time waiting
# for network download, and moved to correct places for Cindy's MFHRN scripts

# Folders provided by Tim
MFHRN_EQUIVALENCE_DIR_PATH = r"M:\proj1\tko\MHN\mfhrn_equivalence_test"
DATA_INCORPORATE_EDITS_DIR_PATH = os.path.join(
    MFHRN_EQUIVALENCE_DIR_PATH, "0_incorporate_edits"
)
DATA_IMPORT_HWYPROJ_CODING_DIR_PATH = os.path.join(
    MFHRN_EQUIVALENCE_DIR_PATH, "1_import_hwy_coding"
)
DATA_UPDATE_HWYPROJ_YEARS = os.path.join(
    MFHRN_EQUIVALENCE_DIR_PATH, "2_update_hwyproj_years"
)
DATA_OUTPUT_DIR_PATH = os.path.join(MFHRN_EQUIVALENCE_DIR_PATH, "3_output")

# General test input data dirs
LOCAL_INCORPORATE_EDITS_DATA_DIR = os.path.join(
    TEST_INPUT_DIR_PATH, "incorporate_edits"
)
LOCAL_IMPORT_HWYCODING_DATA_DIR = os.path.join(TEST_INPUT_DIR_PATH, "import_hwy_coding")
LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR = os.path.join(
    TEST_INPUT_DIR_PATH, "update_hwyproj_years"
)
# the below is the equivale of '3_output' in MFHRN_EQUIVALENCE_DIR_PATH
LOCAL_OUTPUT_DATA_DIR_PATH = os.path.join(TEST_INPUT_DIR_PATH, "output")

# Files on network drives
BEFORE_STATE_0_GDB_PATH_NETWORK = os.path.join(
    DATA_INCORPORATE_EDITS_DIR_PATH, "before_state0.gdb"
)
BEFORE_STATE_1_GDB_PATH_NETWORK = os.path.join(
    DATA_IMPORT_HWYPROJ_CODING_DIR_PATH, "before_state1.gdb"
)
BEFORE_STATE_2_GDB_PATH_NETWORK = os.path.join(
    DATA_UPDATE_HWYPROJ_YEARS, "before_state2.gdb"
)
AFTER_STATE_1_GDB_PATH_NETWORK = os.path.join(
    DATA_INCORPORATE_EDITS_DIR_PATH, "after_state1.gdb"
)
AFTER_STATE_2_GDB_PATH_NETWORK = os.path.join(
    DATA_IMPORT_HWYPROJ_CODING_DIR_PATH, "after_state2.gdb"
)
AFTER_STATE_3_GDB_PATH_NETWORK = os.path.join(
    DATA_UPDATE_HWYPROJ_YEARS, "after_state3.gdb"
)
# list for convenience
NETWORK_TEST_GDBS = [
    BEFORE_STATE_0_GDB_PATH_NETWORK,
    BEFORE_STATE_1_GDB_PATH_NETWORK,
    BEFORE_STATE_2_GDB_PATH_NETWORK,
    AFTER_STATE_1_GDB_PATH_NETWORK,
    AFTER_STATE_2_GDB_PATH_NETWORK,
    AFTER_STATE_3_GDB_PATH_NETWORK,
]

# Local copies of network files (paths)
BEFORE_STATE_0_GDB_PATH_LOCAL = os.path.join(
    LOCAL_INCORPORATE_EDITS_DATA_DIR, "before_state0.gdb"
)
BEFORE_STATE_1_GDB_PATH_LOCAL = os.path.join(
    LOCAL_IMPORT_HWYCODING_DATA_DIR, "before_state1.gdb"
)
BEFORE_STATE_2_GDB_PATH_LOCAL = os.path.join(
    LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "before_state2.gdb"
)
AFTER_STATE_1_GDB_PATH_LOCAL = os.path.join(
    LOCAL_INCORPORATE_EDITS_DATA_DIR, "after_state1.gdb"
)
AFTER_STATE_2_GDB_PATH_LOCAL = os.path.join(
    LOCAL_IMPORT_HWYCODING_DATA_DIR, "after_state2.gdb"
)
AFTER_STATE_3_GDB_PATH_LOCAL = os.path.join(
    LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "after_state3.gdb"
)

LOCAL_TEST_GDBS = [
    BEFORE_STATE_0_GDB_PATH_LOCAL,
    BEFORE_STATE_1_GDB_PATH_LOCAL,
    BEFORE_STATE_2_GDB_PATH_LOCAL,
    AFTER_STATE_1_GDB_PATH_LOCAL,
    AFTER_STATE_2_GDB_PATH_LOCAL,
    AFTER_STATE_3_GDB_PATH_LOCAL,
]

# List of pairs for test gdbs (network, local)
TEST_GDB_PAIRS = [
    (BEFORE_STATE_0_GDB_PATH_NETWORK, BEFORE_STATE_0_GDB_PATH_LOCAL),
    (BEFORE_STATE_1_GDB_PATH_NETWORK, BEFORE_STATE_1_GDB_PATH_LOCAL),
    (BEFORE_STATE_2_GDB_PATH_NETWORK, BEFORE_STATE_2_GDB_PATH_LOCAL),
    (AFTER_STATE_1_GDB_PATH_NETWORK, AFTER_STATE_1_GDB_PATH_LOCAL),
    (AFTER_STATE_2_GDB_PATH_NETWORK, AFTER_STATE_2_GDB_PATH_LOCAL),
    (AFTER_STATE_3_GDB_PATH_NETWORK, AFTER_STATE_3_GDB_PATH_LOCAL),
]

# List of pairs for test input data!
TEST_INPUT_DATA_DIR_PAIRS = [
    (DATA_INCORPORATE_EDITS_DIR_PATH, LOCAL_INCORPORATE_EDITS_DATA_DIR),
    (DATA_IMPORT_HWYPROJ_CODING_DIR_PATH, LOCAL_IMPORT_HWYCODING_DATA_DIR),
    (DATA_UPDATE_HWYPROJ_YEARS, LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR),
    (DATA_OUTPUT_DIR_PATH, LOCAL_OUTPUT_DATA_DIR_PATH),
]

# OUTPUT GDBs!!
OUTPUT_AFTER_STATE_1_GDB_PATH = os.path.join(TEST_OUTPUT_DIR_PATH, "after_state1.gdb")
OUTPUT_AFTER_STATE_2_MFHRN = os.path.join(
    TEST_OUTPUT_DIR_PATH, "after_state2_mfhrn.gdb"
)
OUTPUT_AFTER_STATE_2_MHN = os.path.join(TEST_OUTPUT_DIR_PATH, "after_state2_mhn.gdb")

# Other files
TEST_INPUT_HWYPROJ_CODING_XLSX = os.path.join(
    LOCAL_IMPORT_HWYCODING_DATA_DIR, "import_proj_test"
)

# MHN Script paths
MHN_INCORPORATE_EDITS_PY_PATH = os.path.join(
    MHN_REPO_PATH, "src", "incorporate_edits.py"
)

# MFHRN Script paths
MFHRN_IMPORT_HWYPROJ_CODING_PY_PATH = os.path.join(
    PROJECT_ROOT, "scripts", "1_travel", "import_hwyproj_coding.py"
)

# Arc table FC paths
HWYPROJ_CODING_FC_PATH = os.path.join(INPUT_MHN_GDB_PATH, "hwyproj_coding")
HWYNET_ARC_FC_PATH = os.path.join(INPUT_MHN_GDB_PATH, "hwynet/hwynet_arc")

# Optional fields that Cindy assumed would be present
HWYPROJ_CODING_OPTIONAL_FIELDS = {
    "CHANGE_PARKRES1": ("SHORT", None, None, None),
    "CHANGE_PARKRES2": ("SHORT", None, None, None),
    "ADD_BUSLANES1": ("SHORT", None, None, 100),
    "ADD_BUSLANES2": ("SHORT", None, None, 100),
    "NEW_VCLEARANCE": (
        "DOUBLE",
        None,
        None,
        None,
    ),
}

HWYNET_ARC_OPTIONAL_FIELDS = {
    "BUSLANES1": ("SHORT", None, None, None),
    "BUSLANES2": ("SHORT", None, None, None),
    "PARKRES1": ("SHORT", None, None, 100),
    "PARKRES2": ("SHORT", None, None, 100),
    "VCLEARANCE": (
        "DOUBLE",
        None,
        None,
        None,
    ),
}


# SECTION: Functions
def fix_broken_hwynet_arc(gdb_to_fix, based_on_gdb):
    """
    Helper function: the provided 'before_state0.gdb' has an issue with `hwynet_arc` FC
    where link with OBJECTID=33122 is missing attributes. This function simply copies
    the correct attributes from 'after_state1.gdb' for that link (based on Tim's instructions)
    """
    incorrect_fc = os.path.join(gdb_to_fix, "hwynet", "hwynet_arc")
    correct_fc = os.path.join(based_on_gdb, "hwynet", "hwynet_arc")

    fields_to_change = [
        f.name for f in arcpy.ListFields(correct_fc) if f.type != "Geometry"
    ]

    broken_link_objectid = 33122
    row_in_correct_fc = None

    with arcpy.da.SearchCursor(
        correct_fc, fields_to_change, where_clause=f"OBJECTID = {broken_link_objectid}"
    ) as cursor:
        for row in cursor:
            row_in_correct_fc = row
            break

    if row_in_correct_fc is None:
        raise ValueError(
            f"Could not find OBJECTID {broken_link_objectid} in {correct_fc}"
        )

    with arcpy.da.UpdateCursor(
        incorrect_fc,
        fields_to_change,
        where_clause=f"OBJECTID = {broken_link_objectid}",
    ) as cursor:
        for row in cursor:
            for i in range(len(fields_to_change)):
                row[i] = row_in_correct_fc[i]
            cursor.updateRow(row)

    arcpy.management.ClearWorkspaceCache()
    print(f"Succesfully fixed 'hwynet_arc' in {gdb_to_fix}")


def copy_gdb(source_path, destination_path):
    """
    Helper function: Copies the base MHN from its location on the network storage to the given
    destination
    """
    if arcpy.Exists(str(destination_path)):
        arcpy.management.Delete(str(destination_path))

    arcpy.management.Copy(
        str(source_path),
        str(destination_path),
    )


# NOTE: AR: The MHN copy (in the base GDB as well as in the before_... and after_... GDBs)
# has duplicate rows with the TIPIDS in `DUPLICATE_TIPIDS`, so need to remove them to make
# sure Cindy's scripts don't error.
def remove_duplicate_tipids(mhn_gdb_path):
    """
    Helper function: removes duplicate rows from MHN and adds expected rows

    """
    workspace = mhn_gdb_path
    hwyproj_fc_path = mhn_gdb_path / "hwynet" / "hwyproj"
    seen_tipids = set()

    with arcpy.da.Editor(workspace) as edit:
        with arcpy.da.UpdateCursor(str(hwyproj_fc_path), ["TIPID"]) as cursor:
            for row in cursor:
                tipid = str(row[0])

                if tipid not in DUPLICATE_TIPIDS:
                    continue

                if tipid in seen_tipids:
                    cursor.deleteRow()
                else:
                    seen_tipids.add(tipid)


def ensure_optional_fields(fc_path, optional_fields):
    """
    Helper function: ensures optional fields exist in the feature class
    """

    existing = {f.name.upper() for f in arcpy.ListFields(fc_path)}
    for name, (ftype, prec, scale, length) in optional_fields.items():
        if name not in existing:
            arcpy.management.AddField(
                in_table=fc_path,
                field_name=name,
                field_type=ftype,
                field_precision=prec,
                field_scale=scale,
                field_length=length,
            )
            print(f"Added missing optional field: {name} ({ftype})")


def copy_network_input_data():
    """
    Helper function: makes sure that pipeline test has access to necessary
    GDBs by copying them from network drive if necessary.
    """
    print("Ensuring that all input test files for pipeline test are in correct place")
    for network_test_dir, local_test_dir in TEST_INPUT_DATA_DIR_PAIRS:
        if not Path(network_test_dir).is_dir() and not Path(local_test_dir).is_dir():
            raise FileNotFoundError(
                f"Network MHN not found at {network_test_dir}"
                " You might not be connected to CMAP's network!"
            )
        elif not Path(local_test_dir).is_dir():
            print(f"Copying {network_test_dir} to {local_test_dir}")
            shutil.copytree(network_test_dir, local_test_dir, dirs_exist_ok=True)
            print(f"Successfully copied {network_test_dir} to {local_test_dir}")
        else:
            print(
                f"Local copy of {network_test_dir} already exists at {local_test_dir}"
            )


def check_gdb_equality(gdb_1, gdb_2):
    """
    Function to check whether two Geodatabases are equivalent!
    """
    print(f"Checking {gdb_1} and {gdb_2} for equality")
    arcpy.env.overwriteOutput = True

    arcpy.env.workspace = gdb_1
    gdb_1_fcs = set(arcpy.ListFeatureClasses() or [])
    gdb_1_tables = set(arcpy.ListTables() or [])

    arcpy.env.workspace = gdb_2
    gdb_2_fcs = set(arcpy.ListFeatureClasses() or [])
    gdb_2_tables = set(arcpy.ListTables() or [])

    if gdb_2_fcs != gdb_1_fcs or gdb_2_tables != gdb_1_tables:
        print("GDBs are not equal")
        print(f"Missing feature classes in {gdb_1}: {gdb_2_fcs - gdb_1_fcs}")
        print(f"Missing feature classes in {gdb_2}: {gdb_1_fcs - gdb_2_fcs}")
        return False

    # since both have same FCs, can iterate through both
    all_equal = True
    for fc in gdb_1_fcs:
        gdb_1_fc = os.path.join(gdb_1, fc)
        gdb_2_fc = os.path.join(gdb_2, fc)

        result = arcpy.management.FeatureCompare(
            in_base_features=gdb_1_fc,
            in_test_features=gdb_2_fc,
            sort_field="OBJECTID",
            compare_type="ALL",
            continue_compare="CONTINUE_COMPARE",
        )

        print(f"Result: {result[1]}")
        if result[1] == "false":
            print(f"Differences found in fc {fc}")
            all_equal = False
        else:
            print(f"Feature Class {fc} matches")

    for table in gdb_1_tables:
        gdb_1_table = os.path.join(gdb_1, table)
        gdb_2_table = os.path.join(gdb_2, table)

        result = arcpy.management.TableCompare(
            in_base_table=gdb_1_table,
            in_test_table=gdb_2_table,
            sort_field="OBJECTID",
            compare_type="ALL",
            continue_compare="CONTINUE_COMPARE",
        )

        if result[1] == "false":
            print(f"Table: {table}")
            all_equal = False
        else:
            print(f"Table {table} matches")

    return all_equal


def incorporate_edits():
    """
    Helper function: runs incorporate_edits tool
    """
    # need to temporarily back up the original 'before_state0.gdb' because
    # current MHN tool overwrites it
    temp_back_up_dir = os.path.join(TEST_OUTPUT_DIR_PATH, "temp")
    os.makedirs(temp_back_up_dir, exist_ok=True)
    temp_back_up_before_state_0_gdb_path = os.path.join(
        temp_back_up_dir, "before_state0.gdb"
    )
    copy_gdb(BEFORE_STATE_0_GDB_PATH_LOCAL, temp_back_up_before_state_0_gdb_path)

    # now fix broken input gdb
    fix_broken_hwynet_arc(
        gdb_to_fix=BEFORE_STATE_0_GDB_PATH_LOCAL,
        based_on_gdb=AFTER_STATE_1_GDB_PATH_LOCAL,
    )

    # now can run command
    command = [
        ARCPY_PYTHON_PATH,
        MHN_INCORPORATE_EDITS_PY_PATH,
        BEFORE_STATE_0_GDB_PATH_LOCAL,
    ]
    command_result = subprocess.run(command, check=True).stdout
    print(command_result)

    # once command has run, check for equality, and move backup back to correct location
    # as well as outputted GDB to outputs
    copy_gdb(
        source_path=BEFORE_STATE_0_GDB_PATH_LOCAL,
        destination_path=OUTPUT_AFTER_STATE_1_GDB_PATH,
    )

    # delete and refresh from backup
    shutil.rmtree(BEFORE_STATE_0_GDB_PATH_LOCAL)
    copy_gdb(
        source_path=temp_back_up_before_state_0_gdb_path,
        destination_path=BEFORE_STATE_0_GDB_PATH_LOCAL,
    )
    shutil.rmtree(temp_back_up_before_state_0_gdb_path)

    assert check_gdb_equality(
        gdb_1=AFTER_STATE_1_GDB_PATH_LOCAL, gdb_2=OUTPUT_AFTER_STATE_1_GDB_PATH
    )
    arcpy.management.ClearWorkspaceCache()
    print("`incorporate_edits` tool passed")


def test_mhn_import_hwyproj_coding():
    pass


def test_mfhrn_import_hwyproj_coding():
    """
    Tests that the MFHRN `import_hwyproj_coding` tool produces
    'after_state2.gdb' from 'before_state1.gdb' correctly!
    """
    # first need to clean input dir
    shutil.rmtree(INPUT_MHN_GDB_PATH, ignore_errors=True)
    if not Path(OUTPUT_AFTER_STATE_1_GDB_PATH):
        raise Exception(
            "You must first call the incorporate_edits function in the pipeline test!"
        )
    copy_gdb(
        source_path=OUTPUT_AFTER_STATE_1_GDB_PATH, destination_path=INPUT_MHN_GDB_PATH
    )

    # now can copy over the xlsx to the correct place
    shutil.copy(src=TEST_INPUT_HWYPROJ_CODING_XLSX, dst=INPUT_HWYPROJ_CODING_XLSX)

    # and now run the whole tool
    command = [ARCPY_PYTHON_PATH, MFHRN_IMPORT_HWYPROJ_CODING_PY_PATH]
    command_result = subprocess.run(command, check=True).stdout
    print(command_result)


# SECTION: Main function


def main():
    print("Running full MHN + MFHRN pipeline test")
    copy_network_input_data()

    print("Running incorporate_edits from MHN")
    print("-" * 80)
    try:
        incorporate_edits()
    except Exception as e:
        print(f"Incorporate edits (from MHN) failed: {e}")
    else:
        print("Incorporate edits succeeded")

    # NOTE: AR: Because of the stupid arcpy locks, need to wait here untils locks
    # release fully
    time.sleep(15)
    print("Running `import_hwyproj_coding` from MFHRN")
    print("-" * 80)
    try:
        test_mfhrn_import_hwyproj_coding()
    except Exception as e:
        print(f"`import_hwyproj_coding` (from MFHRN) failed: {e}")
    else:
        print("`import_hwyproj_coding` (from MFHRN) succeeded")


if __name__ == "__main__":
    main()
