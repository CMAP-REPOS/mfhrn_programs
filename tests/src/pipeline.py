"""
This script prepares the base MHN geodatabase used by the MFHRN tests
and runs the full MHN + MFHRN equivalence pipeline.

Author: Aaron Rumph
Updated: 07/31/2026
"""

# SECTION: External dependencies

import os
import shutil
import subprocess
from pathlib import Path
import arcpy

# SECTION: Internal dependencies
from _testing_config import ARCPY_ENV_PATH, MHN_REPO_PATH


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
MRN_GDB_PATH_NETWORK = r"M:\adb\MRN\dev\mrn.gdb"

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
LOCAL_MRN_GDB_PATH = os.path.join(LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "mrn.gdb")

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
OUTPUT_AFTER_STATE_1_GDB_PATH = os.path.join(
    LOCAL_INCORPORATE_EDITS_DATA_DIR, "output_after_state1.gdb"
)
OUTPUT_AFTER_STATE_2_MFHRN = os.path.join(
    TEST_OUTPUT_DIR_PATH, "after_state2_mfhrn.gdb"
)
OUTPUT_AFTER_STATE_2_MHN = os.path.join(TEST_OUTPUT_DIR_PATH, "after_state2_mhn.gdb")
OUTPUT_AFTER_STATE_3_GDB_PATH = os.path.join(TEST_OUTPUT_DIR_PATH, "after_state3.gdb")
MFHRN_TRAVEL_OUTPUT_DIR_PATH = os.path.join(PROJECT_ROOT, "output", "1_travel")

# Other files
TEST_INPUT_HWYPROJ_CODING_XLSX = os.path.join(
    LOCAL_IMPORT_HWYCODING_DATA_DIR, "import_proj_test.xlsx"
)
TEST_INPUT_YEAR_CSV = os.path.join(
    LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "year_c26q2.csv"
)
TEST_INPUT_REQUIRED_CSV = os.path.join(
    LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "required_c26q2.csv"
)
TEST_INPUT_NOCODE_CSV = os.path.join(
    LOCAL_UPDATE_HWYPROJ_YEARS_DATA_DIR, "no_code_c26q2"
)

# MHN Script paths
MHN_INCORPORATE_EDITS_PY_PATH = os.path.join(
    MHN_REPO_PATH, "src", "incorporate_edits.py"
)
MHN_UPDATE_HWYPROJ_YEARS_PY_PATH = os.path.join(
    MHN_REPO_PATH, "src", "update_highway_project_years.py"
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
def clear_workspace_cache(*workspace_paths):
    """
    Helper function: clears this process's ArcPy references to the given workspaces
    """
    # need to unset this before clearing the cached workspaces
    arcpy.env.workspace = None

    for workspace_path in workspace_paths:
        if workspace_path and arcpy.Exists(str(workspace_path)):
            arcpy.management.ClearWorkspaceCache(str(workspace_path))

    # also clear any other workspaces cached by geoprocessing tools
    arcpy.management.ClearWorkspaceCache()


def fix_broken_hwynet_arc(gdb_to_fix, based_on_gdb):
    """
    Helper function: the provided 'before_state0.gdb' has an issue with `hwynet_arc` FC
    where link with OBJECTID=33122 is missing attributes. This function simply copies
    the correct attributes from 'after_state1.gdb' for that link (based on Tim's instructions)
    """
    incorrect_fc = os.path.join(gdb_to_fix, "hwynet", "hwynet_arc")
    correct_fc = os.path.join(based_on_gdb, "hwynet", "hwynet_arc")

    fields_to_change = [
        field.name
        for field in arcpy.ListFields(correct_fc)
        if field.editable and field.type != "Geometry"
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

    clear_workspace_cache(gdb_to_fix, based_on_gdb)
    print(f"Succesfully fixed 'hwynet_arc' in {gdb_to_fix}")


def copy_gdb(source_path, destination_path):
    """
    Helper function: Copies the base MHN from its location on the network storage to the given
    destination
    """
    source_path = str(source_path)
    destination_path = str(destination_path)

    try:
        if not arcpy.Exists(source_path):
            raise FileNotFoundError(f"Geodatabase not found: {source_path}")

        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        # clear the cache before trying to delete an existing output GDB
        clear_workspace_cache(source_path, destination_path)

        if arcpy.Exists(destination_path):
            arcpy.management.Delete(destination_path)

        arcpy.management.Copy(source_path, destination_path)
    finally:
        # make sure a failed copy does not leave the GDB cached for the next test
        clear_workspace_cache(source_path, destination_path)


# NOTE: AR: The MHN copy (in the base GDB as well as in the before_... and after_... GDBs)
# has duplicate rows with the TIPIDS in `DUPLICATE_TIPIDS`, so need to remove them to make
# sure Cindy's scripts don't error.
def remove_duplicate_tipids(mhn_gdb_path):
    """
    Helper function: removes duplicate rows from MHN and adds expected rows

    """
    workspace = str(mhn_gdb_path)
    hwyproj_fc_path = mhn_gdb_path / "hwynet" / "hwyproj"
    seen_tipids = set()

    with arcpy.da.Editor(workspace):
        with arcpy.da.UpdateCursor(str(hwyproj_fc_path), ["TIPID"]) as cursor:
            for row in cursor:
                tipid = str(row[0])

                if tipid not in DUPLICATE_TIPIDS:
                    continue

                if tipid in seen_tipids:
                    cursor.deleteRow()
                else:
                    seen_tipids.add(tipid)

    clear_workspace_cache(mhn_gdb_path)


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
        elif Path(network_test_dir).is_dir():
            print(f"Copying {network_test_dir} to {local_test_dir}")
            shutil.copytree(
                network_test_dir,
                local_test_dir,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns("*.lock"),
            )
            print(f"Successfully copied {network_test_dir} to {local_test_dir}")
        else:
            print(
                f"Local copy of {network_test_dir} already exists at {local_test_dir}"
            )

    # now copy the GDBs that were skipped above
    for network_gdb, local_gdb in TEST_GDB_PAIRS:
        if not Path(local_gdb).is_dir():
            print(f"Copying {network_gdb} to {local_gdb}")
            copy_gdb(network_gdb, local_gdb)

    # update_highway_project_years also needs a local copy of the MRN
    if not Path(LOCAL_MRN_GDB_PATH).is_dir():
        if not Path(MRN_GDB_PATH_NETWORK).is_dir():
            raise FileNotFoundError(
                f"MRN not found at {MRN_GDB_PATH_NETWORK}. You might not be "
                "connected to CMAP's network!"
            )
        print(f"Copying {MRN_GDB_PATH_NETWORK} to {LOCAL_MRN_GDB_PATH}")
        copy_gdb(MRN_GDB_PATH_NETWORK, LOCAL_MRN_GDB_PATH)


def _list_comparable_gdb_items(gdb_path):
    """
    Helper function: gets the relative path and data type for every table and FC
    in a GDB (including the FCs inside feature datasets like `hwynet`)
    """
    items = {}

    for directory, _, names in arcpy.da.Walk(
        gdb_path, datatype=["FeatureClass", "Table"]
    ):
        for name in names:
            item_path = os.path.join(directory, name)
            relative_path = os.path.relpath(item_path, gdb_path).replace("\\", "/")
            description = arcpy.Describe(item_path)
            items[relative_path] = (item_path, description.dataType)

    return items


def check_gdb_equality(gdb_1, gdb_2):
    """
    Function to check whether two Geodatabases are equivalent!
    """
    print(f"Checking {gdb_1} and {gdb_2} for equality")
    arcpy.env.overwriteOutput = True

    try:
        gdb_1_items = _list_comparable_gdb_items(gdb_1)
        gdb_2_items = _list_comparable_gdb_items(gdb_2)
        gdb_1_names = set(gdb_1_items)
        gdb_2_names = set(gdb_2_items)

        if gdb_1_names != gdb_2_names:
            print("GDBs do not contain the same tables and feature classes")
            print(f"Missing from {gdb_1}: {sorted(gdb_2_names - gdb_1_names)}")
            print(f"Missing from {gdb_2}: {sorted(gdb_1_names - gdb_2_names)}")
            return False

        # since both have the same FCs/tables, can iterate through both
        all_equal = True
        for relative_path in sorted(gdb_1_names):
            gdb_1_item, gdb_1_type = gdb_1_items[relative_path]
            gdb_2_item, gdb_2_type = gdb_2_items[relative_path]

            if gdb_1_type != gdb_2_type:
                print(
                    f"{relative_path} has different data types: "
                    f"{gdb_1_type} != {gdb_2_type}"
                )
                all_equal = False
                continue

            sort_field = arcpy.Describe(gdb_1_item).OIDFieldName
            if gdb_1_type == "FeatureClass":
                result = arcpy.management.FeatureCompare(
                    in_base_features=gdb_1_item,
                    in_test_features=gdb_2_item,
                    sort_field=sort_field,
                    compare_type="ALL",
                    ignore_options=["IGNORE_RELATIONSHIPCLASSES"],
                    continue_compare="CONTINUE_COMPARE",
                )
            else:
                result = arcpy.management.TableCompare(
                    in_base_table=gdb_1_item,
                    in_test_table=gdb_2_item,
                    sort_field=sort_field,
                    compare_type="ALL",
                    ignore_options=["IGNORE_RELATIONSHIPCLASSES"],
                    continue_compare="CONTINUE_COMPARE",
                )

            items_match = str(result.getOutput(1)).lower() == "true"
            if items_match:
                print(f"{relative_path} matches")
            else:
                print(f"Differences found in {relative_path}")
                print(result.getMessages())
                all_equal = False
            del result

        return all_equal
    finally:
        clear_workspace_cache(gdb_1, gdb_2)


def _prepare_mfhrn_import_gdb(gdb_path):
    """
    Helper function: applies the same compatibility fixes to the input GDB and
    the copy of the expected output GDB
    """
    # both provided GDBs have the duplicate TIPIDs noted above
    remove_duplicate_tipids(Path(gdb_path))

    # Cindy's tool assumes these optional fields are present
    ensure_optional_fields(
        os.path.join(gdb_path, "hwyproj_coding"),
        HWYPROJ_CODING_OPTIONAL_FIELDS,
    )
    ensure_optional_fields(
        os.path.join(gdb_path, "hwynet", "hwynet_arc"),
        HWYNET_ARC_OPTIONAL_FIELDS,
    )
    clear_workspace_cache(gdb_path)


def _find_mfhrn_output_gdb():
    """
    Helper function: finds the output GDB from import_hwyproj_coding
    (the actual GDB name depends on the base year)
    """
    output_gdbs = sorted(Path(MFHRN_TRAVEL_OUTPUT_DIR_PATH).glob("MHN_*.gdb"))
    output_gdbs = [path for path in output_gdbs if path.is_dir()]

    if len(output_gdbs) != 1:
        raise RuntimeError(
            "Expected import_hwyproj_coding to create exactly one MHN_*.gdb in "
            f"{MFHRN_TRAVEL_OUTPUT_DIR_PATH}; found {len(output_gdbs)}"
        )

    return output_gdbs[0]


def incorporate_edits():
    """
    Helper function: runs incorporate_edits tool
    """
    # need to copy the original 'before_state0.gdb' because the current MHN tool
    # overwrites its input; this keeps the local test input unchanged
    copy_gdb(BEFORE_STATE_0_GDB_PATH_LOCAL, OUTPUT_AFTER_STATE_1_GDB_PATH)

    try:
        # now fix broken output gdb
        fix_broken_hwynet_arc(
            gdb_to_fix=OUTPUT_AFTER_STATE_1_GDB_PATH,
            based_on_gdb=AFTER_STATE_1_GDB_PATH_LOCAL,
        )

        # now can run command
        command = [
            ARCPY_PYTHON_PATH,
            MHN_INCORPORATE_EDITS_PY_PATH,
            OUTPUT_AFTER_STATE_1_GDB_PATH,
        ]
        subprocess.run(command, check=True)

        # once command has run, check for equality with 'after_state1.gdb'
        if not check_gdb_equality(
            gdb_1=AFTER_STATE_1_GDB_PATH_LOCAL,
            gdb_2=OUTPUT_AFTER_STATE_1_GDB_PATH,
        ):
            raise AssertionError(
                "incorporate_edits output does not match after_state1.gdb"
            )
        print("`incorporate_edits` tool passed")
    finally:
        # always clear these workspaces, including when the MHN tool errors
        clear_workspace_cache(
            BEFORE_STATE_0_GDB_PATH_LOCAL,
            AFTER_STATE_1_GDB_PATH_LOCAL,
            OUTPUT_AFTER_STATE_1_GDB_PATH,
        )


def test_mfhrn_import_hwyproj_coding():
    """
    Tests that the MFHRN `import_hwyproj_coding` tool produces
    'after_state2.gdb' from 'before_state1.gdb' correctly!
    """
    try:
        # first need to clean input dir and copy over 'before_state1.gdb'
        copy_gdb(
            source_path=BEFORE_STATE_1_GDB_PATH_LOCAL,
            destination_path=INPUT_MHN_GDB_PATH,
        )
        # make a copy of the expected MHN output so the original stays unchanged
        copy_gdb(AFTER_STATE_2_GDB_PATH_LOCAL, OUTPUT_AFTER_STATE_2_MHN)

        # apply the same required fixes to the test input and expected output
        _prepare_mfhrn_import_gdb(INPUT_MHN_GDB_PATH)
        _prepare_mfhrn_import_gdb(OUTPUT_AFTER_STATE_2_MHN)

        # now can copy over the xlsx to the correct place
        shutil.copy(
            src=TEST_INPUT_HWYPROJ_CODING_XLSX,
            dst=INPUT_HWYPROJ_CODING_XLSX,
        )

        # and now run the whole tool
        command = [ARCPY_PYTHON_PATH, MFHRN_IMPORT_HWYPROJ_CODING_PY_PATH]
        subprocess.run(command, check=True)

        # copy the MFHRN result into test outputs and compare it with after_state2
        copy_gdb(_find_mfhrn_output_gdb(), OUTPUT_AFTER_STATE_2_MFHRN)
        if not check_gdb_equality(
            gdb_1=OUTPUT_AFTER_STATE_2_MHN,
            gdb_2=OUTPUT_AFTER_STATE_2_MFHRN,
        ):
            raise AssertionError(
                "import_hwyproj_coding output does not match after_state2.gdb"
            )
        print("`import_hwyproj_coding` tool passed")
    finally:
        # always clear these workspaces so the next pipeline test can run
        clear_workspace_cache(
            BEFORE_STATE_1_GDB_PATH_LOCAL,
            AFTER_STATE_2_GDB_PATH_LOCAL,
            INPUT_MHN_GDB_PATH,
            OUTPUT_AFTER_STATE_2_MHN,
            OUTPUT_AFTER_STATE_2_MFHRN,
        )


def test_mhn_update_hwyproj_years():
    """
    Tests that the MHN `update_highway_project_years` tool produces
    'after_state3.gdb' from 'before_state2.gdb' correctly!
    """
    try:
        # first need to copy the before state because the MHN tool edits it in place
        copy_gdb(BEFORE_STATE_2_GDB_PATH_LOCAL, OUTPUT_AFTER_STATE_3_GDB_PATH)

        # now can run the whole tool with the three CSVs and local MRN copy
        command = [
            ARCPY_PYTHON_PATH,
            MHN_UPDATE_HWYPROJ_YEARS_PY_PATH,
            OUTPUT_AFTER_STATE_3_GDB_PATH,
            LOCAL_MRN_GDB_PATH,
            TEST_INPUT_YEAR_CSV,
            TEST_INPUT_REQUIRED_CSV,
            TEST_INPUT_NOCODE_CSV,
        ]
        subprocess.run(command, check=True)

        # once command has run, check for equality with 'after_state3.gdb'
        if not check_gdb_equality(
            gdb_1=AFTER_STATE_3_GDB_PATH_LOCAL,
            gdb_2=OUTPUT_AFTER_STATE_3_GDB_PATH,
        ):
            raise AssertionError(
                "update_highway_project_years output does not match after_state3.gdb"
            )
        print("`update_highway_project_years` tool passed")
    finally:
        # always clear these workspaces, including when the MHN tool errors
        clear_workspace_cache(
            BEFORE_STATE_2_GDB_PATH_LOCAL,
            AFTER_STATE_3_GDB_PATH_LOCAL,
            OUTPUT_AFTER_STATE_3_GDB_PATH,
            LOCAL_MRN_GDB_PATH,
        )


# SECTION: Main function


def main():
    print("Running full MHN + MFHRN pipeline test")
    copy_network_input_data()

    # NOTE: AR: Because of the stupid arcpy locks, this used to wait 15 seconds
    # here until the locks released fully. That should not be necessary now because
    # each tool starts with its own BEFORE_STATE copy and clears its cached GDBs.
    # This also lets the later tests run if an earlier test fails.
    stages = [
        ("incorporate_edits (MHN)", incorporate_edits),
        ("import_hwyproj_coding (MFHRN)", test_mfhrn_import_hwyproj_coding),
        ("update_highway_project_years (MHN)", test_mhn_update_hwyproj_years),
    ]
    failures = []

    # run all the test functions
    for label, stage in stages:
        print(f"Running `{label}`")
        print("-" * 80)
        try:
            stage()
        except Exception as error:
            failures.append((label, error))
            print(f"`{label}` failed: {error}")
        else:
            print(f"`{label}` succeeded")

    # still fail the full pipeline test if any individual tool failed
    if failures:
        details = "\n".join(f"- {label}: {error}" for label, error in failures)
        raise RuntimeError(f"Pipeline equivalence test failed:\n{details}")


if __name__ == "__main__":
    main()
