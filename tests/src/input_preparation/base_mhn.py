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

# SECTION: Internal dependencies


# SECTION: Constants

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SOURCE_MHN_GDB_PATH = Path(r"M:\proj1\tko\to_aaronrumph\MasterHighway\mhn_c26q2.gdb")
TEST_MHN_GDB_PATH = PROJECT_ROOT / "tests" / "inputs" / "BASE_MHN.gdb"
INPUT_MHN_GDB_PATH = PROJECT_ROOT / "input" / "1_travel" / "MHN.gdb"
TARGET_MHN_GDB_PATHS = (TEST_MHN_GDB_PATH, INPUT_MHN_GDB_PATH)
DUPLICATE_TIPIDS = {"10000115", "10030008"}
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


def copy_base_mhn(source_path, destination_path):
    """
    Helper function: Copies the base MHN from its location on the network storage to the given
    destination
    """
    destination_path.parent.mkdir(parents=True, exist_ok=True)

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


# SECTION: Main function


def main():
    if not arcpy.Exists(str(SOURCE_MHN_GDB_PATH)) and not arcpy.Exists(
        str(TEST_MHN_GDB_PATH)
    ):
        raise FileNotFoundError(
            f"Base MHN not found at {SOURCE_MHN_GDB_PATH}"
            " You might not be connected to CMAP's network!"
        )

    # copies the base MHN to both 'input/1_travel/MHN.gdb' and
    # 'tests/inputs/BASE_MHN.gdb' and removes any duplicates
    # (if already have BASE_MHN in tests/inputs then will copy
    # locally to avoid slow network copy)

    if not arcpy.Exists(str(INPUT_MHN_GDB_PATH)) and arcpy.Exists(
        str(TEST_MHN_GDB_PATH)
    ):
        copy_base_mhn(
            source_path=TEST_MHN_GDB_PATH, destination_path=INPUT_MHN_GDB_PATH
        )

    elif not arcpy.Exists(str(INPUT_MHN_GDB_PATH)):
        for destination_path in TARGET_MHN_GDB_PATHS:
            copy_base_mhn(destination_path)
            remove_duplicate_tipids(destination_path)
            print(f"Prepared base MHN at {destination_path}")

    ensure_optional_fields(HWYPROJ_CODING_FC_PATH, HWYPROJ_CODING_OPTIONAL_FIELDS)
    ensure_optional_fields(HWYNET_ARC_FC_PATH, HWYNET_ARC_OPTIONAL_FIELDS)


if __name__ == "__main__":
    main()
