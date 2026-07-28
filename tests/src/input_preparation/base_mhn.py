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

# SECTION: Internal dependencies


# SECTION: Constants

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SOURCE_MHN_GDB_PATH = Path(r"M:\proj1\tko\to_aaronrumph\MasterHighway\mhn_c26q2.gdb")
TEST_MHN_GDB_PATH = PROJECT_ROOT / "tests" / "inputs" / "BASE_MHN.gdb"
INPUT_MHN_GDB_PATH = PROJECT_ROOT / "input" / "1_travel" / "MHN.gdb"
TARGET_MHN_GDB_PATHS = (TEST_MHN_GDB_PATH, INPUT_MHN_GDB_PATH)
DUPLICATE_TIPIDS = {"10000115", "10030008"}


# SECTION: Functions


def copy_base_mhn(destination_path):
    """
    Helper function: Copies the base MHN from its location on the network storage to the given
    destination
    """
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    if arcpy.Exists(str(destination_path)):
        arcpy.management.Delete(str(destination_path))

    arcpy.management.Copy(
        str(SOURCE_MHN_GDB_PATH),
        str(destination_path),
    )


# NOTE: AR: The MHN copy (in the base GDB as well as in the before_... and after_... GDBs)
# has duplicate rows with the TIPIDS in `DUPLICATE_TIPIDS`, so need to remove them to make
# sure Cindy's scripts don't error.
def remove_duplicate_tipids(mhn_gdb_path):
    """
    Helper function: removes duplicate rows from MHN
    """
    hwyproj_fc_path = mhn_gdb_path / "hwynet" / "hwyproj"
    seen_tipids = set()

    with arcpy.da.UpdateCursor(str(hwyproj_fc_path), ["TIPID"]) as cursor:
        for row in cursor:
            tipid = str(row[0])

            if tipid not in DUPLICATE_TIPIDS:
                continue

            if tipid in seen_tipids:
                cursor.deleteRow()
            else:
                seen_tipids.add(tipid)


# SECTION: Main function


def main():
    if not arcpy.Exists(str(SOURCE_MHN_GDB_PATH)):
        raise FileNotFoundError(
            f"Base MHN not found at {SOURCE_MHN_GDB_PATH}"
            " You might not be connected to CMAP's network!"
        )

    # copies the base MHN to both 'input/1_travel/MHN.gdb' and
    # 'tests/inputs/BASE_MHN.gdb' and removes any duplicates
    for destination_path in TARGET_MHN_GDB_PATHS:
        copy_base_mhn(destination_path)
        remove_duplicate_tipids(destination_path)
        print(f"Prepared base MHN at {destination_path}")


if __name__ == "__main__":
    main()
