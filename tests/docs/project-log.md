# Project Log:
-----
NOTE: You can always find my notes in the code by running:
```shell
$ rg "#\s*NOTE[s]?:" -A5

$ # if you want more context you can run:
$ rg "#\s*NOTE[s]?:" -A15 -B5
```

## 07/07/2026:
----
Trying to get the first tool (export_future_hwys) working but it calls into `scripts/1_travel/modules/HN.py`.
The HN class constructor does a bunch of checks to make sure that the provided MHN gdb is actually valid.
Some of these checks, however, do not work, whether because of logic bugs, or because of assumptions about 
the data (that are incorrect). 

In particular, the following do not work:
- Check that PARKRES1 & 2 are 0 if one-way road (but this is logic error)
- Check that certain fields are null (assumes null fields will use "-" instead of " ", "-", or "")
- ditto as above but for a number of fields (`$ rg "all_fields2" -A10`)

Also some fields are missing from the copy of the MHN that I have:
- BUSLANES1 & BUSLANES2 (`$ rg "BUSLANES1 -B5 -A5`)
    - add fields to 'hwynet_arc'
- CHANGE_PARKRES1 & CHANGE_PARKRES2 (`$ rg CHANGE_PARKRES" -B10 -A10"`)
    - add fields to 'hwyproj_coding'
- ADD_BUSLANES1 & ADD_BUSLANES2
    - add fields to 'hwyproj_coding'
- NEW_VCLEARANCE
    - add field to 'hywproj_coding'

### TODOS: 
- Creating combined gdb... Building highway network for 2019...
    Traceback (most recent call last): File 
    "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\1_export_future_hwys.py", 
    line 36, in <module> HN.build_future_hwys() File 
    "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\modules\HN.py", 
    line 1233, in build_future_hwys self.hwy_forward_one_year() 
    File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\modules\HN.py",
    line 1898, in hwy_forward_one_year max((row[buslanes1_pos] + abuslanes1), 0), 1 
    TypeError: unsupported operand type(s) for +: 'NoneType' and 'NoneType'


## 07/09/2026:
----
Spent the day debugging the rest of the code for the HN class,
specifically the methods that are called in the course of running
the first tool. Finally got it working by fixing some problems
related to the BUSLANES1 and BUSLANES2 fields. Specifically,
the above error message (07/07/2026: TODOS) comes from Cindi 
assuming that the buslanes fields would be filled out, so 
`row[buslanes1_pos]` would not be None. Fixed by changing to 
the following instead:

```python

max(
    (row[buslanes1_pos] + abuslanes1
    if row[buslanes1_pos] is not None
    and abuslanes1 is not None
    else 0
    ), 
    0,
    
)
```

After making that change anywhere in the code where
assumes that BUSLANES1 or BUSLANES2 is necessary, I was
finally able to get the whole first tool script to run succesfully!!

## 07/10/2026:
----
Spent the day looking over the MHN code so that I can map
the functionality of one tool to that of another (for equivalence)
testing. Also spent a decent amount of time working on testing
harness/setup/env, but had to do trainings and had shorter day 
so did not make as much progress as would have liked.

## 07/14/2026:
----
Started the day by checking to make sure that tool 2
(`generate_hwy_files.py`) works. Ran succesfully, and seemed 
to create all the necessary files, will confirm with Tim.
Only required one small change:
There is an access to `parkres` that fails if parkres' values are
null. Added a None check:
```python

if parkres is not None:
    ...
```
To find: `$ rg "if parkres is not None" -A5`


Next, testing `create_bus_layers` tool. Tool takes forever to run
(14:30 mins) so a bit painful. Similarly had to add None check 
for attempt to access value of `parkres`:
```python

if parkres is not None:
    ...
```
To find: `$ rg "if parkres is not None" -A5`. Seemed to run 
successfully, but will check files in ArcGIS to check that they 
look correct.

## 07/15/2026:
----
Working on testing the `import_hwyproj_coding` tool. Need 
"import_hwyproj_coding.xlsx" file in inputs folder at:
`PROJECT_ROOT/input/1_travel/import_hwyproj_coding.xlsx`. 
Tried to run tool on MHN provided by Tim, had to run 
`export_future_hwys` tool before running 
`import_hwyproj_coding`. Running with provided MHN gave 
same errors as initial MHN attempt, so had to add same fields
as in [07/07/2026](project-log#07/07/2026:). Once did that
tool ran but stopped with duplicate ABB pairs?

Error message is line 612 below, and can see where 'duplicate_records'
is checked

``` python
594-        # check where tipid-abb is not unique
595-        import_df["abb"] = import_df.apply(
596-            lambda x: abb_dict[(x["anode"], x["bnode"])]["ABB"], axis=1
597-        )
598-        import_records = import_df.to_dict("records")
599-
600-        tipid_abb_series = import_df.groupby(["tipid", "abb"]).size()
601-        duplicate_combos = tipid_abb_series[tipid_abb_series > 1].to_dict()
602-
603-        duplicate_records = []
604-
605-        for row in import_records:
606-            if (row["tipid"], row["abb"]) in duplicate_combos:
607-                duplicate_records.append(row)
608-
609-        if len(duplicate_records) > 0:
610-            duplicate_records_df = pd.DataFrame(duplicate_records)
611-            duplicate_records_df.to_csv(import_errors_csv, index=False)
612:            sys.exit("Rows detected where TIPID - ABB is not unique. Crashing program.")
```

Duplicate combo: 

*{(TIPID, ABB): Occurances}*
{(12345678, '18781-18785-0'): 2}

## 07/16/2026:
----
Short day because of doctors appointment, spent day working mostly on
figuring out what was causing the above bug, and I believe it's because
of a logic error in the code above that doesn't take TOD into 
account when deciding whether two links are equivalent (ABB and TIP
taken into account, but not TOD). As such, not sure whether can test
with existing data or will need to change input data to avoid issue
altogether.

## 07/17/2026: 
----
N/A, spent day introducing Tyler to cmaputils and MPM stuff, as well
as working on intern presentation

## 07/21/2026:
----
Spent basically the whole time working on environment issues and arcpy
issues!!


## 07/22/2026:
----
Been working on getting the `import_hwyproj_coding` tool working.
Ran into problems with faulty check to make sure that links are 
unique. Also ran into problems with assumption that 'remove' would be
in 'import_hwyproj_coding.xlsx' cols. To fix, added simple None guard 
(see below for my notes):

scripts/1_travel/modules/HN.py
----
```python
594:        # NOTE: AR: The below code that checks whether ABB and TIPID are
595-        # unique had to be changed because it would fail when the two links
596-        # are different TODs. In the future, a check should be added
597-        # to make sure that all TODs are covered (1-8 inclusive)
598-
599-        # check where tipid-abb is not unique
600-        import_df["abb"] = import_df.apply(
601-            lambda x: abb_dict[(x["anode"], x["bnode"])]["ABB"], axis=1
602-        )
603-        import_records = import_df.to_dict("records")
604-

637:            # NOTE: AR: The original check below fails because
638-            # Cindy assumed there would be a 'remove' column,
639-            # instead, only access dict if remove in cols
640-            if "remove" in record:
641-                if record["remove"] == "Y":
642-                    delete_rows.append((tipid, abb))
643-            elif (tipid, abb) in existing_list:
644-                update_rows[(tipid, abb)] = record
645-            else:
646-                insert_rows.append(record)
```
Also ran into the following error from Cindy assuming that 'parkres1' would 
be a column, so added a similar None guard:

```shell
Copying base year...
Base year copied and prepared for modification.

Checking feature classes for errors...
Base feature classes checked for errors.

Importing highway project coding...
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\import_hwyproj_coding.py", line 18, in <module>
    HN.import_hwyproj_coding()
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\modules\HN.py", line 716, in import_hwyproj_coding
    row[cf_dict["CHANGE_PARKRES1"]] = new_attrs["parkres1"]
                                      ~~~~~~~~~^^^^^^^^^^^^
KeyError: 'parkres1'

```

```python
717:                # NOTE: AR: added simple None guard for 'parkres1' and 'parkres2'
718-                row[cf_dict["CHANGE_PARKRES1"]] = (
719-                    new_attrs["parkres1"] if "parkres1" in new_attrs else None
720-                )
721-                row[cf_dict["CHANGE_PARKRES2"]] = (
722-                    new_attrs["parkres2"] if "parkres2" in new_attrs else None
```

Also had to add a similar None guard for buslanes due to the following error:

```shell
Importing highway project coding...
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\import_hwyproj_coding.py", line 18, in <module>
    HN.import_hwyproj_coding()
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\modules\HN.py", line 725, in import_hwyproj_coding
    row[cf_dict["ADD_BUSLANES1"]] = new_attrs["buslanes1"]
                                    ~~~~~~~~~^^^^^^^^^^^^^
KeyError: 'buslanes1'
```

I added:
```python
725:                # NOTE: AR: added simple None guard for 'buslanes1' and 'buslanes2'
726-                row[cf_dict["ADD_BUSLANES1"]] = (
727-                    new_attrs["buslanes1"] if "buslanes1" in new_attrs else None
728-                )
729-                row[cf_dict["ADD_BUSLANES2"]] = (
730-                    new_attrs["buslanes2"] if "buslanes2" in new_attrs else None
```

Also ran into the following error (same Key Error problem but for rrgradex):
```shell
Copying base year...
Base year copied and prepared for modification.

Checking feature classes for errors...
Base feature classes checked for errors.

Importing highway project coding...
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\import_hwyproj_coding.py", line 18, in <module>
    HN.import_hwyproj_coding()
  File "C:\Users\arumph\Repos\mfhrn_programs\scripts\1_travel\modules\HN.py", line 734, in import_hwyproj_coding
    row[cf_dict["ADD_RRGRADECROSS"]] = new_attrs["rrgradex"]
                                       ~~~~~~~~~^^^^^^^^^^^^
KeyError: 'rrgradex'
```
Added a similar None guard:

```python
735:                # NOTE: AR: added None guard for 'rrgradex'
736-                row[cf_dict["ADD_RRGRADECROSS"]] = (
737-                    new_attrs["rrgradex"] if "rrgradex" in new_attrs else None
738-                )
739-                row[cf_dict["NEW_TOLLDOLLARS"]] = new_attrs["tolldollars"]
740-                row[cf_dict["NEW_MODES"]] = new_attrs["modes"]
```

Same thing for 'vclearance'.

**Finally**(!) after fixing all of those problems one at a time, I was able to get
the full `import-hwyproj_coding` tool to run!

## 07/23/26:
Added a MHN preparation script which moves the MHN from the network drive
into the correct places to be used for testing (both into 'tests/inputs/'
and into the expected place in 'input/1_travel/MHN.gdb')

Also, fixed some minor logic bugs and cleaned up the code a little bit
for a couple of the tests.

## 07/24/26:
Added a test to see that the `generate_hwy_files` tool correctly produces
all the expected EMME files. Began working on the MFHRN + MHN pipeline
tests that use the before and after GDBs provided by Tim.

## 07/28/26:
Trying to run all tests including the `base_mhn` prep, but getting the following error:
```shell
 mfhrn_programs  working/aaron arcpy $ date +"%T" && python tests/src/main.py && date +"%T"
10:47:11
Running all tests
Putting base MHN in correct locations
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\main.py", line 89, in <module>
    main()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\main.py", line 69, in main
    base_mhn.main()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\input_preparation\base_mhn.py", line 84, in main
    remove_duplicate_tipids(destination_path)
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\input_preparation\base_mhn.py", line 58, in remove_duplicate_tipids
    for row in cursor:
RuntimeError: Objects in this class cannot be updated outside an edit session [hwyproj]
```

## 08/03/2026:
```
 mfhrn_programs  working/aaron arcpy $ mamba activate arcpy; date +"%T"; python tests/src/pipeline.py; date +"%T"; mamba activate cmaputils
07:28:17
Running full MHN + MFHRN pipeline test
Ensuring that all input test files for pipeline test are in correct place
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\3_output to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\output
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\3_output to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\output
Copying M:\adb\MRN\dev\mrn.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\mrn.gdb
Running `incorporate_edits (MHN)`
--------------------------------------------------------------------------------
Succesfully fixed 'hwynet_arc' in C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb

Validating edits:
-- All arcs have all required attributes
-- All arcs have valid truck restriction attributes
-- New node values have been assigned for split arcs
-- No nodes have duplicate IDs
-- No nodes overlap each other

Updating features (in memory):
-- New NODE values assigned
-- Park-n-Ride NODE values verified
-- Node zone17, subzone17, capzone17 & IMArea fields recalculated
-- Arc ANODE & BNODE fields recalculated
-- Arc ABB field recalculated
-- Arc MILES field recalculated
-- Arc BEARING field recalculated
-- Arc TOLLTYPE field recalculated
-- No duplicate directional links detected

Rebuilding route systems (in memory):
-- hwyproj...
-- bus_base...
-- bus_current...
-- bus_future...
WARNING:
Geodatabase temporarily backed up to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1_20260803081811.gdb. (If update fails for any reason, replace C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb with this.)

Saving changes to disk...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\hwynet_arc...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\hwynet_node...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\hwyproj...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwyproj_coding...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\bus_base...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\bus_base_itin...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\bus_current...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\bus_current_itin...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\hwynet\bus_future...
-- C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb\bus_future_itin...

Rebuilding relationship classes...

Changes successfully applied!

Checking C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\after_state1.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
Differences found in hwynet/hwynet_arc
Start Time: Monday, August 3, 2026 8:21:47 AM
FeatureClass: ObjectID 13114 is different for Field ANODE (Base: 18778, Test: 18781).
FeatureClass: ObjectID 13114 is different for Field ABB (Base: 18778-19681-1, Test: 18781-19681-1).
FeatureClass: ObjectID 33117 is different for Field BNODE (Base: 18784, Test: 18785).
FeatureClass: ObjectID 33117 is different for Field ABB (Base: 19680-18784-0, Test: 19680-18785-0).
FeatureClass: ObjectID 33118 is different for Field ANODE (Base: 18781, Test: 18782).
FeatureClass: ObjectID 33118 is different for Field BNODE (Base: 18785, Test: 18786).
FeatureClass: ObjectID 33118 is different for Field ABB (Base: 18781-18785-0, Test: 18782-18786-0).
FeatureClass: ObjectID 33119 is different for Field ANODE (Base: 18782, Test: 18783).
FeatureClass: ObjectID 33119 is different for Field BNODE (Base: 18786, Test: 18787).
FeatureClass: ObjectID 33119 is different for Field ABB (Base: 18782-18786-0, Test: 18783-18787-0).
FeatureClass: ObjectID 33120 is different for Field ANODE (Base: 18783, Test: 18784).
FeatureClass: ObjectID 33120 is different for Field ABB (Base: 18783-19680-1, Test: 18784-19680-1).
FeatureClass: ObjectID 33121 is different for Field BNODE (Base: 18778, Test: 18781).
FeatureClass: ObjectID 33121 is different for Field ABB (Base: 19680-18778-1, Test: 19680-18781-1).
FeatureClass: ObjectID 33122 is different for Field ANODE (Base: 18778, Test: 18781).
FeatureClass: ObjectID 33122 is different for Field ABB (Base: 18778-19678-1, Test: 18781-19678-1).
FeatureClass: Shape types are the same.
FeatureClass: Feature types are the same.
Table: Table row counts are the same.
FeatureClass: Feature class extents are the same.
GeometryDef: GeometryDefs are the same.
Field: Field properties are the same.
Table: Table row counts are the same.
SpatialReference: Spatial references are the same.
Succeeded at Monday, August 3, 2026 8:21:51 AM (Elapsed Time: 4.19 seconds)
Differences found in hwynet/hwynet_node
Start Time: Monday, August 3, 2026 8:21:51 AM
FeatureClass: ObjectID 1 is different for Field NODE (Base: 18781, Test: 18782).
FeatureClass: ObjectID 2 is different for Field NODE (Base: 18782, Test: 18783).
FeatureClass: ObjectID 3 is different for Field NODE (Base: 18783, Test: 18784).
FeatureClass: ObjectID 4 is different for Field NODE (Base: 18784, Test: 18785).
FeatureClass: ObjectID 5 is different for Field NODE (Base: 18785, Test: 18786).
FeatureClass: ObjectID 6 is different for Field NODE (Base: 18786, Test: 18787).
FeatureClass: ObjectID 17433 is different for Field Shape (Base: Geometry, Test: Geometry).
FeatureClass: ObjectID 17433 is different for Field NODE (Base: 18778, Test: 18779).
FeatureClass: ObjectID 17433 is different for Field POINT_X (Base: 286032.406190335751, Test: 622618.078818168491).
FeatureClass: ObjectID 17433 is different for Field POINT_Y (Base: 1855818.562500588596, Test: 1924687.673777833581).
FeatureClass: ObjectID 17433 is different for Field subzone17 (Base: 16934, Test: 2596).
FeatureClass: ObjectID 17433 is different for Field zone17 (Base: 3148, Test: 1196).
FeatureClass: ObjectID 17433 is different for Field capzone17 (Base: 11, Test: 5).
FeatureClass: ObjectID 17433 is different for Field IMArea (Base: 0, Test: 1).
FeatureClass: ObjectID 17434 is different for Field Shape (Base: Geometry, Test: Geometry).
FeatureClass: ObjectID 17434 is different for Field NODE (Base: 18779, Test: 18780).
FeatureClass: ObjectID 17434 is different for Field POINT_X (Base: 622618.078818168491, Test: 622373.491052750498).
FeatureClass: ObjectID 17434 is different for Field POINT_Y (Base: 1924687.673777833581, Test: 1924793.681439839303).
FeatureClass: ObjectID 17435 is different for Field Shape (Base: Geometry, Test: Geometry).
FeatureClass: ObjectID 17435 is different for Field NODE (Base: 18780, Test: 18781).
FeatureClass: ObjectID 17435 is different for Field POINT_X (Base: 622373.491052750498, Test: 286032.406190335751).
FeatureClass: ObjectID 17435 is different for Field POINT_Y (Base: 1924793.681439839303, Test: 1855818.562500588596).
FeatureClass: ObjectID 17435 is different for Field subzone17 (Base: 2596, Test: 16934).
FeatureClass: ObjectID 17435 is different for Field zone17 (Base: 1196, Test: 3148).
FeatureClass: ObjectID 17435 is different for Field capzone17 (Base: 5, Test: 11).
FeatureClass: ObjectID 17435 is different for Field IMArea (Base: 1, Test: 0).
FeatureClass: Shape types are the same.
FeatureClass: Feature types are the same.
Table: Table row counts are the same.
FeatureClass: Feature class extents are the same.
GeometryDef: GeometryDefs are the same.
Field: Field properties are the same.
Table: Table row counts are the same.
SpatialReference: Spatial references are the same.
Succeeded at Monday, August 3, 2026 8:21:53 AM (Elapsed Time: 1.48 seconds)
hwynet/hwyproj matches
hwyproj_coding matches
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
`incorporate_edits (MHN)` failed: incorporate_edits output does not match after_state1.gdb
Running `import_hwyproj_coding (MFHRN)`
--------------------------------------------------------------------------------
Added missing optional field: CHANGE_PARKRES1 (SHORT)
Added missing optional field: CHANGE_PARKRES2 (SHORT)
Added missing optional field: ADD_BUSLANES1 (SHORT)
Added missing optional field: ADD_BUSLANES2 (SHORT)
Added missing optional field: NEW_VCLEARANCE (DOUBLE)
Added missing optional field: BUSLANES1 (SHORT)
Added missing optional field: BUSLANES2 (SHORT)
Added missing optional field: CHANGE_PARKRES1 (SHORT)
Added missing optional field: CHANGE_PARKRES2 (SHORT)
Added missing optional field: ADD_BUSLANES1 (SHORT)
Added missing optional field: ADD_BUSLANES2 (SHORT)
Added missing optional field: NEW_VCLEARANCE (DOUBLE)
Added missing optional field: BUSLANES1 (SHORT)
Added missing optional field: BUSLANES2 (SHORT)
Copying base year...
Base year copied and prepared for modification.

Checking feature classes for errors...
Base feature classes checked for errors.

Importing highway project coding...
Highway project coding imported.

Checking base project table for errors...
Base highway project table checked for errors.

Finalizing highway data...
Highway data finalized.

Adding back relationship classes...
Relationship classes added.
1m 25s to execute.
Done
Checking C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mhn.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mfhrn.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
Differences found in hwynet/hwyproj
Start Time: Monday, August 3, 2026 8:26:09 AM
WARNING 001620: Difference count exceeds 1500; stopping display. Output compare file was saved to C:\Users\arumph\Documents\ArcGIS\FeatureCompareOut_2.txt.
Succeeded at Monday, August 3, 2026 8:26:10 AM (Elapsed Time: 0.40 seconds)
Differences found in hwyproj_coding
Start Time: Monday, August 3, 2026 8:26:10 AM
WARNING 001620: Difference count exceeds 1500; stopping display. Output compare file was saved to C:\Users\arumph\Documents\ArcGIS\FeatureCompareOut_3.txt.
Succeeded at Monday, August 3, 2026 8:26:11 AM (Elapsed Time: 1.15 seconds)
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
`import_hwyproj_coding (MFHRN)` failed: import_hwyproj_coding output does not match after_state2.gdb
Running `update_highway_project_years (MHN)`
--------------------------------------------------------------------------------
ERROR:
C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\no_code_c26q2 doesn't exist!

Checking C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\after_state3.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state3.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
Differences found in hwynet/hwyproj
Start Time: Monday, August 3, 2026 8:28:39 AM
FeatureClass: ObjectID 623 is different for Field COMPLETION_YEAR (Base: 2039, Test: 2034).
FeatureClass: ObjectID 624 is different for Field COMPLETION_YEAR (Base: 2034, Test: 9999).
FeatureClass: Shape types are the same.
FeatureClass: Feature types are the same.
Table: Table row counts are the same.
FeatureClass: Feature class extents are the same.
GeometryDef: GeometryDefs are the same.
Field: Field properties are the same.
Table: Table row counts are the same.
SpatialReference: Spatial references are the same.
Succeeded at Monday, August 3, 2026 8:28:39 AM (Elapsed Time: 0.36 seconds)
hwyproj_coding matches
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
`update_highway_project_years (MHN)` failed: update_highway_project_years output does not match after_state3.gdb
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 680, in <module>
    main()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 676, in main
    raise RuntimeError(f"Pipeline equivalence test failed:\n{details}")
RuntimeError: Pipeline equivalence test failed:
- incorporate_edits (MHN): incorporate_edits output does not match after_state1.gdb
- import_hwyproj_coding (MFHRN): import_hwyproj_coding output does not match after_state2.gdb
- update_highway_project_years (MHN): update_highway_project_years output does not match after_state3.gdb
08:29:03
 mfhrn_programs  working/aaron cmaputils $
```

## 08/06/2026

```shell

 mfhrn_programs  working/aaron cmaputils $ mamba activate arcpy; date +"%T"; python tests/src/pipeline.py; date +"%T"; mamba activate cmaputils
15:43:53
Running full MHN + MFHRN pipeline test
Ensuring that all input test files for pipeline test are in correct place
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Local copy of M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding already exists at C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding
Local copy of M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years already exists at C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years
Local copy of M:\proj1\tko\MHN\mfhrn_equivalence_test\3_output already exists at C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\output
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 831, in <module>
    main()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 798, in main
    copy_network_input_data()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 414, in copy_network_input_data
    raise FileNotFoundError(
FileNotFoundError: Neither network nor local test GDB exists: M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits\before_state0.gdb
15:44:01
 mfhrn_programs  working/aaron cmaputils $ mamba activate arcpy; date +"%T"; python tests/src/pipeline.py; date +"%T"; mamba activate cmaputils
15:44:13
Running full MHN + MFHRN pipeline test
Ensuring that all input test files for pipeline test are in correct place
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\3_output to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\output
Successfully copied M:\proj1\tko\MHN\mfhrn_equivalence_test\3_output to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\output
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits\before_state0.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\before_state0.gdb
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding\before_state1.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding\before_state1.gdb
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years\before_state2.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\before_state2.gdb
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\0_incorporate_edits\after_state1.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\after_state1.gdb
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\1_import_hwy_coding\after_state2.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding\after_state2.gdb
Copying M:\proj1\tko\MHN\mfhrn_equivalence_test\2_update_hwyproj_years\after_state3.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\after_state3.gdb
Copying M:\adb\MRN\dev\mrn.gdb to C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\mrn.gdb
Checking C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\after_state1.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding\before_state1.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
hwynet/hwyproj matches
hwyproj_coding matches
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
Checking C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\import_hwy_coding\after_state2.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\before_state2.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
hwynet/hwyproj matches
hwyproj_coding matches
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
Pipeline fixture handoffs and auxiliary inputs are valid
Running `incorporate_edits (MHN)`
--------------------------------------------------------------------------------
Succesfully fixed 'hwynet_arc' in C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\output_after_state1.gdb

Validating edits:
-- All arcs have all required attributes
-- All arcs have valid truck restriction attributes
-- New node values have been assigned for split arcs
-- No nodes have duplicate IDs
-- No nodes overlap each other

Updating features (in memory):
-- New NODE values assigned
-- Park-n-Ride NODE values verified
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mhn_programs\src\incorporate_edits.py", line 473, in <module>
    subzone_lyr = MHN.make_skinny_feature_layer(MHN.subzone, 'subzone_lyr', [MHN.zone_attr, MHN.subzone_attr, MHN.capzone_attr])
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\arumph\Repos\mhn_programs\src\MHN.py", line 679, in make_skinny_feature_layer
    return self.make_skinny(True, fc, lyr, keep_fields_list, where_clause)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\arumph\Repos\mhn_programs\src\MHN.py", line 662, in make_skinny
    input_fields = arcpy.ListFields(in_obj)
                   ^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Program Files\ArcGIS\Pro\Resources\ArcPy\arcpy\__init__.py", line 1228, in ListFields
    return gp.listFields(dataset, wild_card, field_type)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Program Files\ArcGIS\Pro\Resources\ArcPy\arcpy\geoprocessing\_base.py", line 378, in listFields
    self._gp.ListFields(*gp_fixargs(args, True)))
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
OSError: "C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\incorporate_edits\zone_systems.gdb\zonesys17\subzones17" does not exist
`incorporate_edits (MHN)` failed: Command '['C:\\Users\\arumph\\AppData\\Local\\ESRI\\conda\\envs\\arcpy\\python.exe', 'C:/Users/arumph/Repos/mhn_programs\\src\\incorporate_edits.py', 'C:\\Users\\arumph\\Repos\\mfhrn_programs\\tests\\inputs\\pipeline\\incorporate_edits\\output_after_state1.gdb']' returned non-zero exit status 1.
Running `import_hwyproj_coding (MFHRN)`
--------------------------------------------------------------------------------
Added missing optional field: CHANGE_PARKRES1 (SHORT)
Added missing optional field: CHANGE_PARKRES2 (SHORT)
Added missing optional field: ADD_BUSLANES1 (SHORT)
Added missing optional field: ADD_BUSLANES2 (SHORT)
Added missing optional field: NEW_VCLEARANCE (DOUBLE)
Initialized added fields to 0: CHANGE_PARKRES1, CHANGE_PARKRES2, ADD_BUSLANES1, ADD_BUSLANES2, NEW_VCLEARANCE
Added missing optional field: BUSLANES1 (SHORT)
Added missing optional field: BUSLANES2 (SHORT)
Initialized added fields to 0: BUSLANES1, BUSLANES2
Added missing optional field: CHANGE_PARKRES1 (SHORT)
Added missing optional field: CHANGE_PARKRES2 (SHORT)
Added missing optional field: ADD_BUSLANES1 (SHORT)
Added missing optional field: ADD_BUSLANES2 (SHORT)
Added missing optional field: NEW_VCLEARANCE (DOUBLE)
Initialized added fields to 0: CHANGE_PARKRES1, CHANGE_PARKRES2, ADD_BUSLANES1, ADD_BUSLANES2, NEW_VCLEARANCE
Added missing optional field: BUSLANES1 (SHORT)
Added missing optional field: BUSLANES2 (SHORT)
Initialized added fields to 0: BUSLANES1, BUSLANES2
Copying base year...
Base year copied and prepared for modification.

Checking feature classes for errors...
Base feature classes checked for errors.

Importing highway project coding...
Highway project coding imported.

Checking base project table for errors...
Base highway project table checked for errors.

Finalizing highway data...
Highway data finalized.

Adding back relationship classes...
Relationship classes added.
0m 58s to execute.
Done
Checking C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mhn.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mfhrn.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
Differences found in hwynet/hwyproj
Rows missing from C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mfhrn.gdb: 19; sample keys: [('10030007',), ('10030008',), ('8000047',), ('8000058',), ('8070010',), ('9000030',), ('9000033',), ('9060015',), ('9990101',), ('9990102',), ('10000115',), ('10940014',), ('12070007',), ('11000407',), ('6140014',), ('8160032',), ('10110061',), ('9030002',), ('12345678',)]
Differences found in hwyproj_coding
Rows missing from C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mfhrn.gdb: 1511; sample keys: [('1000059', '21890-21897-1'), ('1000059', '21887-21889-1'), ('1000059', '21889-21893-1'), ('1000059', '15545-15520-1'), ('1000059', '21892-21888-1'), ('1000059', '21880-21883-1'), ('1000059', '21896-21891-1'), ('1000059', '15521-15546-1'), ('10030003', '9362-9444-0'), ('10030003', '9362-9615-1'), ('10030003', '9444-9615-0'), ('10030007', '8364-8262-0'), ('10030007', '8276-8262-1'), ('10030007', '8364-8276-1'), ('10030007', '8204-8262-0'), ('10030007', '8204-8276-1'), ('10060020', '10310-19891-0'), ('10060020', '10204-19893-0'), ('10060020', '10311-10052-1'), ('10060020', '10052-19892-0')]
Rows missing from C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state2_mhn.gdb: 44; sample keys: [('4000023', '11624-11523-1'), ('4000023', '12199-20215-1'), ('4000023', '12286-20210-1'), ('4000023', '12343-12521-1'), ('4000023', '12521-23956-1'), ('4000023', '12535-20211-1'), ('4000023', '12606-12535-1'), ('4000023', '12638-12642-1'), ('4000023', '12642-12652-1'), ('4000023', '13349-21076-1'), ('4000023', '13891-13998-1'), ('4000023', '13998-21051-1'), ('4000023', '14074-21048-1'), ('4000023', '14222-14074-1'), ('4000023', '14225-14273-1'), ('4000023', '14273-14458-1'), ('4000023', '14274-14222-1'), ('4000023', '14458-14512-1'), ('4000023', '14463-14274-1'), ('4000023', '14512-14634-1')]
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
`import_hwyproj_coding (MFHRN)` failed: import_hwyproj_coding output does not match after_state2.gdb
Running `update_highway_project_years (MHN)`
--------------------------------------------------------------------------------

Checking future transit projects...

All in-region, conformed projects coded in MHN are listed in C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\year_c26q2.csv or C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\required_c26q2.csv!
WARNING:
WARNING: Some projects in C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\year_c26q2.csv or C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\required_c26q2.csv but not C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\no_code_c26q2.csv are not yet coded in MHN. See C:\Users\arumph\Repos\mhn_programs\temp\in_year_not_mhn.txt for details.

Updating COMPLETION_YEAR values for projects coded in MHN that are listed in C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\year_c26q2.csv or C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\required_c26q2.csv...

All done!

Checking C:\Users\arumph\Repos\mfhrn_programs\tests\inputs\pipeline\update_hwyproj_years\after_state3.gdb and C:\Users\arumph\Repos\mfhrn_programs\tests\outputs\pipeline\after_state3.gdb for equality
bus_base_itin matches
bus_current_itin matches
bus_future_itin matches
hwynet/bus_base matches
hwynet/bus_current matches
hwynet/bus_future matches
hwynet/hwynet_arc matches
hwynet/hwynet_node matches
hwynet/hwyproj matches
hwyproj_coding matches
mhn_baselinks matches
parknride matches
sensor_points matches
sensor_roads_intersect matches
z_bus_future_itin_2024 matches
`update_highway_project_years` tool passed
`update_highway_project_years (MHN)` succeeded
Traceback (most recent call last):
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 831, in <module>
    main()
  File "C:\Users\arumph\Repos\mfhrn_programs\tests\src\pipeline.py", line 827, in main
    raise RuntimeError(f"Pipeline equivalence test failed:\n{details}")
RuntimeError: Pipeline equivalence test failed:
- incorporate_edits (MHN): Command '['C:\\Users\\arumph\\AppData\\Local\\ESRI\\conda\\envs\\arcpy\\python.exe', 'C:/Users/arumph/Repos/mhn_programs\\src\\incorporate_edits.py', 'C:\\Users\\arumph\\Repos\\mfhrn_programs\\tests\\inputs\\pipeline\\incorporate_edits\\output_after_state1.gdb']' returned non-zero exit status 1.
- import_hwyproj_coding (MFHRN): import_hwyproj_coding output does not match after_state2.gdb
16:41:43
```
