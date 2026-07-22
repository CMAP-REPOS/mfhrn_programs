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
