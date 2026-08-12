# MFHRN Testing Takeaways

Overall, Cindy's MFHRN code mostly works, with a few small changes needed
in some of the tools to allow the tools to run without errors caused by
differences in the current MHN schema and the presumed new schema.

Cindy's code assumes that the following fields which do not match the current
MHN schema will exist:
- RRGRADECROSS
- BUSLANES1
- BUSLANES2
- PARKRES1
- PARKRES2

## Process and Tests

### MFHRN Tool Tests
The majority of the tests I wrote exist just to verify that
Cindy's code runs successfully. Beyond this, it is very difficult to write
good tests, as in order to check whether the scripts produce the correct
or intended outputs, I would have needed to come up with a way of producing 
known good data myself (i.e., I would essentially have had to write my own
complete scripts). As such, I had to manually check the outputs in most cases to 
ensure the scripts produce the correct outputs. I inspected the outputted MHNs 
in ArcGIS, and the outputted EMME files in nvim.



### Pipeline Test
The primary exception to this is the 'pipeline' test. Tim provided me with 
a few 'before' and 'after' GDBs to allow me to test the following tools:
1. `incorporate_edits`
2. `import_hwyproj_coding`
3. `update_hwyproj_years`

However, since Cindy's code only provides an equivalent tool for
`import_hwyproj_coding`, I needed to use the MHN repo's tools for 
`incorporate_edits` and `update_hwyproj_years`. For this test, the 
before and after changes are cumulative, so I needed to write the
tests in a pipeline that first runs the MHN `incorporate_edits`
then runs the MFHRN `import_hwyproj_coding`, then finally, runs
the MHN `update_hwyproj_years` tool. Eventually, the pipeline test ran correctly, 
with some minor changes needed to get around issues with ArcPy. 


#### First stage
The first stage of the pipeline test ran successfully, with minor differences 
between the processed `before_state0.gdb` (i.e., the output of
the `incorporate_edits` tool) and the expected `after_state1.gdb`. 
These differences can be attributed to the way ArcGIS assigns OBJECTIDs. 
A manual inspection of the differences shows that the output of 
`incorporate_edits` matches the expected output.


#### Second stage
The second stage of the pipeline test also ran successfully. Unlike the 
previous stage, the `import_hwyproj_coding` tool produced significantly
different results than expected. The difference between the actual output
of MFHRN's `import_hwyproj_coding` tool and the expected output can be attributed
to differences in schema/expectations for the MFHRN tool and for the MHN tool.

The MFHRN `import_hwyproj_coding` tool treats the inputted Excel spreadsheet used to
make edits as containing in-place edits, whereas the MHN tool treats the spreadsheet as
containing an entirely new project coding. The MFHRN tool does the following:
1. Add any new rows (identified by unique 'ABB' and 'TIPID')
2. Update any existing rows to match the specified desired changes (identified by unique 'ABB' and 'TIPID')
3. Delete any rows marked for deletion with 'REMOVE' field marked with "Y"

On the other hand, the MHN tool does the following:
1. Import spreadsheet changes
2. Validates `hwyproj_coding` table, makring invalid entries with "USE = 0"
3. Deletes invalid rows
4. Deletes any highway project without a valid coding

In short, the MHN tool expects that *all* highway projects will be included
in the input coding spreadsheet, while MFHRN only expects changes to the existing
`hwyproj_coding` table to be included in the input spreadsheet. As such,
the output produced by the two tools does not match. In the case of the MHN tool,
only the highway project coding information provided in the input spreadsheet makes it
into the resulting `hwyproj_coding` table, whereas in the case of the MFHRN tool,
only explicit changes in the spreadsheet are translated to the `hwyproj_coding` table.

This explains the high number of mismatches/errors when comparing the output of MFHRN's 
`import_hwyproj_coding` tool to the expected `after_state2.gdb`. It also explains why
the MFHRN tool will error and refuse to run when using an input spreadsheet designed for
the MHN tool, as there will almost certainly be rows that already exist in the existing
`hwyproj_coding` table (as identified by ABB/TIPID).

#### Third stage
The third stage ran successfully with all feature classes in the output
of the `update_hwyproj_years` tool matching those in the expected `after_state3.gdb`.

## Results
Based on my testing, I can say that the tools seem to produce the correct outputs,
and I would feel comfortable saying that the MFHRN tools below can comfortably be used
in place of their MHN equivalents:

- export_future_hwys (`scripts/1_travel/1_export_future_hwys.py`)
- generate_hwy_files (`scripts/1_travel/2_generate_hwy_files.py`)
- create_bus_layers (`scripts/1_travel/3_create_bus_layers.py`)
- generate_transit_files (`scripts/1_travel/4_generate_transit_files.py`)
- import_hwyproj_coding (`scripts/import_hwyproj_coding`)

### Schema differences
While the tools listed above are ready to use, it should be noted that the expected 
inputs to the MFHRN tools are different that those expected by the MHN tools. In some 
cases, this is as simple as adding a field to a table in ArcGIS. In the case of the
`import_hwyproj_coding` tool, however, the MFHRN version has a completely different
set of expectations about the ***nature*** of the the inputs which is extremely important
to understand before using. Below is a breakdown of what schema changes are needed
to use the MFHRN tools in their original state *(as it exists on the 'main' branch)*:

- export_future_hwys:
    `hwynet/hwynet_arc` table should contain: 
    - "PARKRES1"
    - "PARKRES2"
    - "BUSLANES1"
    - "BUSLANES2"
    - "VCLEARANCE"

    Accordingly, `hwyproj_coding` table should contain:
    - "CHANGE_PARKRES1"
    - "CHANGE_PARKRES2"
    - "ADD_BUSLANES1"
    - "ADD_BUSLANES2"
    - "NEW_VCLEARANCE"

- generate_hwy_files:
    Same as export_future_hwys.

- create_bus_layers:
    Same as export_future_hwys.

- generate_transit_files:
    Same as export_future_hwys.

- import_hwyproj_coding:
    **Important**:  the MFHRN `import_hwyproj_coding` tool expects
    that the input Excel spreadsheet *contains only* the desired edits
    for the `hwyproj_coding` table. As such, previously used MHN 
    spreadsheets **will NOT work**. 

    Additionally, the MFHRN tool expects
    the following spreadsheet fields that are not currently used for MHN
    coding spreadsheets:
    - "parkres1"    (add/remove parking restriction anode-bnode direction)
    - "parkres2"    (add/remove parking restriction bnode-anode direction)
    - "buslanes1"   (add/remove buslanes anode-bnode direction)
    - "buslanes2"   (add/remove buslanes bnode-anode direction)
    - "rrgradex"    (add/remove railroad grade crossing)
    - "vclearance"  (vertical clearance)
    - "remove"      (whether to remove the row from `hwyproj coding`)

    On the other hand, the MHN spreadsheet uses the following spreadsheet fields
    not used for MFHRN:
    - "rr_grade_sep"
    - "tod"
    - "rep_anode"
    - "rep_bnode"


## Final thoughts

Though I believe these tools are functional and can be used in place of the MHN tools,
I would still recommend that the outputs of these tools be checked (at least initially).
These checks do not need to be extensive; a brief comparison between the equivalent MHN
tool's output, or a quick check of the MFHRN tool's output in ArcGIS, EMME, or Excel would
suffice. These checks would ensure that any potential issues which might not have been caught,
or which only appear with specific input data, will not slip through the cracks.

I would also advise caution when switching to using MFHRN's `import_hwyproj_coding`
tool, as it requires an entirely different input spreadsheet in terms of both
fields and in terms of what it contains.


