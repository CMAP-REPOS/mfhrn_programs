# Project Log:

## 07/07/2026:
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
Spent the day looking over the MHN code so that I can map
the functionality of one tool to that of another (for equivalence)
testing. Also spent a decent amount of time working on testing
harness/setup/env, but had to do trainings and had shorter day 
so did not make as much progress as would have liked.
