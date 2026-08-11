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

Based on my inspection of the outputs, I can say that the tools seem to produce the correct outputs

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
the MHN `update_hwyproj_years`.
