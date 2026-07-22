This page describes 2_generate_hwy_files.py, which generates highway files for modeling in EMME. To use this tool, run

`python [path-to-tool]`.

This script is a translation of the [Generate Highway Files](https://github.com/CMAP-REPOS/mhn_programs/wiki/Generate-Highway-Files-Tool) tool in the [mhn_programs](https://github.com/CMAP-REPOS/mhn_programs) repository. 

## Input

**Note: Before you run this tool, you MUST have already have run the [[Export Future Highways]] tool, so that your output folder already contains MHN_all.gdb.** 

```
input
|-- input_years.csv
output
`-- 1_travel
    `-- MHN_all.gdb
```

## Function

This script simply translates the already built highway networks into text files suitable for importing into EMME. 

## Output

```
output
`-- 1_travel
    `-- highway
        `-- {scen_1}00
        `-- {scen_2}00
        *
        *
        *
```

A directory called highway will be created. Inside will be a subdirectory for each scenario specified in the input- such as 100 for scenario 1, 200 for scenario 2, etcetera. 

For each scenario, 2 link files and 2 node files will be generated for each TOD (with extensions l1, l2, n1, and n2, where "1" files are for required attributes and "2" files are for extra attributes). Additionally for each scenario, a linkshape file will be created containing the coordinates of the vertices of each link which exists in that scenario. 
