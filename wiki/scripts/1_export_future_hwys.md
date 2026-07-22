This page describes 1_export_future_hwys.py, which exports future highway networks. To use this tool, run 

`python [path-to-tool]` with an optional -s (subset) flag if you only want certain projects to be built. 

This script is a translation of the [Export Future Network](https://github.com/CMAP-REPOS/mhn_programs/wiki/Export-Future-Network-%28Utilities%29-Tool) utility in the [mhn_programs](https://github.com/CMAP-REPOS/mhn_programs) repository, except you can generate multiple future networks. 

## Input

Ensure your input directory has this structure: 

```
input
|-- input_years.csv
`-- 1_travel
    `-- MHN.gdb
    |-- subset_hwy_projects.csv
```

You need a csv file called input_years.csv to specify the scenario years and a copy of the MHN as MHN.gdb. If you only want certain projects to be built, you need a csv file called subset_hwy_projects.csv. 

In templates, there is a template file called subset_hwy_projects_template.csv which you can just fill in. Put the TIPID-ABB combinations that you want built. You can put "all" in the ABB column as a shortcut to say "build everything in this project." 

## Function

There are 3 major steps in the script. 

### Step 1 - Check Feature Classes
The feature classes (nodes, links, and projects) are checked for errors. If there are any errors with the feature classes, then the program will be crashed. 

### Step 2 - Check Project Coding Table
The project coding table is checked for errors. If there are any rows with errors, the script will keep running, but a field called "USE" will be flagged (by setting it to 0 as opposed to 1) that this row is invalid, and the project coding in that row will not be applied. 

### Step 3 - Build Future Networks
The future networks are built by applying the projects year by year, which can also be thought of as pushing the base year up year by year. 

This works as a for loop inside of a for loop. For example, let's say that the build years specified were 2020 and 2025. Then this can be visualized as:

```
--- 2015
|
|
--> 2020 
|   -> 2016 -> 2017 -> 2018 -> 2019 -> 2020
|
--> 2025 
    -> 2021 -> 2022 -> 2023 -> 2024 -> 2025
```

The outer for loop indicates what gdb is being worked on. MHN_2015.gdb gets copied to MHN_2020.gdb, and then MHN_2020.gdb gets copied to MHN_2025.gdb. 

The inner for loop pushes up the base year. Once MHN_2015.gdb gets copied to MHN_2020.gdb, the base year is still 2015, so the base year then gets pushed up to 2016, then 2017, up to 2020. The base year is pushed up by applying the projects associated with that year. 

A combined geodatabase is created to store the built highway networks, as well as tables to easily verify that projects were applied correctly.

## Output

```
output
`-- 1_travel
    `-- MHN_{base}.gdb
    `-- MHN_{year1}.gdb
    `-- MHN_{year2}.gdb
    *
    *
    *
    `-- MHN_all.gdb
    |-- base_feature_class_errors.txt
    |-- base_project_table_errors.txt
    |-- base_project_table_errors.xlsx
```

1. The script outputs a new geodatabase called MHN_{base}.gdb. For example, if the base year is 2015, then it will be called MHN_2015.gdb.
2. The script outputs the file base_feature_class_errors.txt **IF AND ONLY IF** there were issues with the base feature classes.
3. The script outputs the files base_project_table_errors.txt and base_project_table_errors.xlsx - the text file contains a summary of errors and warnings about the project coding rows, and the excel file contains the project coding rows with errors and warnings. 
4. The script outputs a new geodatabase called MHN_{year}.gdb for every specified build year. For example, if the years were 2020 and 2025, then there would be MHN_2020.gdb and MHN_2025.gdb. As indicated by the name, the network has been built to the year in the geodatabase. 
5. All the useful information in the other geodatabases is combined into MHN_all.gdb. 
