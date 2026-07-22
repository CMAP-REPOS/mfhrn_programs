This page describes import_hwyproj_coding.py, which imports highway project coding and checks its validity. To use this tool, run

`python [path-to-tool]`.

This script is a translation of the [Import Highway Projects](https://github.com/CMAP-REPOS/mhn_programs/wiki/Import-Highway-Projects-Tool) tool in the [mhn_programs](https://github.com/CMAP-REPOS/mhn_programs) repository. 

## Input

Ensure your input directory has this structure:

```
input
|-- input_years.csv
`-- 1_travel
    `-- MHN.gdb
    |-- import_hwyproj_coding.xlsx
```

You need a csv file called input_years.csv to specify the scenario years and a copy of the MHN as MHN.gdb. (While the scenario years are not logically relevant here, it is required for the code to work.)

If you just want to check the validity of the project table, you do not need import_hwyproj_coding.xlsx. However, if you do want to import new project coding, you must have an excel file called import_hwyproj_coding.xlsx. There are 28 required columns in this excel file, and in the templates directory, there is a template file called import_hwyproj_coding_template.xlsx which you can just fill in. 

The column "remove" indicates rows to be dropped from the project table. Set remove = Y if you want to remove that row. 

## Function

There are 4 major steps in the script.

### Step 1 - Check Feature Classes
The feature classes (nodes, links, and projects) are checked for errors. If there are any errors with the feature classes, then the program will be crashed.

### Step 2 - Import New Project Coding
If there is new project coding to import, the project coding will be imported. 

1. Each row must have all four required fields filled in: TIPID, ANODE, BNODE, and ACTION CODE.
2. ANODE-BNODE must correspond to a valid link in the highway network (so that ABB can be calculated). 
3. Each row must be uniquely identifiable by its TIPID-ABB combination. 

If any of the rows fail, the program crashes. Offending rows will be written to a file called import_project_coding_errors.csv.

The imported rows are then divided into 3 groups. 

1. Group 1 - rows to delete, where the TIPID-ABB combination already exists in the project table and remove = Y. These rows are removed from the project coding table. 
1. Group 2 - rows to update, where the TIPID-ABB combination already exists in the project table. These rows are updated (overwritten) by the imported rows. 
2. Group 3 - rows to insert, where the TIPID-ABB combination does not already exist in the project table. The imported rows are inserted into the project table. 

### Step 3 - Check Project Coding Table
The project coding table is checked for errors. If there are any rows with errors, the script will keep running, but a field called "USE" will be flagged (by setting it to 0 as opposed to 1) that this row is invalid. 

### Step 4 - Finalize Highway Data
The project coding table drops all invalid rows (where USE = 0). The project feature class has its geometry recalculated with the rows of hwyproj_coding which were kept. If a project no longer has a single valid corresponding row in the project coding table, then it is written to a file called removed_projects.txt. 

More cleanup is done (removing temporary fields + adding relationship classes).

## Output

```
output
`-- 1_travel
    `-- MHN_{base}.gdb
    |-- base_feature_class_errors.txt
    |-- base_project_table_errors.txt
    |-- base_project_table_errors.xlsx
    |-- import_project_coding_errors.csv
    |-- removed_projects.txt
```

1. The script outputs a new geodatabase called MHN_{base}.gdb. For example, if the base year is 2015, then it will be called MHN_2015.gdb.
2. The script outputs the file base_feature_class_errors.txt **IF AND ONLY IF** there were issues with the base feature classes.
3. The script outputs the file import_project_coding_errors.csv **IF AND ONLY IF** the projects were not successfully imported. The csv file will contain the unusable rows. 
4. The script outputs the files base_project_table_errors.txt and base_project_table_errors.xlsx if the projects were successfully imported- the text file will contain a summary of errors and warnings about the project coding rows, and the excel file will have the project coding rows with errors and warnings. 
5. The scripts outputs the file removed_projects.txt, which contains information on projects which no longer have corresponding coding. 

## Notes

1. Because of the way the script checks for errors, project coding can only be successfully added when the project already exists in the hwynet/hwyproj feature class. This means that the TIPID must already be in that feature class. If adding a totally new project, make sure to manually add the TIPID into hwynet/hwyproj first (creating a row with a null geometry). 
2. The output file base_project_table_errors.xlsx has the 28 required columns, so you can directly edit it to fix the errors, change the name to import_hwy_project_coding.xlsx, and then stick it in the input folder to be re-imported. 
