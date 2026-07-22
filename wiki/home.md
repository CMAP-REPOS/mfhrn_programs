Welcome to the mfhrn_programs wiki!

### Setup
1. Ensure you have ArcGIS installed on your computer. (I have a Basic license.) 
2. Clone this repository onto your computer, in your local disk. Do NOT clone it into a network drive as this will run very slowly!
3. Ensure you have access to a clean, up-to-date copy of the MHN. 

I personally use the environment arcgispro-py3 which comes installed with ArcGIS. 

### Usage
The concept of this repository is that it reimagines mhn_programs (the original repository for processing the MHN) in a modern way which also acknowledges its role in freight and travel modeling. Essentially, this repository is designed to be the first step in a process which could later involve the MFN and MRN in later steps. 

### Directory Structure 

The input and script folder structures mirror each other. Ex. the input in input/1_travel will be processed by the scripts in scripts/1_travel.

```
docs 
`-- images
input
`-- 1_travel
`-- 2_freight
scripts 
`-- 1_travel
`-- 2_freight
templates
```

- **images** - this folder contains images which are shown in the README.md and the wiki.
- **input** - this folder contains data which the programs use as input.
  - **1_travel** - this contains input which is specific to travel modeling and involves the MHN/MRN. 
  - **2_freight** - this contains input which is specific to freight modeling and involves the MFN.
- **scripts** - this folder contains scripts for running the programs. 
  - **1_travel** - this contains scripts which are specific to travel modeling and involves information from the MHN/MRN. 
  - **2_freight** - this contains scripts which are specific to freight modeling and involves information from the MFN.
- **templates** - contains template input files. 

After running the scripts, a folder called "output" will be created automatically. This will contain the output for the scripts. 
