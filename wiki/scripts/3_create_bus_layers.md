This page describes 3_create_bus_layers.py, which creates bus layers for different scenarios. To use this tool, run

`python [path-to-tool]`.

This script is a translation of the first half of the [Generate Transit Files](https://github.com/CMAP-REPOS/mhn_programs/wiki/Generate-Transit-Files-Tool) tool in the [mhn_programs](https://github.com/CMAP-REPOS/mhn_programs) repository. 

## Input

**Note: Before you run this tool, you MUST have already have run the [[Export Future Highways]] tool, so that your output folder already contains MHN_all.gdb.** 

```
input
|-- input_years.csv
`-- 1_travel
    `-- MHN.gdb
output
`-- 1_travel
    `-- MHN_all.gdb
```

## Function

The script runs in two main steps. The first step is not concerned with scenarios, while the second step builds the bus network for a specified set of scenarios. 

### Step 1 - Collapse Bus Routes

1. A GDB called collapsed_routes.gdb is created. 
2. Within that GDB, for each transit TOD (1, 2, 3, 4), the bus_base and bus_current feature classes (both GTFS) are partitioned according to the TOD period their runs fall into. 
3. For each TOD period, similar runs are collapsed / grouped together (similar meaning the runs share the same MODE-ROUTE_ID and have similar stopping patterns), and a representative run is chosen for each group (the run which is longest then starts earliest). The headway is calculated for each group. 
4. For representative runs, the corresponding itinerary is imported into the GDB and checked for errors. Itinerary gaps and false links are noted and written to the error file error_file_1.txt.
5. We can think of the bus_future feature class as already being in its collapsed state. The corresponding future itineraries are imported and checked for errors the same way as the GTFS itineraries, with itinerary gaps and false links being noted and written to the error file error_file_1.txt.
6. The park and ride nodes are imported into the GDB. 

## Step 2 - Create Bus Layers

For each scenario and transit TOD:

1. The available highway network is found. This is the collection of links and nodes which are available in that scenario and at that TOD. Buses can only travel on the links on this network.
2. The GTFS runs are combined with the added and replaced future runs (added means that the run is added, replaced means that this new run replaces old runs with the specified MODE-ROUTE_ID combination). For added and replaced runs, the headway is calculated through a rather convoluted process. 
3. The corresponding itineraries are modified to satisfy the following requirements:
   - If the MODE-ROUTE_ID specifies that the line should be rerouted, a reroute is performed. Lines which were unable to be rerouted are written to error_file_2.txt. 
   - If either the first node or the last node of the line is not found in the network, it will be replaced by the nearest available node. Lines where the first node or last node could not be replaced will be written to error_file_2.txt.
   - For consecutive links of the itinerary which are not in the network, shortest paths will be calculated to fill them in. Lines where no shortest path is available will be written to error_file_2.txt.

For each scenario overall:

1. The park and ride nodes are selected for that scenario. A set of scenario nodes are created, meaning nodes which are available in that scenario at all times of day. If a park and ride node is not in that set of scenario nodes, the closet node **in the same zone** will be used as a replacement. If a replacement node cannot be found, this will be written to error_file_2.txt.

## Output

```
output
`-- 1_travel
    `-- bus_network
        `-- collapsed_routes.gdb
        `-- SCENARIO_1.gdb
        `-- SCENARIO_2.gdb
        *
        *
        *
        |-- error_file_1.txt
        |-- error_file_2.txt
```

A directory called bus_network will be created. Everything else is created inside of it.

1. A geodatabase called collapsed_routes.gdb will be created which contains the collapsed routes as described by Step 1 - Collapse Bus Routes. 
2. A geodatabase will be created for each specified scenario as described by Step 2 - Create Bus Layers. Each of the geodatabases will contain the lines, itineraries, highway links + necessary park and ride nodes that applies to that scenario. 
3. An error file called error_file_1.txt will be created that describes anything that went wrong while collapsing the bus routes.
4. An error file called error_file_2.txt will be created that describes anything that went wrong while creating the bus layers. 
