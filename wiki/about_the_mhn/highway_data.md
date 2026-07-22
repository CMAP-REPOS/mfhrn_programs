A diagram of the MHN is shown below, with bus-specific data grayed out. 

![A diagram showing the highway data relationships.](https://github.com/CMAP-REPOS/mfhrn_programs/blob/main/docs/images/highway_data.png)

## Nodes (Point Feature Class)

The nodes are defined by their unique node ID. 

**For nodes, all of the fields are automatically calculated by the [[Incorporate Edits]] tool, and no manual updates should be made on this feature class!**

|#|NAME|TYPE|LENGTH|DEFAULT|DOMAIN|NOTE|
|---|---|---|---|---|---|---|
|1|**NODE**🔑 |LONG|   |   |NODE|Node ID|
|2|POINT_X|DOUBLE|   |   |   |Generated x-coordinate|
|3|POINT_Y|DOUBLE|   |   |   |Generated y-coordinate|
|4|subzone17|LONG|   |   |SUBZONE|CMAP trip generation zone|
|5|zone17|LONG|    |    |ZONE|CMAP modeling zone|
|6|capzone17|LONG|    |    |CAPZONE|CMAP capacity zone|
|7|IMArea|SHORT|    |    |BINARY|Illinois Vehicle Inspection and Maintenance Program area flag|

## Links (Polyline Feature Class)

The links get their ANODE and BNODE values from the node feature class. The links are defined by their unique ABB (calculated from ANODE-BNODE-BASELINK).

In the table below, PROJ refers to whether the field has a corresponding field in the highway project coding table. Skeleton links (where the baselink is 0) have fields 7-28 set to the default (0 or in the case of parkres -), but can have information stored in fields 29-34. 

**ANODE, BNODE, ABB, MILES, and BEARING are calculated automatically by the [[Incorporate Edits]] tool. Do not manually update!**

**ANODE, BNODE, and SRA allows null values. Every other field is non-nullable.**

|#|NAME|TYPE|LENGTH|DEFAULT|DOMAIN|PROJ|NOTE|
|---|---|---|---|---|---|---|---|
|1 & 2|ANODE & BNODE🔗|LONG|   |    |NODE|   |"From" and "To" node IDs|
|3|BASELINK|TEXT|1|0|BASELINK|Y|Regular vs skeleton link|
|4|**ABB**🔑|TEXT|13|   |   |Y|Link identifier (ANODE-BNODE-BASELINK)|
|5|ROADNAME|TEXT|50|   |   |   |Name of road as string|
|6|DIRECTIONS|TEXT|1|0|DIRECTIONS|Y|Link directions flag|
|7 & 8|TYPE1 & 2|TEXT|2|0|VDF|Y|Volume displacement function flag|
|9 & 10|AMPM1 & 2|TEXT|1|0|AMPM|Y|AMPM restriction flag|
|11 & 12|POSTEDSPEED1 & 2|SHORT|   |0|POSITIVE|Y|Posted speed limit|
|13 & 14|THRULANES1 & 2|SHORT|   |0|POSITIVE|Y|# of driving lanes|
|15 & 16|THRULANEWIDTH1 & 2|SHORT|   |0|POSITIVE|Y|Width of lane (feet)|
|17 & 18|PARKLANES1 & 2|SHORT|   |0|POSITIVE|Y|# of parking lanes|
|19 & 20|PARKRES1 & 2|TEXT|2|-|PARKRES|Y|TOD parking restrictions, **coded separately for DIRECTIONS = 2**|
|21 & 22|BUSLANES1 & 2|SHORT|    |0|BINARY|Y|Bus lane present- yes/no|
|23|SIGIC|SHORT|    |0|BINARY|Y|Signal interconnect flag|
|24|CLTL|SHORT|    |0|BINARY|Y|Bi-directional continuous left turn flag|
|25|RRGRADECROSS|SHORT|    |0|BINARY|Y|At-grade railroad crossing flag|
|26|TOLLDOLLARS|TEXT|255|0|    |Y| Toll amount ($)|
|27|MODES|TEXT|3|0|HWYMODE|Y|Modes permitted on link|
|28|VCLEARANCE|SHORT|    |0|VCLEARANCE|Y|Overhead clearance (inches), 999 = ? above 162|
|29|NHSIC|SHORT|    |0|BINARY|    |National Highway System intermodal connector flag|
|30|SRA|TEXT|6|    |SRA|    |Strategic Regional Arterial system route code|
|31|CHIBLVD|SHORT|    |0|BINARY|    |Chicago boulevard system flag|
|32|TOLLSYS|SHORT|    |0|BINARY|    |Current/future toll system link flag|
|33|TRUCKRTE|TEXT|1|0|TRUCKRTE|    |Truck route code|
|34|MESO|SHORT|    |0|BINARY|    |Meso-scale freight highway network flag|
|35|MILES|DOUBLE|    |0|    |    |Link length in miles|
|36|BEARING|TEXT|3|X|BEARING|    |Simple bearing of link in from-to direction|

In order to allow the toll to change during the day, the toll can be either saved as a decimal (for example, "0.25" to indicate that it's $0.25 the entire day) or a string of 8 decimals separated by spaces, (for example "0 0.16 0.24 0.16 0.12 0.16 0.24 0.16" to indicate that's it $0 at TOD 1, $0.16 at TOD 2468, $0.24 at TOD 37, and $0.12 at TOD 5). 

## Projects (Polyline Feature Class)

The projects are defined by their unique TIPID. 

**TIPID and COMPLETION_YEAR do not allow null values.**

|#|NAME|TYPE|LENGTH|DEFAULT|NOTE|
|---|---|---|---|---|---|
|1|**TIPID**🔑|TEXT|10|    |TIP project ID (in the format 00-00-0000)|
|2|COMPLETION_YEAR|SHORT|    |9999|Completion year of project - 9999 if not used|
|3|MCP_ID|TEXT|6|    |Major Capital Project ID number| 
|4|RSP_ID|LONG|    |    |Regionally Significant Project ID number|
|5|RCP_ID|LONG|    |    |Regional Capital Project ID number|
|6|NOTES|TEXT|255|    |Notes to self|

## Project Coding (Table)

The project coding gets its TIPID values from the projects and its ABB values from the links. The project coding rows are defined by their unique TIPID - ABB combination. 

**ABB is automatically calculated by the [[Import Highway Project Coding]] tool. Do not manually update!**

**All fields are non-nullable.**

|#|NAME|TYPE|LENGTH|DEFAULT|DOMAIN|NOTE|
|---|---|---|---|---|---|---|
|1|**TIPID**🔑🔗|TEXT|10|    |    |TIP project ID (in the format 00-00-0000)|
|2|**ABB**🔑🔗|TEXT|13|   |   |Link identifier|
|3|ACTION_CODE|TEXT|1|   |ACTION|CMAP action code|
|4|NEW_DIRECTIONS|TEXT|1|0|DIRECTIONS|New directions flag| 
|5 & 6|NEW_TYPE1 & 2|TEXT|2|0|VDF|New type flag|
|7 & 8|NEW_AMPM1 & 2|TEXT|1|0|AMPM|New AMPM flag|
|9 & 10|NEW_POSTEDSPEED1 & 2|SHORT|    |0|POSITIVE|New posted speed limit|
|11 & 12|NEW_THRULANES1 & 2|SHORT|    |0|POSITIVE|New # of driving lanes|
|13 & 14|NEW_THRULANEWIDTH1 & 2|SHORT|    |0|POSITIVE|New width of lane (feet)|
|15 & 16|ADD_PARKLANES1 & 2|SHORT|    |0|     |Add/remove parking lanes|
|17 & 18|CHANGE_PARKRES1 & 2|TEXT|2|0|PARKRES|Change parking restriction|
|19 & 20|ADD_BUSLANES1 & 2|SHORT|    |0|ADDBINARY|Add/remove bus lanes|
|21|ADD_SIGIC|SHORT|    |0|ADDBINARY|Add/remove signal interconnect flag|
|22|ADD_CLTL|SHORT|    |0|ADDBINARY|Add/remove bi-directional continuous left turn flag|
|23|ADD_RRGRADECROSS|SHORT|    |0|ADDBINARY|Add/remove at-grade railroad crossing flag|
|24|NEW_TOLLDOLLARS|TEXT|255|0|    |Change toll amount ($)|
|25|NEW_MODES|TEXT|3|0|HWYMODE|New modes permitted on link|
|26|NEW_VCLEARANCE|SHORT|    |0|VCLEARANCE|Overhead clearance (inches), 999 = ? above 162|

Fields which start with **NEW**: When a link is modified (action 1), the link field will change only if that project field is not set to 0. When a link is added (action 4), the link will become that project field value no matter what. 

The fields **NEW_VCLEARANCE** and **NEW_TOLLDOLLARS** are special in that putting the value of -1 sets the corresponding field to 0.

Fields which start with **ADD**: When a link is modified (action 1) or added (action 4), the link field will have that project field value added to it. 

Fields which start with **CHANGE**: Since the default value of the link field is "-", it acts a little differently than **NEW**. When a link is modified (action 1) or added (action 4), the link field will change only if that project field is not set to 0. 
