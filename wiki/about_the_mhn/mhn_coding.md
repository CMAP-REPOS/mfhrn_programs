## Node Coding

Several domains are coded into the MHN to specifically control for the node attributes.

### NODE

Node IDs in the MHN are restricted to <= 29999 (as nodes >= 30000 are reserved for the MRN).

Node IDs <= 5000 have a special meaning. Of the node IDs in the MHN, 1-3632 are special nodes which refer to zone centroids. 3633-3649 refer to point-of-entries (POEs). 

### SUBZONE

(From the travel model appendix) "Trip generation zones (or subzones) are the smallest level of geography used in the travel demand model. Subzones are quarter-section-sized geographies that CMAP uses for household and employment forecasting." There are 17418 subzones. A subzone of 0 means that there is no corresponding subzone for that node. 

### ZONE

There are 3649 modeling zones. 1-3632 are part of the CMAP region, while 3633-3649 are POEs (corresponding to the node IDs). The modeling zones were built by aggregating the subzones. A zone of 9999 means that there is no corresponding zone for that node. 

### CAPZONE

The capacity zones were also built by aggregating the subzones. 

|Code|Description|
|---|---|
|**0**|Unassigned|
|**1**|Chicago CBD|
|**2**|Chicago central area|
|**3**|Chicago|
|**4**|Inner-ring suburbs|
|**5**|Chicago urbanized area (IL)|
|**6**|Chicago urbanized area (IN)|
|**7**|Other urbanized area (IL)|
|**8**|Other urbanized area (IN)|
|**9**|Rural CMAP region|
|**10**|Rural Lake County (IN)|
|**11**|External area|
|**99**|Point of entry|

## Link Coding

While physically, a road does not change throughout the day, the attributes of a road (such as the toll) might vary depending on the time-of-day (TOD). The MHN is set up so that the attributes are flexible enough to accommodate those variations. 

|TOD|Description|
|---|---|
|**1**|Overnight: 8 PM - 6 AM|
|**2**|AM start shoulder: 6 AM - 7 AM|
|**3**|AM peak: 7 AM - 9 AM|
|**4**|AM end shoulder: 9 AM - 10 AM|
|**5**|Midday: 10 AM - 2 PM|
|**6**|PM start shoulder: 2 PM - 4 PM|
|**7**|PM peak: 4 PM - 6 PM|
|**8**|PM end shoulder: 6 PM - 8 PM|

Several domains are coded into the MHN to specifically control for the link attributes. A few of these domains are expounded on below.

### VDF

The facility type determines the volume displacement function. 

|Code|Description|
|---|---|
|**1**|Arterial|
|**2**|Freeway (controlled-access)|
|**3**|Freeway-Arterial Ramp|
|**4**|Expressway (limited-access)|
|**5**|Freeway-Freeway Ramp|
|**6**|Centroid Connector|
|**7**|Toll Plaza|
|**8**|Metered Ramp|

Centroid connectors are a special kind of link connecting the zone centroids to the highway network. They do not physically exist.

Toll plazas indicate a fixed-cost toll. Otherwise the toll is distance-based. 

### AMPM

Some roads are only open during a certain part of the day. 

|Code|Description|Open|
|---|---|---|
|**0**|N/A|   |
|**1**|Unrestricted|12345678|
|**2**|AM Only|2345|
|**3**|PM Only|1678|
|**4**|Off-Peak Only|15|
|**5**|Peak/Shoulder|234678|


### HWYMODE

The field "MODES" is a 3-digit code. It's a combination of the mode (1-digit) and the truck restriction (2-digit). It describes what kind of vehicles are allowed on the link. 

|Code|Description|Allowed|
|---|---|---|
|**0**|N/A|   |
|**100**|All vehicles|ASHThmlb|
|**2XX**|All vehicles + truck restrictions|Varies|
|**300**|Trucks only|AThmlb|
|**400**|Transit only (only called for transit networks)|None|
|**500**|HOV only|AH|

The mode 2XX indicates that there is a truck restriction applied by XX. A truck restriction of 0 (code 200) indicates that the truck restriction is unknown. Otherwise, codes vary from 1-49. To see their meanings you can just look in the geodatabase because I will not be typing it all out. 

Also, I cannot believe I need a table for this table but:

|Mode|Description|
|---|---|
|**A**|Primary auto|
|**S**|Single occupant auto (SOV)|
|**H**|High occupancy (HOV)|
|**T**|General truck|
|**b**|B-plate truck|
|**l**|Light truck|
|**m**|Medium truck|
|**h**|Heavy truck|
