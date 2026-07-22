A diagram of the MHN is shown below, with data unrelated to buses grayed out. 

![A diagram showing the bus data relationships.](https://github.com/CMAP-REPOS/mfhrn_programs/blob/main/docs/images/bus_data.png)

## Nodes and Links

As a refresher, the primary key🔑 for nodes is **NODE**, while the primary key🔑 for links is **ABB**. See [[Highway Data]] for more information.

## Bus Runs (Polyline Feature Classes)

There are 3 bus run feature classes- base, present, and future. The bus runs are defined by their CMAP unique bus run ID (Lowercase mode + 5-digit number, starting at 00000 for base, 50000 for current, and 99000 for future).

**The fields in the following table are present for all 3 bus run feature classes.**

|#|NAME|TYPE|LENGTH|DOMAIN|NOTE|
|---|---|---|---|---|---|
|1|**TRANSIT_LINE**🔑 |TEXT|6|    |CMAP unique bus run ID|
|2|DESCRIPTION|TEXT|50|    |Description of bus route|
|3|MODE|TEXT|1|BUSMODE|Bus mode code|
|4|VEHICLE_TYPE|TEXT|2|CTVEH|Bus vehicle type code|
|5|HEADWAY|SHORT|    |POSITIVE|Headway (minutes) - see note below table|
|6|SPEED|SHORT|    |POSITIVE|Average speed (mph)|

Note: for base/current runs, HEADWAY describes the length of the TOD period that the run falls into. For example, if it's TOD 1 (8 PM - 6 AM), then the headway is 10 hours, or 600 minutes. The actual headway (duration of time between buses) is calculated later. However, for future runs, HEADWAY describes the actual headway during AM/PM peak time periods.

**These fields are specific to bus base/current and are derived from GTFS input**:

|#|NAME|TYPE|LENGTH|DOMAIN|NOTE|
|---|---|---|---|---|---|
|7|ROUTE_ID|TEXT|5|    |Actual CTA/Pace route number|
|8|~LONGNAME~|TEXT|50|    |CTA/Pace route name|
|9|DIRECTION|TEXT|5|    |Direction of travel|
|10|~TERMINAL~|TEXT|50|    |Name of bus destination|
|11|START|LONG|    |    |Start time (seconds after midnight)|
|12|STARTHOUR|SHORT|    |HOUR|Hour of the day in which the run begins (0-23)|
|13|~AM_SHARE~|DOUBLE|    |    |Proportion of run falling within the AM peak period (7 AM - 9 AM)|
|14|FEEDLINE|TEXT|20|    |Unique ID based on raw GTFS data|

The strikethrough fields (LONGNAME/ TERMINAL/ AM_SHARE) are legacy fields which do not have much use anymore. 

**These fields are specific to bus future**:

|#|NAME|TYPE|LENGTH|DEFAULT|NOTE|
|---|---|---|---|---|---|
|7|SCENARIO|TEXT|10|    |Scenarios bus line will be used in (ex. 234)|
|8|REPLACE|TEXT|50|    |MODE-ROUTE_ID (s) which will be replaced by the future project, separated by colons. Only affects specified TOD|
|9|REROUTE|TEXT|50|    |MODE-ROUTE_ID (s) which will be rerouted by the future project, separated by colons. Only affects specified TOD|
|10|TOD|TEXT|10|0|Text string of affected time periods (0 indicates all)|
|11|NOTES|TEXT|50|    |Optional additional information (such as TIPID)|

## Bus Itineraries (Tables)

There are 3 bus itinerary feature classes- base, present, and future. 

**The fields in the following table are present for all 3 bus run feature classes.**

|#|NAME|TYPE|LENGTH|DOMAIN|DEFAULT|NOTE|
|---|---|---|---|---|---|---|
|1|**TRANSIT_LINE**🔑🔗|TEXT|6|    |    |CMAP unique bus run ID|
|2|**ITIN_ORDER**🔑|SHORT|    |POSITIVE|0|Order number of bus segment in itinerary, beginning with 1|
|3|ITIN_A🔗|LONG|    |NODE|0|CMAP node number bus travels from|
|4|ITIN_B🔗|LONG|    |NODE|0|CMAP node number bus travels to|
|5|ABB🔗|TEXT|13|    |    |Unique link ID that bus travels on|
|6|~LAYOVER~|SHORT|    |POSITIVE|0|Layover time (minutes) applied to ITIN_B|
|7|DWELL_CODE|TEXT|1|DWELLCODE|0|Code for stops (corresponding Emme code), applied to ITIN_B|
|8|~ZONE_FARE~|SHORT|    |POSITIVE|0|Incremental zone fare in cents|
|9|LINE_SERV_TIME|FLOAT|    |    |0|Itinerary segment travel time (minutes)|
|10|TTF|TEXT|1|TTF|1|Emme transit time function code|
|15|F_MEAS|DOUBLE|    |    |0|Percentage of route already passed at ITIN_A (0-100)|
|16|T_MEAS|DOUBLE|    |    |0|Percentage of route already passed at ITIN_B (0-100)|

The strikethrough field LAYOVER is not used anymore (the value is always 3 minutes on the last stop on a line) and ZONE_FARE only applies to Metra. 

**These fields are specific to bus base/current and are derived from GTFS input**:

|#|NAME|TYPE|LENGTH|DOMAIN|DEFAULT|NOTE|
|---|---|---|---|---|---|---|
|11|LINK_STOPS|SHORT|   |POSITIVE|0|Number of GTFS stops along link|
|12|~IMPUTED~|TEXT|1|IMPUTED|0|Flag indicating if segment was imputed|
|13|DEP_TIME|LONG|    |    |0|Time departing from ITIN_A (seconds since midnight)|
|14|ARR_TIME|LONG|    |    |0|Time arriving to ITIN_B (seconds since midnight)|

## Park n' Ride (Table)

This table stores information about park-n-ride locations serving Pace and CTA bus riders.

|#|NAME|TYPE|LENGTH|DOMAIN|NOTE|
|---|---|---|---|---|---|
|1|FACILITY|TEXT|255|    |Name of park-n-ride facility|
|2|NODE🔗|LONG|    |NODE|CMAP node number|
|3|COST|SHORT|   |POSITIVE|Daily cost of parking at facility (cents)|
|4|SPACES|SHORT|    |POSITIVE|Number of parking spaces at facility|
|5|ESTIMATE|SHORT|   |BINARY|If data is verified (0) or just an estimate (1)|
|6|SCENARIO|TEXT|20|    |Scenarios park-n-ride lot will be used in. Cannot be blank|
