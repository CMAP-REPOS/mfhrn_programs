This page is an update of the [MHN Wiki Page](http://wiki.cmap.local/mediawiki/index.php/MHN).

![A diagram of the schema of the MHN](https://github.com/CMAP-REPOS/mfhrn_programs/blob/main/docs/images/mhn_schema.png)

Rectangles with rounded corners indicate feature classes (located in the hwynet dataset), rectangles with sharp corners indicate tables, solid lines indicate relationship classes, and dashed lines indicate logical connections. 

|#|Name|Data Type|#|Name|Data Type|
|---|---|---|---|---|---|
|1|**hwynet_node**|Feature Class (Point)|11|**parknride**|Table|
|2|**hwynet_link**|Feature Class (Line)|12|**rel_hwyproj_to_coding**|Relationship Class (Composite)| 
|3|**hwyproj**|Feature Class (Line)|13|**rel_arcs_to_hwyproj_coding**|Relationship Class (Simple)|
|4|**hwyproj_coding**|Table|14|**rel_bus_base_to_itin**|Relationship Class (Composite)|
|5|**bus_base**|Feature Class (Line)|15|**rel_bus_current_to_itin**|Relationship Class (Composite)|
|6|**bus_current**|Feature Class (Line)|16|**rel_bus_future_to_itin**|Relationship Class (Composite)|
|7|**bus_future**|Feature Class (Line)|17|**rel_arcs_to_bus_base_itin**|Relationship Class (Simple)|
|8|**bus_base_itin**|Table|18|**rel_arcs_to_bus_current_itin**|Relationship Class (Simple)|
|9|**bus_current_itin**|Table|19|**rel_arcs_to_bus_future_itin**|Relationship Class (Simple)|
|10|**bus_future_itin**|Table|20|**rel_nodes_to_parknride**|Relationship Class (Simple)|

We can think of the MHN in two layers. Layer 1 is the highway layer, and Layer 2 is the bus layer which is built on top of the highway layer. Logically, a road is a road no matter what, but a bus route is determined by the roads the bus can drive on. We want the schema to reflect this. 

- [[Highway Data]] 
    - `hwynet/hwynet_arc` and `hwynet/hwynet_node` determine the geometry 
    - `hwynet/hwyproj`, and `hwyproj_coding` determines the way the links will change
- [[Bus Data]] 
    - `hwynet/hwynet_arc` and `hwynet/hwynet_node` determine the geometry (same as above)
    - `bus_base`, `bus_current`, and `bus_future` are the bus runs 
    - `bus_base_itin`, `bus_current_itin`, and `bus_future_itin` are the bus runs fitted to the geometry 
    - `parknride` describes Pace/CTA park-n-ride locations 
- [[MHN Coding]] - elaboration on the meaning of the coding in the MHN
- [[MHN Analysis]] - additional analysis on the MHN
