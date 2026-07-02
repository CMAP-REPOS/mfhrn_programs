# Tool Overview

*Author: Aaron Rumph*
*Updated: 07/02/26*

## MHN Tools
The MHN repo contains the following (12) tools:
- generate_highway_files.py
- generate_transit_files.py
- import_future_bus_routes.py
- import_highway_projects.py
- incorporate_edits.py
- update_highway_project_years.py
- export_future_network.py
- export_hwyproj_coding.py
- generate_directional_links.py
- generate_iris_correspondance_table.py
- straighten_selected_links.py
- update_mhn_base_year.py

## MFRHN tools
The MFHRN repo (at time of writing) contains the following tools:
- 1_export_future_hwys.py
- 2_generate_hwy_files.py
- 3_create_bus_layers.py
- 4_generate_transit_files.py
- import_hwyproj_coding.py

## MHN -> MFHRN Translation
(MHN) export_future_network.py -> (MFHRN) 1_export_future_hwys.py
(MHN) generate_highway_files.py -> (MFHRN) 2_generate_hwy_files.py
(MHN) generate_transit_files.py -> (MFHRN) 3_create_bus_layers.py
(MHN) import_highway_projects.py -> (MFHRN) import_hwyproj_coding.py

## MFHRN -> MHN Translation
(MFHRN) 1_export_future_hwys.py -> (MHN) export_future_network.py
(MFHRN) 2_generate_hwy_files.py -> (MHN) generate_highway_files.py
(MFHRN) 3_create_bus_layers.py -> (MHN) generate_transit_files.py
(MFHRN) import_hwyproj_coding.py -> (MHN) import_highway_projects.py

