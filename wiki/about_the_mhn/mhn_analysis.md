## Network 

The concept of the ArcGIS network is taken from graph theory. A graph consists of edges and nodes. There are two categories of graphs: directed and undirected. Intuitively, a transportation network is suited for a directed graph, ex. when you're driving down a road, you're driving a certain direction!

![A directed graph and its undirected representation](https://github.com/CMAP-REPOS/mfhrn_programs/blob/main/docs/images/graph_example.png)

A directed graph is shown at the left of the figure. ANODE indicates the source node, and BNODE indicates the target node. The table below is the edge list of this graph. You can see that both edges 1->2 and 2->1 are stored. 

The undirected representation of that same graph is shown at the right of the figure. ANODE still indicates the source node, and BNODE still indicates the target node, but there is now a "DIRECTIONS" flag. The flag of 1 means it's one way from source to target, and the flag of 2 means it's both ways. The MHN links (hwynet_arc) basically follow this scheme, but with an additional flag of 3 which means that it's both ways but that some attribute differs between the directions (ex. a road with 3 lanes one way and 2 lanes the other). 

**NOTE: Occasionally in the MHN, there will be two links that have the same two endpoint nodes, and instead of coding it as 1 link with direction flag 2 or 3, it will be coded as 2 links each with direction flag 1, ex. US 41 / LAKE SHORE DR is separated into NB and SB arcs. Doing it like this causes the underlying undirected graph to be a multigraph. It isn't necessarily a cause for concern, but it should be minded.** 

## Projects

All highway links of the past, present, and future are saved in hwy_net. Each link is either in the state of 0 (a skeleton link) or 1 (a regular link). The traditional definition for skeleton links is that skeleton links "represent future roadway improvements" (according to the MHN wiki), but I will imagine skeleton links as being any link which does not currently exist. 

The base year is the year before any project is applied. All the regular links are stored with their base year attributes. 

The projects consist of an action code being applied to a base link. The project table is well maintained as long as the project hasn't been discontinued (completion year 9999). With my expanded definition of a skeleton link, we can represent the action codes as this: 

![A graphic describing the project action codes](https://github.com/CMAP-REPOS/mfhrn_programs/blob/main/docs/images/action_codes.png)

- **Action Code 1: Modify** - a regular link becomes a regular link with different attributes. 
- **Action Code 3: Delete** - a regular link becomes a skeleton link. 
- **Action Code 4: Add** - a skeleton link becomes a regular link. 

If you think about the action codes like this, being applied to a number of links, then the **total number of links does not change**. Each link is either in a state of 0 or 1, and only the state changes. However, the state is only allowed to change once. For example, it can't go from a regular link to a skeleton link, then back to a regular link. 

In the project table, only regular links (ABB ending with 1) are allowed to have action codes 1 and 3 applied to them; conversely, action codes 1 and 3 are only allowed to be applied to regular links. Only skeleton links (ABB ending with 0) are allowed to have action code 4 applied to them; conversely, action code 4 is only allowed to be applied to skeleton links. 

Since any project can become discontinued, **projects are independent from each other**. The action code is reflected on how it will change the base link, and no project builds on information from another project. 
