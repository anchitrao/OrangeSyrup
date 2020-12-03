# OrangeSyrup
A parallel cloud computing framework similar to Apache Hadoop MapReduce.

Built as a final project for [Distributed Systems - CS 425 at UIUC](https://cs.illinois.edu/academics/courses/CS425)

## Academic Integrity Violation

Please refer your student code and the academic-integrity.md file for academic integrity requirements. You may use code from this project freely under the license terms, but it is **your own responsibility** to ensure that such use meets the requirements set by your course and department. I am not responsible for any academic integrity violations for use of other students copying any portion of this code.

## Getting Started

### Prerequisities

This framework was developed and tested using Docker virtual machines. Installation and setup of Docker will lead to a smoother experience during setup and usage.

#### Launching Servers

Launch `N` servers on Docker using `./start.sh N` and attach a current node to a running node by using `./attach N` for a given node `N`.

### Build

To build the framework, use `cd src` to navigate into the source directory and then run `make`.

Select a machine as the introducer node by running `./mp3` within the corresponding node's terminal.

To connect to the nodes to a network, run `./mp3 -i {IP_ADDR}`, where `IP_ADDR` is the IP Address of the introducer node within the same network.

### Running

Start the network with `./start.sh N` to get `N` containers running. Then, join the introducer and connect all remaining nodes onto the same network using the commands described in the Build section.

You can also use the command `./attach.sh N` to attach to a running instance for a given node `N`. By default the master will be set to the first node added, while all other nodes will automatically connect to master (if using the Docker setup as described above). The Introducer Node's IP address can also be set using the flag `-i {IP}`.

## Useful Commands

`put {localfile} {sdfsname}`: Upload `localfile` to SDFS with the name `sdfsname`.

`get {sdfsname} {localfile}`: Download `sdfsname` from SDFS to the file `localfile`.

`delete {sdfsname}`: Delete `sdfsname` from SDFS. This may not physically remove the files from other servers, but it will become unavailable to all nodes to download with `get`.

`ls {sdfsname}`: Show where `sdfsname` is located within SDFS.

`store`: List all files stored locally at this node.

`show master`: Returns the current master IP address.

`show tokens`: If this node is the master, this will show any in-use tokens.

`lsmembers`: show membership list

`join`: join the network (does nothing if you are the introducer or already in a group)

`mode set {gossip | all}`: change mode for the network to either gossip or all to all

`mode get`: show the current mode

`self`: show info about this node

`leave`: leave the network

## Testing Applications

There are two apps documented and used for testing as described below. Similar apps can be created following the outline of the applications in this program.

### Condorcet Application

`maple apps/condorcet_map1.py 4 prefix inputs/app1_map1`

`juice apps/condorcet_reduce1.py 4 prefix output_phase1 0`

`maple apps/condorcet_map2.py 4 prefix output_phase1`

`juice apps/condorcet_reduce2.py 4 prefix outputs 1`

Expected output:
`(A:B:C,'”No Condorcet winner, Highest Condorcet counts')`

### Traffic Light Counter

This counts the number of traffic lights at each intersection using a [Champaign Map Dataset](https://gis- cityofchampaign.opendata.arcgis.com/search?collection=Dataset) containg information about traffic lights and their locations. 

`maple apps/mapper.py N-1 prefix inputs/smalltest`

`juice apps/reducer.py N-1 prefix outputs_traffic 1`

Expected output:

```shell
root@10847bc43e5b:/home/proj/src# cd outputs_traffic/
root@10847bc43e5b:/home/proj/src/outputs_traffic# cat * | md5sum
7b17122b6f4c9cd3f7cf75ba73ccc460
```

## Contributors

* Anchit Rao
* Joseph Ravichandran
* Naveen Nathan