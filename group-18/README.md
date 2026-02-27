### How to Run Program: 

To run the program, you must first create a registry node by running the following commands with your chosen port number: 

```
cd registry

go run registry.go $PORT_NUMBER
``` 

Then, create messenger nodes by running the bash script with `./network_test.sh`. Take note to adjust the variables inside the bash script to fit your chosen port number and number of nodes required. 

To verify that all messenger nodes have been created, run the command `list` in the registry terminal. Once verified, you can run `setup $NODE_NUM` then `start $PACKET_NUM` to begin the message passing. 

### File Description: 

The submission contains the following files and folders: 

- registry/registry.go: The go file containing all logic for the registry node. 

- messenger/messenger.go: The go file containing all logic for the messenger node. 

- network_test.sh: A bash file that creates a specified number of messenger nodes. 

- go.mod: A module definition file containing parent project ‘github.com/mkyas/minichord’. 

- go.sum: A file that stores the expected cryptographic checksums of all module content. 

- README: A text file describing the program and justification for program design. 

### Program Justification: 

#### Registry Node 

The registry serves as the main storage of information for all messenger nodes in the network, storing this information in a Registry struct. The registry struct also contains a read write mutex using `sync.RWMutex`, which we will use to ensure only one process accesses the registry at a time, ensuring mutual exclusion. 

##### ID Allocation 

When a messenger node communicates its interest in registering, the Registry handles this request in the AddMessanger() function on line 76. Here, it chooses a random ID number for the node and ensures that the selected number is unique by comparing to r.messengers in the Registry struct. Only unique identifiers will be selected and saved to the registry. 

##### Avoiding Partitions 

When a user sets up the network, we ensure all nodes can reach each other by calculating and establishing finger tables in each messenger node from the registry. The overlay network is structured as a chord, where each messenger node will store links of up to the specified setup number according to the formula: n >= p + 2^(i-1). 

This way, every messenger node can eventually talk to every other node even if they are not in its finger table through hops, where every node listens for NodeData in HandleIncomingMessages() on line 228, and calculates the next best hop to send the data to if it is not the final destination of the data. 

#### Messenger Node 

The messenger are individual nodes that intend to communicate to other messengers. To add them to the network, they register with the registry node on start. 

##### Delivering Packets 

Packets are created as NodeData structs, which contain information on its source, destination, payload, hops, and trace. Each messenger node will send a specified number of packets on start, calculating the best hop to send it to for every round. They will also listen for NodeData, either storing the information if it is the destination or sending the packet on its way. 

##### Avoid Duplication 

Packet destinations and payload are generated randomly in every round, where every messenger node cannot send packets to itself. By using a random generation of payload from from 2147483647 to −214748364, we greatly reduce the likelihood of a duplicated packet. 

 

 

##### Task summary 

Able to retrieve the completed task summary from messaging nodes 

``` 

2026/02/27 15:42:41 ReceiveMiniChordMessage(): received taskFinished:{Id:26 Address:"127.0.0.1:37531"} ([194 1 22 10 15 49 50 55 46 48 46 48 46 49 58 51 55 53 51 49 21 26 0 0 0]), 25 from 127.0.0.1:37684 

Task completed from node 26 (9/10) 

```

##### Traffic summary 

Traffic summary was collated and formatted using the example table as reference. 

 

``` 

Traffic summary from node 449 (10/10) 

Node,Sent,Received,Relayed,TotalSent,TotalReceived 

808,25000,24886,100127,-43729033849,-3311573607 

880,25000,24840,22249,-21889918384,279060697355 

890,25000,25007,0,-32615798338,-74126272190 

843,25000,25309,0,7572957088,437566741809 

813,25000,24743,0,171831480372,-101144135636 

586,25000,24961,100013,124533921397,12568746613 

449,25000,25062,0,238318154507,57890512989 

408,25000,25230,0,302357696744,279879947513 

346,25000,24856,100266,-47526537959,-280869805353 

26,25000,25106,100122,227691927875,319029989960 

Sum,250000,250000,422777,926544849453,926544849453 

``` 

##### Deadlocks 

The messenger program and the registry program only uses one mutex each to protect the shared resources in the critical sections. Since there is only one lock, there can be no circular-waiting for resources and Coffman’s condition for deadlock is broken for both programs. 

 

 
