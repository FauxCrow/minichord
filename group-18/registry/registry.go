package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	minichord "github.com/mkyas/minichord/packages"
)

// Registry information storage
type Registry struct {
	messangers   map[int32]string  // key: random identifier (between 0–1023), value: given address
	fingerTables map[int32][]int32 // key: node associated with finger table, value: list of IDs
	mutex        sync.RWMutex      // mutex: ensures safe access to Registry (Reference: https://gobyexample.com/mutexes)
}

// List all currently registered messaging nodes’ hostname, port number, and node ID.
func (r *Registry) list() {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// Guard Clause: empty registry
	if len(r.messangers) == 0 {
		fmt.Println("The registry is currently empty.")
		return
	}

	// Header Text
	fmt.Println("\nRegistered Messaging Nodes")
	fmt.Println("\nNode ID | Hostname/IP | Port")

	// Print all rows
	for id, address := range r.messangers {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			host = address
			port = "?"
		}
		fmt.Printf("%d | %s | %s\n", id, host, port)
	}
}

// List the computed finger tables for each node in the overlay.
func (r *Registry) route() {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// Guard Clause: empty finger tables
	if len(r.fingerTables) == 0 {
		fmt.Println("The finger tables have not been setup.")
		return
	}

	// Header Text
	fmt.Println("\nOverlay Finger Tables")
	fmt.Println("\nNode ID | Peers")

	// Print all rows
	for id, peers := range r.fingerTables {
		fmt.Printf("%d | %d", id, peers)
	}
}

// Registration: Adds new messanger to Registry
func (r *Registry) AddMessanger(givenAddress string, actualAddress string) (int32, string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Guard Clause: duplicate address registration
	for _, addr := range r.messangers {
		if addr == givenAddress {
			return -1, "Node has already been registered.", fmt.Errorf("Duplicate Registration")
		}
	}

	// Guard Clause: exceeding maximum identifiers
	if len(r.messangers) >= 1024 {
		return -1, "Registry is full.", fmt.Errorf("Max Capacity")
	}

	// Generate unique and random identifier
	newID := int32(rand.Intn(1024))
	for {
		if _, exists := r.messangers[newID]; !exists { // O(1)
			break
		}
		newID = int32(rand.Intn(1024))
	}

	// Add new messanger to Registry
	r.messangers[newID] = givenAddress

	// Return RegistrationResponse -- include a message indicating the number of entries currently in its registry
	response := fmt.Sprintf("Registration successful. The number of messaging nodes currently constituting the overlay is %d", len(r.messangers))
	return newID, response, nil
}

// Deregistration: Remove messanger from Registry
func (r *Registry) RemoveMessanger(key int32, givenAddress string) (string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Guard Clause: Check if node exists in registry, and if given address matches
	if _, exists := r.messangers[key]; !exists {
		return "Key does not exist in Registry.", fmt.Errorf("Invalid Key")
	}
	// if r.messangers[key] != givenAddress {
	// 	return "Address does not match key.", fmt.Errorf("Address Mismatch")
	// }

	// Remove messanger key
	delete(r.messangers, key)

	return "Deregistration successful.", nil
}

// Sends message back to messanger node for registration
func HandleRegistration(conn net.Conn, id int32, info string) error {
	// Initialise a registration response request
	registration := &minichord.RegistrationResponse{
		Result: id,
		Info:   info,
	}

	// Create Minichord, where message is assignable to MiniChord_RegistrationResponse type
	registrationResponse := &minichord.MiniChord{
		Message: &minichord.MiniChord_RegistrationResponse{
			RegistrationResponse: registration,
		},
	}

	// Send minichord to messager using SendMiniChordMessage
	err := minichord.SendMiniChordMessage(conn, registrationResponse)
	if err != nil {
		return err
	}

	return nil
}

// Sends message back to messanger node for deregistration
func HandleDeregistration(conn net.Conn, id int32, info string) error {
	// Initialise a deregistration response request
	deregistration := &minichord.DeregistrationResponse{
		Result: id,
		Info:   info,
	}

	// Create Minichord, where message is assignable to MiniChord_DeregistrationResponse type
	deregistrationResponse := &minichord.MiniChord{
		Message: &minichord.MiniChord_DeregistrationResponse{
			DeregistrationResponse: deregistration,
		},
	}

	// Send minichord to messager using SendMiniChordMessage
	err := minichord.SendMiniChordMessage(conn, deregistrationResponse)
	if err != nil {
		return err
	}

	return nil
}

// Essentially manages one messenger node over its life cycle, to look for communication attempts
func HandleIncomingMessage(conn net.Conn, r *Registry) {
	// Check for all message types -- case switch
	for {
		message, err := minichord.ReceiveMiniChordMessage(conn)
		if err != nil {
			fmt.Println("Messenger disconnected")
			return
		}

		// Case 1: Registration
		if registerReq := message.GetRegistration(); registerReq != nil {
			actualAddress := conn.RemoteAddr().String()

			// Guard Clause: mismatch of given and actual address
			// if actualAddress != registerReq.Address {
			// 	fmt.Println("Address Mismatch")
			// }

			id, info, _ := r.AddMessanger(registerReq.Address, actualAddress)

			err := HandleRegistration(conn, id, info)
			if err != nil {
				// TODO: in the rare case that a messaging node fails just after it sends a registration request, the registry cannot communicate with it.
				// In this case, the registry removes the entry of the messaging node from the data structure maintained at the registry.
			}
			// Case 2: Deregistration
		} else if deregisterReq := message.GetDeregistration(); deregisterReq != nil {
			//actualAddress := conn.RemoteAddr().String()
			//
			// Guard Clause: mismatch of given and actual address
			// if actualAddress != deregisterReq.Address {
			// 	fmt.Println("Address Mismatch")
			// }

			info, _ := r.RemoveMessanger(deregisterReq.Id, deregisterReq.Address)

			HandleDeregistration(conn, deregisterReq.Id, info)
		}
	}
}

// Find the first node in the sorted registered IDs that is >= to a provided target value
func successor(target int, sortedIDs []int) int {
	// loop through sortedIDs
	for _, id := range sortedIDs {
		if id >= target {
			return id
		}
	}

	// If loop concludes, just give the smallest node
	return sortedIDs[0]
}

// Setup the overlay with 𝑁𝑅 entries in the finger table.
func (r *Registry) setupNR(nr int) error {
	// Ensure no one is added or removed from the registry while we calculate the finger table
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// sort the ids of all nodes -- from random to ascending order
	var sortedIDs []int // use int so we can use sort later
	for id := range r.messangers {
		sortedIDs = append(sortedIDs, int(id))
	}
	sort.Ints(sortedIDs)

	// for every node, we calculate the finger table using formula: n >= p + 2^(i-1)
	for _, nodeID := range sortedIDs {
		temp := make([]int32, nr)

		// apply formula to every slot in the finger table (corresponds to NR)
		for i := 0; i < nr; i++ {
			calculatedValue := (nodeID + (1 << i)) % 1024
			temp[i] = int32(successor(calculatedValue, sortedIDs))
		}

		// add the temp table to the registry at current nodeID
		r.fingerTables[int32(nodeID)] = temp
	}

	// install the finger table at every node -- use message NodeRegistry
	for id, table := range r.fingerTables {
		tempPeers := []*minichord.Deregistration{}
		tempIds := []int32{}

		for _, peer := range table {
			peers := &minichord.Deregistration{
				Id:      peer,
				Address: r.messangers[peer],
			}
			tempPeers = append(tempPeers, peers)
			tempIds = append(tempIds, peer)
		}

		// Initialise a registration response request
		nodeRegistry := &minichord.NodeRegistry{
			NR:    uint32(nr),
			Peers: tempPeers,
			NoIds: 10,
			Ids:   tempIds,
		}

		// Create Minichord, where message is assignable to MiniChord_DeregistrationResponse type
		nodeRegistryMessage := &minichord.MiniChord{
			Message: &minichord.MiniChord_NodeRegistry{
				NodeRegistry: nodeRegistry,
			},
		}

		// Temp: Check to see if address was saved correctly
		addr, exists := r.messangers[int32(id)]
		if !exists || addr == "" {
			fmt.Printf("Error: No address found for Node ID %d\n", id)
			continue
		}

		conn, dialErr := net.Dial("tcp", addr)
		if dialErr != nil {
			fmt.Printf("Could not connect to node %d at %s\n", id, r.messangers[int32(id)])
			continue
		}
		defer conn.Close()

		// Send minichord to messager using SendMiniChordMessage
		err := minichord.SendMiniChordMessage(conn, nodeRegistryMessage)
		if err != nil {
			return err
		}

		// Wait for the response here -- means each node will report to the registry on their status before moving to the next
		// Rationale: if one node fails, we can immedietely exit instead of waiting for all of them to complete checking their neighbours
		response, err := minichord.ReceiveMiniChordMessage(conn)
		if err != nil {
			fmt.Printf("Node %d failed to confirm setup\n", id)
			return err
		} else if result := response.GetNodeRegistryResponse(); result != nil {
			fmt.Printf("Confirmation from node %d: %d %s\n", id, result.Result, result.Info)
		}

		conn.Close()
	}

	fmt.Printf("The registry is now ready to initiate tasks.")
	return nil
}

// TODO: Helper: The start command makes the registry send the message TaskInitiate message to all nodes registered in the overlay. A command of start 50 results in each messaging node sending 50 packets to randomly chosen nodes.
func (r *Registry) Start(n int) {
	//sending a message InitiateTask control message to all nodes
}

// Main program ------------------------------------------------------------------------------------

func main() {
	// Check if port was provided as an argument
	if len(os.Args) < 2 {
		fmt.Println("No port provided. Expected command: go run registry.go <port>")
		return
	}
	port := os.Args[1]

	// Initialise the registry
	r := &Registry{
		messangers:   make(map[int32]string),
		fingerTables: make(map[int32][]int32),
	}

	// Setup connection (Reference: minichord.pdf)
	listener, _ := net.Listen("tcp", ":"+port)

	// listen for connections in separate goroutine -- so we can still look for user inputs while this runs
	go func() {
		for {
			conn, _ := listener.Accept()

			// Rationale: creating a goroutine allows us to continue listening for other messenger nodes while this one is connected, instead of waiting
			go func(c net.Conn) {
				defer c.Close()
				HandleIncomingMessage(c, r)
			}(conn)
		}
	}()

	// loop and wait for user inputs (Reference: minichord.pdf)
	reader := bufio.NewReader(os.Stdin)

	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "list":
			r.list()
		case "setup":
			fmt.Println("setup")
			r.setupNR(10)
		case "route":
			r.route()
		case "start":
			fmt.Println("start")
		default:
			fmt.Printf("Command not understood: %s", cmd)
		}
	}
}
