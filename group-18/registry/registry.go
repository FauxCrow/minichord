package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	minichord "github.com/mkyas/minichord/packages"
)

// Registry information storage
type Registry struct {
	messangers   map[int32]string  // key: random identifier (between 0–1023), value: given address
	fingerTables map[int32][]int32 // key: node associated with finger table, value: list of IDs
	mutex        sync.RWMutex      // mutex: ensures safe access to Registry (Reference: https://gobyexample.com/mutexes)

	taskDone  map[int32]bool
	summaries map[int32]*minichord.TrafficSummary
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
		fmt.Printf("%d | %v\n", id, peers)
	}
}

// Registration: Adds new messanger to Registry
func (r *Registry) AddMessanger(givenAddress string) (int32, string, error) {
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
	if r.messangers[key] != givenAddress {
		return "Address does not match key.", fmt.Errorf("mismatched address")
	}

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

// compare host only
func checkSameHost(a string, b string) bool {
	ha, _, errA := net.SplitHostPort(a)
	hb, _, errB := net.SplitHostPort(b)
	if errA != nil || errB != nil {
		return false
	}
	return ha == hb
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

			if !checkSameHost(actualAddress, registerReq.Address) {
				HandleRegistration(conn, -1, "Registration host mismatch")
				return
			}

			id, info, addErr := r.AddMessanger(registerReq.Address)
			if addErr != nil {
				HandleRegistration(conn, -1, info)
				return
			}

			if err := HandleRegistration(conn, id, info); err != nil {
				// In the rare case that a messaging node fails just after it sends a registration request, the registry cannot communicate with it.
				// In this case, the registry removes the entry of the messaging node from the data structure maintained at the registry.
				fmt.Printf("Failed to send RegistrationResponse to %s: %v. Cleaning up...\n", registerReq.Address, err)
				r.RemoveMessanger(id, registerReq.Address)
			}

			// Case 2: Deregistration
		} else if deregisterReq := message.GetDeregistration(); deregisterReq != nil {
			actualAddress := conn.RemoteAddr().String()

			if !checkSameHost(actualAddress, deregisterReq.Address) {
				HandleDeregistration(conn, -1, "De-registration host mismatch")
				return
			}

			info, remErr := r.RemoveMessanger(deregisterReq.Id, deregisterReq.Address)
			if remErr != nil {
				HandleDeregistration(conn, -1, info)
				return
			}
			HandleDeregistration(conn, deregisterReq.Id, info)

			// task finished
		} else if tf := message.GetTaskFinished(); tf != nil {
			r.completedTask(tf)

			// traffic summary
		} else if ts := message.GetReportTrafficSummary(); ts != nil {
			r.handleTrafficSummary(ts)
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
	// guard input
	if nr <= 0 {
		return fmt.Errorf("nr must be > 0")
	}
	if len(r.messangers) == 0 {
		return fmt.Errorf("no registered nodes")
	}
	if nr > len(r.messangers)-1 {
		return fmt.Errorf("nr cannot be larger than number of registered nodes")
	}

	// Ensure no one is added or removed from the registry while we calculate the finger table
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// sort the ids of all nodes -- from random to ascending order
	var sortedIDs []int // use int so we can use sort later
	for id := range r.messangers {
		sortedIDs = append(sortedIDs, int(id))
	}
	sort.Ints(sortedIDs)

	// incase there are older entries
	r.fingerTables = make(map[int32][]int32, len(sortedIDs))

	// build the list of overlayIDs
	overlayIDs := make([]int32, 0, len(sortedIDs))
	for _, id := range sortedIDs {
		overlayIDs = append(overlayIDs, int32(id))
	}

	// for every node, we calculate the finger table using formula: n >= p + 2^(i-1)
	for _, nodeID := range sortedIDs {
		temp := make([]int32, nr)

		// apply formula to every slot in the finger table (corresponds to NR)
		// should not connect to itself, skip itself
		for i := 0; i < nr; i++ {
			calculatedValue := (nodeID + (1 << i)) % 1024
			next := int32(successor(calculatedValue, sortedIDs))
			if next == int32(nodeID) {
				pos := 0
				for pos < len(sortedIDs) && sortedIDs[pos] != nodeID {
					pos++
				}
				next = int32(sortedIDs[(pos+1)%len(sortedIDs)])
			}
			temp[i] = next
		}

		// add the temp table to the registry at current nodeID
		r.fingerTables[int32(nodeID)] = temp
	}

	// install the finger table at every node -- use message NodeRegistry
	for id, table := range r.fingerTables {
		tempPeers := []*minichord.Deregistration{}

		for _, peer := range table {
			peers := &minichord.Deregistration{
				Id:      peer,
				Address: r.messangers[peer],
			}
			tempPeers = append(tempPeers, peers)
		}

		// Initialise a registration response request
		nodeRegistry := &minichord.NodeRegistry{
			NR:    uint32(nr),
			Peers: tempPeers,
			NoIds: uint32(len(overlayIDs)),
			Ids:   overlayIDs,
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

		// Send minichord to messager using SendMiniChordMessage
		err := minichord.SendMiniChordMessage(conn, nodeRegistryMessage)
		if err != nil {
			conn.Close()
			return err
		}

		// Wait for the response here -- means each node will report to the registry on their status before moving to the next
		// Rationale: if one node fails, we can immedietely exit instead of waiting for all of them to complete checking their neighbours
		response, err := minichord.ReceiveMiniChordMessage(conn)
		conn.Close()
		if err != nil {
			fmt.Printf("Node %d failed to confirm setup\n", id)
			return err
		} else if result := response.GetNodeRegistryResponse(); result != nil {
			fmt.Printf("Confirmation from node %d: %d %s\n", id, result.Result, result.Info)

			if result.Result < 0 {
				return fmt.Errorf("node %d failed overlay setup: %s", id, result.Info)
			}
		} else {
			return fmt.Errorf("node %d did not send NodeRegistryResponse", id)
		}

	}

	fmt.Printf("The registry is now ready to initiate tasks.\n")
	return nil
}

// The start command makes the registry send the message TaskInitiate message to all nodes registered in the overlay. A command of start 50 results in each messaging node sending 50 packets to randomly chosen nodes.
func (r *Registry) Start(n int) {
	//sending a message InitiateTask control message to all nodes
	if n <= 0 {
		fmt.Println("start count must be > 0")
		return
	}

	// san check states
	r.mutex.Lock()
	r.taskDone = make(map[int32]bool)
	r.summaries = make(map[int32]*minichord.TrafficSummary)
	r.mutex.Unlock()

	r.mutex.RLock()

	// create list of addresses to send instruction to InitiateTask
	addrByID := make(map[int32]string, len(r.messangers))
	for id, addr := range r.messangers {
		addrByID[id] = addr
	}

	r.mutex.RUnlock()

	// send intruction to all nodes sequentially
	for id, addr := range addrByID {
		task := &minichord.InitiateTask{
			Packets: uint32(n),
		}

		msg := &minichord.MiniChord{
			Message: &minichord.MiniChord_InitiateTask{
				InitiateTask: task,
			},
		}

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("start | dial error for node %d at %s: %v\n", id, addr, err)
			continue
		}

		err = minichord.SendMiniChordMessage(conn, msg)
		conn.Close()
		if err != nil {
			fmt.Printf("start | send error for node %d at %s: %v\n", id, addr, err)
		}
	}
}

func (r *Registry) completedTask(tf *minichord.TaskFinished) {
	r.mutex.Lock()
	r.taskDone[tf.Id] = true

	done := len(r.taskDone)
	total := len(r.messangers)
	r.mutex.Unlock()

	fmt.Printf("Task completed from node %d (%d/%d)\n", tf.Id, done, total)

	if done == total && total > 0 {
		r.requestTrafficSummaries()
	}
}

func (r *Registry) requestTrafficSummaries() {
	r.mutex.RLock()
	addrByID := make(map[int32]string, len(r.messangers))
	for id, addr := range r.messangers {
		addrByID[id] = addr
	}

	r.mutex.RUnlock()

	for id, addr := range addrByID {
		msg := &minichord.MiniChord{
			Message: &minichord.MiniChord_RequestTrafficSummary{
				RequestTrafficSummary: &minichord.RequestTrafficSummary{},
			},
		}

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("summary | dial error for node %d at %s: %v\n", id, addr, err)
			continue
		}

		err = minichord.SendMiniChordMessage(conn, msg)
		if err != nil {
			fmt.Printf("summary | send error for node %d at %s: %v\n", id, addr, err)
		}

		conn.Close()
	}
}

func (r *Registry) handleTrafficSummary(ts *minichord.TrafficSummary) {
	r.mutex.Lock()
	r.summaries[ts.Id] = ts

	count := len(r.summaries)
	total := len(r.messangers)
	r.mutex.Unlock()

	fmt.Printf("Traffic summary from node %d (%d/%d)\n", ts.Id, count, total)

	if count == total && total > 0 {
		r.outputTrafficSummaries()
	}
}

func (r *Registry) outputTrafficSummaries() {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	fmt.Println("Node,Sent,Received,Relayed,TotalSent,TotalReceived")

	var totalSent uint32
	var totalRecv uint32
	var totalRelay uint32
	var totalSumSent int64
	var totalSumRecv int64

	for id, ts := range r.summaries {
		fmt.Printf("%d,%d,%d,%d,%d,%d\n",
			id,
			ts.Sent,
			ts.Received,
			ts.Relayed,
			ts.TotalSent,
			ts.TotalReceived,
		)

		totalSent += ts.Sent
		totalRecv += ts.Received
		totalRelay += ts.Relayed
		totalSumSent += ts.TotalSent
		totalSumRecv += ts.TotalReceived

	}

	fmt.Printf("Sum,%d,%d,%d,%d,%d\n",
		totalSent,
		totalRecv,
		totalRelay,
		totalSumSent,
		totalSumRecv,
	)

	if totalSent != totalRecv {
		fmt.Printf("total sent (%d) != total received (%d)\n", totalSent, totalRecv)
	}

	if totalSumSent != totalSumRecv {
		fmt.Printf("total sum sent (%d) != total sum received (%d)\n", totalSumSent, totalSumRecv)
	}

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
		taskDone:     make(map[int32]bool),
		summaries:    make(map[int32]*minichord.TrafficSummary),
	}

	// Setup connection (Reference: minichord.pdf)
	listener, listErr := net.Listen("tcp", ":"+port)
	if listErr != nil {
		fmt.Println("listen error:", listErr)
		return
	}

	// listen for connections in separate goroutine -- so we can still look for user inputs while this runs
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("accept error:", err)
				continue
			}

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

		// guard input
		tok := strings.Fields(cmd)
		if len(tok) == 0 {
			continue
		}

		switch tok[0] {

		case "list":
			r.list()

		case "setup":
			if len(tok) < 2 {
				fmt.Println("usage: setup [nr]")
				continue
			}
			nr, err := strconv.Atoi(tok[1])
			if err != nil {
				fmt.Println("invalid nr:", err)
				continue
			}

			if err := r.setupNR(nr); err != nil {
				fmt.Println("setup error:", err)
			}

		case "route":
			r.route()

		case "start":
			if len(tok) < 2 {
				fmt.Println("usage: start [packets]")
				continue
			}
			n, err := strconv.Atoi(tok[1])
			if err != nil {
				fmt.Println("invalid start count:", err)
				continue
			}

			r.Start(n)

		default:
			fmt.Printf("Command not understood: %s\n", cmd)
		}
	}
}
