package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"

	minichord "github.com/mkyas/minichord"
)

// Node information storage
type Node struct {
	id              int32                       // stores assigned id from registry
	address         string                      // stores address used by registry
	registryAddress string                      // stores the registry's adress
	fingerTable     []*minichord.Deregistration // list of IDs
	allIDs          []int32

	sendTracker      uint32 // number of data packets sent by that node
	receiveTracker   uint32 // number of received packets
	relayTracker     uint32 // number of packets the node relays
	sendSummation    int64  // continuously sums the values of the random numbers sent
	receiveSummation int64  // accumulates the values of the payloads received

	mut sync.Mutex // guard read writes for messanger, block everyone else when using the shared variable
}

// handles creation and sending of registration message using protobuf
func RegisterSelf(conn net.Conn, address string) {
	// Initialise a registration request
	registerReq := &minichord.Registration{
		Address: address,
	}

	// Create Minichord, where message is assignable to MiniChord_Registration type
	registration := &minichord.MiniChord{
		Message: &minichord.MiniChord_Registration{
			Registration: registerReq,
		},
	}

	// Send minichord to register using SendMiniChordMessage
	err := minichord.SendMiniChordMessage(conn, registration)
	if err != nil {
		fmt.Println("Failure registering node from: " + address)
	}
}

// handles creation and sending of deregistration message using protobuf
func DeregisterSelf(conn net.Conn, n *Node) {
	// Initialise a deregistration request
	n.mut.Lock()
	id := n.id
	address := n.address
	n.mut.Unlock()

	deregisterReq := &minichord.Deregistration{
		Id:      id,
		Address: address,
	}

	// Create Minichord, where message is assignable to MiniChord_Deregistration type
	deregistration := &minichord.MiniChord{
		Message: &minichord.MiniChord_Deregistration{
			Deregistration: deregisterReq,
		},
	}

	// Send minichord to register using SendMiniChordMessage
	err := minichord.SendMiniChordMessage(conn, deregistration)
	if err != nil {
		fmt.Println("Failure deregistering node from: " + n.address)
	}
}

// san check
func (n *Node) runTest(packets int) {
	for i := 0; i < packets; i++ {
		payload := rand.Int31()

		n.mut.Lock()

		n.sendTracker++
		n.sendSummation += int64(payload)

		n.mut.Unlock()
	}

	n.sendTaskFinished()
}

// Each node participates in a set of rounds. Each round involves sending a packet to a randomly chosen node (excluding itself) from the set of all registered nodes advertised in the message NodeRegistry.
// When sending a data packet, the source node consults its finger table to decide which link to send it over
// The payload of each data packet is a random integer with values that range from 2147483647 to −2147483648.
// Each node sends one packet during each round.
func (n *Node) sendPackets(packets int) {
	// each loop represents a round
	for i := 0; i < packets; i++ {
		n.mut.Lock()

		ids := append([]int32(nil), n.allIDs...)
		selfID := n.id

		n.mut.Unlock()

		if len(ids) <= 1 {
			fmt.Println("Not enough nodes")
			break
		}

		// Pick a random target until it is not itself
		var targetID int32
		for {
			targetID = ids[rand.Intn(len(ids))]
			if targetID != selfID {
				break
			}
		}

		// choose random payload number and update statistics
		payload := int32(rand.Uint32())
		// choose who to send this packet to using finger table
		hopAddr := n.FindBestTarget(targetID)
		if hopAddr == "" {
			fmt.Println("No valid hop, table does not exist")
			continue
		}

		// sends a NodeData to next node
		var hops []int32
		data := &minichord.NodeData{
			Destination: targetID,
			Source:      selfID,
			Payload:     payload,
			Hops:        0,
			Trace:       hops,
		}

		dataPacket := &minichord.MiniChord{
			Message: &minichord.MiniChord_NodeData{
				NodeData: data,
			},
		}

		// Open connection to that node
		conn, dialErr := net.Dial("tcp", hopAddr)
		if dialErr != nil {
			fmt.Printf("Failed to dial heop %s: %v\n", hopAddr, dialErr)
			continue
		}

		err := minichord.SendMiniChordMessage(conn, dataPacket)
		conn.Close()
		if err != nil {
			fmt.Printf("Failed to continue hop to %d", targetID)
			continue
		}

		// increment after sending
		n.mut.Lock()
		n.sendTracker++
		n.sendSummation += int64(payload)
		n.mut.Unlock()

	}
	n.sendTaskFinished()
}

func clockwiseDistance(from int32, to int32) int32 {
	if to >= from {
		return to - from
	}
	return (1024 - from) + to
}

func (n *Node) FindBestTarget(targetID int32) string {
	n.mut.Lock()

	selfID := n.id
	table := append([]*minichord.Deregistration(nil), n.fingerTable...)

	n.mut.Unlock()

	if len(table) == 0 {
		return ""
	}

	// exact match
	for _, peer := range table {
		if peer.Id == targetID {
			return peer.Address
		}
	}

	calculateDist := clockwiseDistance(selfID, targetID)

	bestPeer := table[0]
	bestLeftover := int32(1<<30 - 1)

	for _, peer := range table {
		peerDist := clockwiseDistance(selfID, peer.Id)

		// skip 0 dist
		if peerDist == 0 {
			continue
		}

		// dont overshoot
		if peerDist > calculateDist {
			continue
		}

		remaining := clockwiseDistance(peer.Id, targetID)
		if remaining < bestLeftover {
			bestLeftover = remaining
			bestPeer = peer
		}
	}

	return bestPeer.Address
}

func HandleIncomingMessage(conn net.Conn, n *Node) {
	for {
		message, err := minichord.ReceiveMiniChordMessage(conn)
		if err != nil {
			fmt.Println("Messenger disconnected")
			return
		}

		// Case 1: Setup - Node Overlay
		if nodeRegistry := message.GetNodeRegistry(); nodeRegistry != nil {
			fmt.Printf("\nReceived updated finger table with %d entries.\n", nodeRegistry.NR)
			// copy the values and try not to hold, i suspect it might block for too long
			peers := append([]*minichord.Deregistration(nil), nodeRegistry.Peers...)
			ids := append([]int32(nil), nodeRegistry.Ids...)

			n.mut.Lock()

			n.fingerTable = peers
			n.allIDs = ids

			n.mut.Unlock()

			// Initiate connections to the nodes that comprise its finger table, and track how many succeed
			success := 0
			for _, peer := range peers {
				pConn, pconerr := net.Dial("tcp", peer.Address)
				if pconerr != nil {
					fmt.Printf("Failed to connect to peer %d at %s: %v\n", peer.Id, peer.Address, pconerr)
					continue
				}
				pConn.Close()
				success++
			}

			// Initialise a node registry response
			result := int32(0)
			info := "Received"

			if success != len(peers) {
				result = -1
				info = "Unable to connect to all finger table peers"
			}

			nodeRegistryReq := &minichord.NodeRegistryResponse{
				Result: result,
				Info:   info,
			}

			// Create Minichord, where message is assignable to MiniChord_Deregistration type
			nodeRegistryResponse := &minichord.MiniChord{
				Message: &minichord.MiniChord_NodeRegistryResponse{
					NodeRegistryResponse: nodeRegistryReq,
				},
			}

			// Send minichord to register using SendMiniChordMessage
			err := minichord.SendMiniChordMessage(conn, nodeRegistryResponse)
			if err != nil {
				fmt.Println("Failure informing on node registry setup from: " + n.address)
			}
		} else if summary := message.GetRequestTrafficSummary(); summary != nil {
			n.sendTrafficSummary()
		} else if task := message.GetInitiateTask(); task != nil {
			fmt.Printf("Received InitiateTask: %d packets \n", task.Packets)
			//go n.runTest(int(task.Packets))
			go n.sendPackets(int(task.Packets))
		} else if data := message.GetNodeData(); data != nil {

			n.mut.Lock()

			selfID := n.id

			n.mut.Unlock()
			// Final destination -- update stats
			if data.Destination == selfID {
				n.mut.Lock()
				n.receiveTracker++
				n.receiveSummation += int64(data.Payload)
				n.mut.Unlock()
			} else {
				n.mut.Lock()
				n.relayTracker++
				n.mut.Unlock()

				data.Hops++
				data.Trace = append(data.Trace, selfID)

				// Find next best hop and send it on its merry way
				hopAddr := n.FindBestTarget(data.Destination)
				// guard against empty table cause there is a return "" in registry
				if hopAddr == "" {
					fmt.Println("No valid hop, table does not exist")
					return
				}

				// Open connection to that node
				conn, conErr := net.Dial("tcp", hopAddr)
				if conErr != nil {
					fmt.Printf("Dail failed for next node %s: %v\n", hopAddr, conErr)
					return
				}

				dataPacket := &minichord.MiniChord{
					Message: &minichord.MiniChord_NodeData{
						NodeData: data,
					},
				}

				err := minichord.SendMiniChordMessage(conn, dataPacket)
				conn.Close()
				if err != nil {
					fmt.Printf("Failed to continue hop to %d: %v\n", data.Destination, err)
				}
			}
		}
	}
}

// Print information to the console about the number of messages sent, received, and relayed, along with the sums for the messages sent from and received at the node.
func Print(n *Node) {
	n.mut.Lock()

	id := n.id
	sendTracker := n.sendTracker
	receiveTracker := n.receiveTracker
	relayTracker := n.relayTracker
	sendSummation := n.sendSummation
	receiveSummation := n.receiveSummation

	n.mut.Unlock()

	fmt.Printf("Node %d\n", id)
	fmt.Printf("Sent: %d\n", sendTracker)
	fmt.Printf("Received: %d\n", receiveTracker)
	fmt.Printf("Relayed: %d\n", relayTracker)
	fmt.Printf("Sum Sent: %d\n", sendSummation)
	fmt.Printf("Sum Received: %d\n", receiveSummation)
}

// helpers after node finishes sending assigned packets, to send data to registry
func (n *Node) sendTaskFinished() error {
	conn, err := net.Dial("tcp", n.registryAddress)
	if err != nil {
		return err
	}
	n.mut.Lock()
	id := n.id
	address := n.address
	n.mut.Unlock()

	defer conn.Close()

	tf := &minichord.TaskFinished{
		Id:      id,
		Address: address,
	}

	msg := &minichord.MiniChord{
		Message: &minichord.MiniChord_TaskFinished{
			TaskFinished: tf,
		},
	}

	return minichord.SendMiniChordMessage(conn, msg)
}

func (n *Node) sendTrafficSummary() error {
	fmt.Printf("Attempting to send traffic summary...\n")

	conn, err := net.Dial("tcp", n.registryAddress)
	if err != nil {
		fmt.Println("sendTrafficSummary | dial error")
		return err
	}

	defer conn.Close()

	n.mut.Lock()
	// copy values over
	id := n.id
	sent := n.sendTracker
	received := n.receiveTracker
	relayed := n.relayTracker
	TotalSent := n.sendSummation
	TotalReceived := n.receiveSummation

	n.mut.Unlock()

	ts := &minichord.TrafficSummary{
		Id:            id,
		Sent:          sent,
		Received:      received,
		Relayed:       relayed,
		TotalSent:     TotalSent,
		TotalReceived: TotalReceived,
	}

	msg := &minichord.MiniChord{
		Message: &minichord.MiniChord_ReportTrafficSummary{
			ReportTrafficSummary: ts,
		},
	}

	err = minichord.SendMiniChordMessage(conn, msg)
	if err != nil {
		fmt.Println("traffic summary send chord error")
		return err
	}

	n.mut.Lock()

	// reset counters after sending
	n.sendTracker = 0
	n.receiveTracker = 0
	n.relayTracker = 0
	n.sendSummation = 0
	n.receiveSummation = 0

	n.mut.Unlock()

	return nil
}

// Allows a messaging node to exit the overlay. The messaging node should first send a deregistration message (see Section 2.2) to the registry and await a response before exiting and terminating the process.
func (n *Node) Exit() error {
	conn, err := net.Dial("tcp", n.registryAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	DeregisterSelf(conn, n)

	// Registeration - await confirmation that it has been deregistered
	deregResponse, deregErr := minichord.ReceiveMiniChordMessage(conn)
	if deregErr != nil {
		fmt.Println("Error receiving response:", deregErr)
		return deregErr
	}

	// Check if connection works
	if response := deregResponse.GetDeregistrationResponse(); response != nil {
		if response.Result < 0 {
			return fmt.Errorf(response.Info)
		}
		fmt.Printf("Deregistration of %d successful at %s", n.id, n.address)
		return nil
	}

	return fmt.Errorf("uncaught error here")
}

// Main program ------------------------------------------------------------------------------------

func main() {
	// Check if port was provided as an argument
	if len(os.Args) < 2 {
		fmt.Println("No port provided. Expected command: go run messenger.go <registry-host:registry-port>")
		return
	}
	target := os.Args[1]

	// Initialise the node
	n := &Node{}
	n.registryAddress = target

	// Setup active listener to registry -- awaiting commands and executions (Reference: minichord.pdf)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		fmt.Println("listen error:", err)
		return
	}

	// Split the port from the host before we save that information in the registry
	_, port, portErr := net.SplitHostPort(listener.Addr().String())
	if portErr != nil {
		fmt.Println("Failed to get a listener port:", portErr)
		return
	}
	// might need to modify this later
	// fullAddress := "127.0.0.1:" + port

	// Establish connection with registry
	conn, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Println("Dial error: ", err)
		return
	}
	defer conn.Close()

	lhost, _, lisErr := net.SplitHostPort(conn.LocalAddr().String())
	if lisErr != nil {
		fmt.Println("failed to get listener host from registry error:", lisErr)
		return
	}

	fullAddress := net.JoinHostPort(lhost, port)

	// Registeration - initiate contact with registry
	RegisterSelf(conn, fullAddress)

	// Registeration - await confirmation that it has been registered
	regResponse, regErr := minichord.ReceiveMiniChordMessage(conn)
	if regErr != nil {
		fmt.Println("Error receiving response:", regErr)
		return
	}

	// Check if connection works
	if response := regResponse.GetRegistrationResponse(); response != nil {
		// guard against failed connection
		if response.Result < 0 {
			fmt.Printf("Registration failed :%s\n", response.Info)
			return
		}
		n.id = response.Result
		n.address = fullAddress
		fmt.Printf("Registration of %d successful at %s", n.id, fullAddress)
	} else {
		fmt.Println("Did not receive reply registration")
		return
	}

	// listen for connections in separate goroutine -- so we can still look for user inputs while this runs
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("listener accept error:", err)
				continue
			}

			go func(c net.Conn) {
				defer c.Close()
				HandleIncomingMessage(c, n)
			}(conn)
		}
	}()

	// loop and wait for user inputs (Reference: minichord.pdf)
	reader := bufio.NewReader(os.Stdin)

	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				select {}
			}
			fmt.Println("Reader error:", err)
			break
		}

		cmd = strings.TrimSpace(cmd)
		switch cmd {
		case "print":
			Print(n)
		case "exit":
			if err := n.Exit(); err != nil {
				fmt.Println("exit error:", err)
			}
			return

		default:
			fmt.Printf("Command not understood: %s\n", cmd)
		}
	}
}
