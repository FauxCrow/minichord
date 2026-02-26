package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"

	minichord "github.com/mkyas/minichord/packages"
)

// Node information storage
type Node struct {
	id              int32                       // stores assigned id from registry
	address         string                      // stores address used by registry
	registryAddress string                      // stores the registry's adress
	fingerTable     []*minichord.Deregistration // list of IDs
	allIDs          []int32

	sendTracker    uint32
	receiveTracker uint32
	relayTracker   uint32
	sendSummation  int64
	recvSummation  int64
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
	deregisterReq := &minichord.Deregistration{
		Id:      n.id,
		Address: n.address,
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
		n.sendTracker++
		n.sendSummation += int64(payload)
	}

	n.sendTaskFinished()
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
			n.fingerTable = nodeRegistry.Peers
			n.allIDs = nodeRegistry.Ids

			// Initiate connections to the nodes that comprise its finger table, and track how many succeed
			success := 0
			for _, peer := range n.fingerTable {
				pConn, err := net.Dial("tcp", peer.Address)
				if err != nil {
					fmt.Printf("Failed to connect to peer %d at %s: %v\n", peer.Id, peer.Address, err)
					continue
				}
				pConn.Close()
				success++
			}

			// Initialise a node registry response
			result := int32(0)
			info := "Received"

			if success != len(n.fingerTable) {
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
		} else if message.GetReportTrafficSummary() != nil {
			n.sendTrafficSummary()
		} else if task := message.GetInitiateTask(); task != nil {
			fmt.Printf("Received InitiateTask: %d packets \n", task.Packets)
			go n.runTest(int(task.Packets))
		}
	}
}

// TODO: print information to the console about the number of messages sent, received, and relayed, along with the sums for the messages sent from and received at the node.
func Print(n *Node) {
	fmt.Printf("Node %d\n", n.id)
	fmt.Printf("Sent: %d\n", n.sendTracker)
	fmt.Printf("Received: %d\n", n.receiveTracker)
	fmt.Printf("Relayed: %d\n", n.relayTracker)
	fmt.Printf("Sum Sent: %d\n", n.sendSummation)
	fmt.Printf("Sum Received: %d\n", n.recvSummation)
}

// helpers after node finishes sending assigned packets, to send data to registry
func (n *Node) sendTaskFinished() error {
	conn, err := net.Dial("tcp", n.registryAddress)
	if err != nil {
		return err
	}

	defer conn.Close()

	tf := &minichord.TaskFinished{
		Id:      n.id,
		Address: n.address,
	}

	msg := &minichord.MiniChord{
		Message: &minichord.MiniChord_TaskFinished{
			TaskFinished: tf,
		},
	}

	return minichord.SendMiniChordMessage(conn, msg)
}

func (n *Node) sendTrafficSummary() error {
	conn, err := net.Dial("tcp", n.registryAddress)
	if err != nil {
		return err
	}

	defer conn.Close()

	ts := &minichord.TrafficSummary{
		Id:            n.id,
		Sent:          n.sendTracker,
		Received:      n.receiveTracker,
		Relayed:       n.relayTracker,
		TotalSent:     n.sendSummation,
		TotalReceived: n.recvSummation,
	}

	msg := &minichord.MiniChord{
		Message: &minichord.MiniChord_ReportTrafficSummary{
			ReportTrafficSummary: ts,
		},
	}

	err = minichord.SendMiniChordMessage(conn, msg)
	if err != nil {
		return err
	}

	// reset counters after sending
	n.sendTracker = 0
	n.receiveTracker = 0
	n.relayTracker = 0
	n.sendSummation = 0
	n.recvSummation = 0

	return nil
}

// TODO: allows a messaging node to exit the overlay. The messaging node should first send a deregistration message (see Section 2.2) to the registry and await a response before exiting and terminating the process.
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
		fmt.Printf("Deregistration of %d successful at %s", n.id, n.address)
	}

	return nil
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
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	// might need to modify this later
	fullAddress := "127.0.0.1:" + port

	// Establish connection with registry
	conn, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Println("Dial error: ", err)
		return
	}
	defer conn.Close()

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
		n.id = response.Result
		n.address = fullAddress
		fmt.Printf("Registration of %d successful at %s", n.id, fullAddress)
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
			n.Exit()
		default:
			fmt.Printf("Command not understood: %s", cmd)
		}
	}
}
