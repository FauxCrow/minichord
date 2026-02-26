package main

import (
	"bufio"
	"fmt"
	"io"
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
			nodeRegistryReq := &minichord.NodeRegistryResponse{
				Result: int32(success),
				Info:   "Recieved",
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
		}
	}
}

// TODO: print information to the console about the number of messages sent, received, and relayed, along with the sums for the messages sent from and received at the node.
func Print() {

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
	listener, _ := net.Listen("tcp", ":0")

	// Split the port from the host before we save that information in the registry
	_, port, _ := net.SplitHostPort(listener.Addr().String())
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
			conn, _ := listener.Accept()

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
		case "exit":
			n.Exit()
		default:
			fmt.Printf("Command not understood: %s", cmd)
		}
	}
}
