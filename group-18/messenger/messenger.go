package main

import (
	"fmt"
	"net"
	"os"

	minichord "github.com/mkyas/minichord/packages"
)

// Node information storage
type Node struct {
	id      int32  // stores assigned id from registry
	address string // stores address used in registry
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
		fmt.Println("Failure registering node from: " + n.address)
	}
}

// TODO: Helper: print information to the console about the number of messages sent, received, and relayed, along with the sums for the messages sent from and received at the node.
func Print() {

}

// TODO: Helper: allows a messaging node to exit the overlay. The messaging node should first send a deregistration message (see Section 2.2) to the registry and await a response before exiting and terminating the process.
func Exit() {

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

	// Establish connection with registry
	conn, err := net.Dial("tcp", target)
	if err != nil {
		fmt.Println("Dial error: ", err)
		return
	}
	defer conn.Close()

	// Registeration - initiate contact with registry
	RegisterSelf(conn, conn.LocalAddr().String())

	// Registeration - await confirmation that it has been registered
	regResponse, regErr := minichord.ReceiveMiniChordMessage(conn)
	if regErr != nil {
		fmt.Println("Error receiving response:", regErr)
		return
	}

	// Check if connection works
	if response := regResponse.GetRegistrationResponse(); response != nil {
		n.id = response.Result
		n.address = conn.LocalAddr().String()

		fmt.Printf("Registration of %d successful at %s", n.id, n.address)
	}

	// TEMP PLACEMENT: Deregistration - end contact with registry
	DeregisterSelf(conn, n)

	// Registeration - await confirmation that it has been deregistered
	deregResponse, deregErr := minichord.ReceiveMiniChordMessage(conn)
	if deregErr != nil {
		fmt.Println("Error receiving response:", deregErr)
		return
	}

	// Check if connection works
	if response := deregResponse.GetRegistrationResponse(); response != nil {
		fmt.Printf("Deregistration of %d successful at %s", n.id, n.address)
	}

	// TODO: Sending messages to other nodes

	// TEST CODE USING HELLO WORLD WILL REMOVE LATER
	// conn.Write([]byte("Hello World"))
	// recvBuffer := make([]byte, 1024)
	// n, _ := conn.Read(recvBuffer[:])
	// fmt.Println("Received from Server:", string(recvBuffer[:n]))
}
