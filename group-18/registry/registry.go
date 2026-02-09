package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"sync"

	minichord "github.com/mkyas/minichord/packages"
)

// Registry information storage
type Registry struct {
	messangers map[int32]string // key: random identifier (between 0–1023), value: given address
	mutex      sync.RWMutex     // mutex: ensures safe access to Registry (Reference: https://gobyexample.com/mutexes)
}

// Registration: Adds new messanger to Registry
func (r *Registry) AddMessanger(givenAddress string, actualAddress string) (int32, string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Guard Clause: duplicate address registration
	// Note: O(n) implementation, can try to see if can reduce this?
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

	// Return RegistrationResponse
	return newID, "Registration successful.", nil
}

// Deregistration: Remove messanger from Registry
func (r *Registry) RemoveMessanger(key int32, givenAddress string) (string, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Guard Clause: Check if node exists in registry, and if given address matches
	if _, exists := r.messangers[key]; !exists {
		if r.messangers[key] != givenAddress {
			return "Address does not match key.", fmt.Errorf("Address Mismatch")
		}
	} else {
		return "Key does not exist in Registry.", fmt.Errorf("Invalid Key")
	}

	// Remove messanger key
	delete(r.messangers, key)

	return "Deregistration successful.", nil
}

// Essentially manages one messenger node over its life cycle, to look for communication attempts
func handleIncomingMessage(conn net.Conn, r *Registry) {
	defer conn.Close()

	// Check for all message types -- case switch
	for {
		message, err := minichord.ReceiveMiniChordMessage(conn)
		if err != nil {
			fmt.Println("Messenger disconnected")
			return // need to kill the goroutine...
		}

		// Case 1: Registration
		if registerReq := message.GetRegistration(); registerReq != nil {
			actualAddress := conn.RemoteAddr().String()

			// Guard Clause: mismatch of given and actual address
			if actualAddress != registerReq.Address {
				fmt.Println("Address Mismatch")
			}

			id, info, err := r.AddMessanger(registerReq.Address, actualAddress)
			if err != nil {
				// TODO
			}

			// Initialise a registration response request
			registration := &minichord.RegistrationResponse{
				Result: id,
				Info:   info,
			}

			// Create Minichord, where message is assignable to MiniChord_Registration type
			registrationResponse := &minichord.MiniChord{
				Message: &minichord.MiniChord_RegistrationResponse{
					RegistrationResponse: registration,
				},
			}

			// Send minichord to messager using SendMiniChordMessage
			// not sure why doing err := here makes the code start yelling at me icl
			minichord.SendMiniChordMessage(conn, registrationResponse)
		}
	}
}

// TODO: Helper: List all currently registered messaging nodes’ hostname, port number, and node ID.
func List() {

}

// TODO: Helper: Setup the overlay with 𝑁𝑅 entries in the finger table.
func SetupNR() {

}

// TODO: Helper: List the computed finger tables for each node in the overlay.
func Route() {

}

// TODO: Helper: The start command makes the registry send the message TaskInitiate message to all nodes registered in the overlay. A command of start 50 results in each messaging node sending 50 packets to randomly chosen nodes.
func Start() {

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
		messangers: make(map[int32]string),
	}

	// Setup connection (Reference: minichord.pdf)
	listener, _ := net.Listen("tcp", ":"+port)

	for {
		conn, _ := listener.Accept()

		// Rationale: creating a goroutine allows us to continue listening for other messenger nodes while this one is connected
		go handleIncomingMessage(conn, r)
	}
}

// TEST CODE USING HELLO WORLD WILL REMOVE LATER
// func testConnection(conn net.Conn) {
// 	defer conn.Close()

// 	for {
// 		recvBuffer := make([]byte, 1024)
// 		n, err := conn.Read(recvBuffer)
// 		fmt.Println("Messenger sent:", string(recvBuffer[:n]))
// 		if err != nil {
// 			return
// 		}
// 		conn.Write(recvBuffer[:n])
// 	}
// }
