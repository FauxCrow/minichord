package minichord

import (
	"fmt"
	"math/rand"
	"sync"
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

// Main program ------------------------------------------------------------------------------------

func main() {
	// Setup connection

	// Await for messanger nodes to communicate for registration or deregistration

	// Guard Clause: mismatch of given and actual address for registration AND deregistration

	// Convert output of AddMessanger or RemoveMessanger into the protobuf RegistrationResponse and DeregistrationResponse Format
}
