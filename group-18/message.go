package minichord

import (
	"google.golang.org/protobuf/proto"
	"encoding/binary"
	"io"
	"log"
	"net"
)

const I64SIZE = 8

// ReceiveMiniChordMessage receives a protobuf marshaled message on
// a connection conn and unmarshals it.
// Make sure to call this function from only one go routine at a time.
//
func ReceiveMiniChordMessage(conn net.Conn) (message *MiniChord, err error) {
	// First, get the number of bytes to received
	bs := make([]byte, I64SIZE)
	length, err := conn.Read(bs)
	if err != nil {
		if err != io.EOF {
			log.Printf("ReceiveMiniChordMessage() read error: %s\n", err)
		}
		return
	}
	if length != I64SIZE {
		log.Printf("ReceiveMiniChordMessage() length error: %d\n", length)
		return
	}
	numBytes := int(binary.BigEndian.Uint64(bs))

	// Get the marshaled message from the connection
	data := make([]byte, numBytes)
	length, err = conn.Read(data)
	if err != nil {
		if err != io.EOF {
			log.Printf("ReceivedMiniChordMessage() read error: %s\n", err)
		}
		return
	}
	if length != numBytes {
		log.Printf("ReceivedMiniChordMessage() length error: %d\n", length)
		return
	}

	// Unmarshal the message
	message = &MiniChord{}
	err = proto.Unmarshal(data[:length], message)
	if err != nil {
		log.Printf("ReceivedMiniChordMessage() cannot unmarshal %v, error: %s\n",
			data, err)
		return
	}
	log.Printf("ReceiveMiniChordMessage(): received %s (%v), %d from %s\n",
		message, data[:length], length, conn.RemoteAddr().String())
	return
}

// SendMiniChordMessage marshals and sends a protobuf message on
// a connection conn.
func SendMiniChordMessage(conn net.Conn, message *MiniChord) (err error) {
	data, err := proto.Marshal(message)
	log.Printf("SendMiniChordMessage(): sending %s (%v), %d to %s\n",
		message, data, len(data), conn.RemoteAddr().String())
	if err != nil {
		log.Panicln("Failed to marshal message.", err)
	}

	// First send the number of bytes in the marshaled message
	bs := make([]byte, I64SIZE)
	binary.BigEndian.PutUint64(bs, uint64(len(data)))

	// We concatenate the two slices to use only one write.
	msg := append(bs, data...)

	length, err := conn.Write(msg)
	if err != nil {
		log.Printf("SendMiniChordMessage(%v) error: %s\n", message, err)
	}
	if length != len(msg) {
		log.Panicln("Short write?")
	}
	return
}
