package golangNeo4jBoltDriver

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

type recorder struct {
	conn   net.Conn
	name   string
	events []*event
}

func (r *recorder) completedLast() bool {
	event := r.lastEvent()
	if event == nil {
		return true
	}

	return event.completed
}

func (r *recorder) lastEvent() *event {
	if len(r.events) > 0 {
		return r.events[len(r.events)-1]
	} else {
		return nil
	}
}

// Read from the net conn, recording the interaction
func (r *recorder) Read(b []byte) (n int, err error) {
	numRead, err := r.conn.Read(b)
	if numRead > 0 {
		r.record(b[:numRead], false)
	}

	if err != nil {
		r.recordErr(err, false)
	}

	return numRead, err
}

// Close the net conn, outputting the recording
func (r *recorder) Close() error {
	r.print()
	err := r.flush()
	if err != nil {
		return err
	}

	return r.conn.Close()
}

// Write to the net conn, recording the interaction
func (r *recorder) Write(b []byte) (n int, err error) {
	numWritten, err := r.conn.Write(b)
	if numWritten > 0 {
		r.record(b[:numWritten], true)
	}

	if err != nil {
		r.recordErr(err, true)
	}

	return numWritten, err
}

func (r *recorder) record(data []byte, isWrite bool) {
	event := r.lastEvent()
	if event == nil || event.completed || event.isWrite != isWrite {
		event = newEvent(isWrite)
	}

	event.event = append(event.event, data...)
	if data[len(data)-2] == byte(0x00) && data[len(data)-1] == byte(0x00) {
		event.completed = true
	}
}

func (r *recorder) recordErr(err error, isWrite bool) {
	event := r.lastEvent()
	if event == nil || event.completed || event.isWrite != isWrite {
		event = newEvent(isWrite)
	}

	event.error = err
	event.completed = true
}

func (r *recorder) load(name string) error {
	file, err := os.OpenFile("./recordings/"+name+".json", os.O_RDONLY, 0660)
	if err != nil {
		return err
	}

	return json.NewDecoder(file).Decode(r)
}

func (r *recorder) flush() error {
	file, err := os.OpenFile("./recordings/"+r.name+".json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}

	return json.NewEncoder(file).Encode(r)

}

func (r *recorder) print() {
	fmt.Println("PRINTING RECORDING " + r.name)

	for _, event := range r.events {

		fmt.Println()
		fmt.Println()

		typee := "READ"
		if event.isWrite {
			typee = "WRITE"
		}
		fmt.Printf("%s @ %d:\n\n", typee, event.timestamp)

		output := "\t"
		for i, b := range event.event {
			if i+1%16 == 0 {
				output += "\n\n"
			} else if i+1%4 == 0 {
				output += "\t"
			}
			output += fmt.Sprintf("%x ", b)
		}

		if !event.completed {
			fmt.Println("EVENT NEVER COMPLETED!!!!!!!!!!!!!!!")
		}

		if event.error != nil {
			fmt.Printf("ERROR OCCURRED DURING EVENT!!!!!!!\n\nError: %s\n", event.error)
		}

		fmt.Println()
		fmt.Println()
	}

	fmt.Println("RECORDING END " + r.name)
}

type event struct {
	timestamp int64
	event     []byte
	isWrite   bool
	completed bool
	error     error
}

func newEvent(isWrite bool) *event {
	return &event{
		timestamp: time.Now().UnixNano(),
		event:     []byte{},
		isWrite:   isWrite,
	}
}
