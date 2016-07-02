package golangNeo4jBoltDriver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
)

// recorder records a given session with Neo4j.
// allows for playback of sessions as well
type recorder struct {
	net.Conn
	name         string
	events       []*Event
	connStr      string
	currentEvent int
}

func newRecorder(name string, connStr string) *recorder {
	r := &recorder{
		name:    name,
		connStr: connStr,
	}

	if r.connStr == "" {
		if err := r.load(r.name); err != nil {
			log.Fatalf("Couldn't load data from recording files!: %s", err)
		}
	}

	return r
}

func (r *recorder) completedLast() bool {
	event := r.lastEvent()
	if event == nil {
		return true
	}

	return event.Completed
}

func (r *recorder) lastEvent() *Event {
	if len(r.events) > 0 {
		return r.events[len(r.events)-1]
	}
	return nil
}

// Read from the net conn, recording the interaction
func (r *recorder) Read(b []byte) (n int, err error) {
	if r.Conn != nil {
		numRead, err := r.Conn.Read(b)
		if numRead > 0 {
			r.record(b[:numRead], false)
		}

		if err != nil {
			r.recordErr(err, false)
		}

		return numRead, err
	}

	if r.currentEvent >= len(r.events) {
		return 0, errors.New("Trying to read past all of the events in the recorder! %#v", r)
	}
	event := r.events[r.currentEvent]
	if event.IsWrite {
		return 0, errors.New("Recorder expected Read, got Write! %#v, Event: %#v", r, event)
	}

	for i := 0; i < len(b); i++ {
		if len(event.Event) == 0 {
			return i, errors.New("Attempted to read past current event in recorder! Bytes: %s. Recorder %#v, Event; %#v", b, r, event)
		}
		b[i] = event.Event[0]
		event.Event = event.Event[1:]
	}

	if len(event.Event) == 0 {
		r.currentEvent++
	}

	return len(b), nil
}

// Close the net conn, outputting the recording
func (r *recorder) Close() error {
	if r.Conn != nil {
		err := r.flush()
		if err != nil {
			return err
		}
		return r.Conn.Close()
	} else if len(r.events) > 0 {
		if r.currentEvent != len(r.events) {
			return errors.New("Didn't read all of the events in the recorder on close! %#v", r)
		}

		if len(r.events[len(r.events)-1].Event) != 0 {
			return errors.New("Left data in an event in the recorder on close! %#v", r)
		}

		return nil
	}

	return nil
}

// Write to the net conn, recording the interaction
func (r *recorder) Write(b []byte) (n int, err error) {
	if r.Conn != nil {
		numWritten, err := r.Conn.Write(b)
		if numWritten > 0 {
			r.record(b[:numWritten], true)
		}

		if err != nil {
			r.recordErr(err, true)
		}

		return numWritten, err
	}

	if r.currentEvent >= len(r.events) {
		return 0, errors.New("Trying to write past all of the events in the recorder! %#v", r)
	}
	event := r.events[r.currentEvent]
	if !event.IsWrite {
		return 0, errors.New("Recorder expected Write, got Read! %#v, Event: %#v", r, event)
	}

	for i := 0; i < len(b); i++ {
		if len(event.Event) == 0 {
			return i, errors.New("Attempted to write past current event in recorder! %#v, Event: %#v", r, event)
		}
		event.Event = event.Event[1:]
	}

	if len(event.Event) == 0 {
		r.currentEvent++
	}

	return len(b), nil
}

func (r *recorder) record(data []byte, isWrite bool) {
	event := r.lastEvent()
	if event == nil || event.Completed || event.IsWrite != isWrite {
		event = newEvent(isWrite)
		r.events = append(r.events, event)
	}

	event.Event = append(event.Event, data...)
	if data[len(data)-2] == byte(0x00) && data[len(data)-1] == byte(0x00) {
		event.Completed = true
	}
}

func (r *recorder) recordErr(err error, isWrite bool) {
	event := r.lastEvent()
	if event == nil || event.Completed || event.IsWrite != isWrite {
		event = newEvent(isWrite)
		r.events = append(r.events, event)
	}

	event.Error = err
	event.Completed = true
}

func (r *recorder) load(name string) error {
	file, err := os.OpenFile("./recordings/"+name+".json", os.O_RDONLY, 0660)
	if err != nil {
		return err
	}

	return json.NewDecoder(file).Decode(&r.events)
}

func (r *recorder) writeRecording() error {
	file, err := os.OpenFile("./recordings/"+r.name+".json", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	return json.NewEncoder(file).Encode(r.events)
}

func (r *recorder) flush() error {
	if os.Getenv("RECORD_OUTPUT") == "" {
		return nil
	}
	return r.writeRecording()
}

func (r *recorder) print() {
	fmt.Println("PRINTING RECORDING " + r.name)

	for _, event := range r.events {

		fmt.Println()
		fmt.Println()

		typee := "READ"
		if event.IsWrite {
			typee = "WRITE"
		}
		fmt.Printf("%s @ %d:\n\n", typee, event.Timestamp)

		decoded, err := encoding.NewDecoder(bytes.NewBuffer(event.Event)).Decode()
		if err != nil {
			fmt.Printf("Error decoding data! Error: %s\n", err)
		} else {
			fmt.Printf("Decoded Data:\n\n%+v\n\n", decoded)
		}

		fmt.Print("Encoded Bytes:\n\n")
		fmt.Print(sprintByteHex(event.Event))
		if !event.Completed {
			fmt.Println("EVENT NEVER COMPLETED!!!!!!!!!!!!!!!")
		}

		if event.Error != nil {
			fmt.Printf("ERROR OCCURRED DURING EVENT!!!!!!!\n\nError: %s\n", event.Error)
		}

		fmt.Println()
		fmt.Println()
	}

	fmt.Println("RECORDING END " + r.name)
}

func (r *recorder) LocalAddr() net.Addr {
	if r.Conn != nil {
		return r.Conn.LocalAddr()
	}
	return nil
}

func (r *recorder) RemoteAddr() net.Addr {
	if r.Conn != nil {
		return r.Conn.RemoteAddr()
	}
	return nil
}

func (r *recorder) SetDeadline(t time.Time) error {
	if r.Conn != nil {
		return r.Conn.SetDeadline(t)
	}
	return nil
}

func (r *recorder) SetReadDeadline(t time.Time) error {
	if r.Conn != nil {
		return r.Conn.SetReadDeadline(t)
	}
	return nil
}

func (r *recorder) SetWriteDeadline(t time.Time) error {
	if r.Conn != nil {
		return r.Conn.SetWriteDeadline(t)
	}
	return nil
}

// Event represents a single recording (read or write) event in the recorder
type Event struct {
	Timestamp int64 `json:"-"`
	Event     []byte
	IsWrite   bool
	Completed bool
	Error     error
}

func newEvent(isWrite bool) *Event {
	return &Event{
		Timestamp: time.Now().UnixNano(),
		Event:     []byte{},
		IsWrite:   isWrite,
	}
}
