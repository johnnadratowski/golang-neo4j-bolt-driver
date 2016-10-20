package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
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

// NewRecorder initializes a Driver that records the session. Name will be the
// name of the gzipped JSON file containing the recorded session.
func NewRecorder(name string) Driver {
	return &recorder{name: name}
}

// Open opens a new Bolt connection to the Neo4J database
func (r *recorder) Open(connStr string) (driver.Conn, error) {
	if connStr == "" {
		err := r.load(r.name)
		if err != nil {
			return nil, errors.New("couldn't load data from recording file")
		}
	}
	return newBoltConn(connStr, r)
}

// Open opens a new Bolt connection to the Neo4J database. Implements a Neo-friendly alternative to sql/driver.
func (r *recorder) OpenNeo(connStr string) (Conn, error) {
	if connStr == "" {
		err := r.load(r.name)
		if err != nil {
			return nil, errors.New("couldn't load data from recording file")
		}
	}
	return newBoltConn(connStr, r)
}

func newRecorder(name string, connStr string) *recorder {
	r := &recorder{name: name, connStr: connStr}
	return r
}

func (r *recorder) completedLast() bool {
	event := r.lastEvent()
	return event == nil || event.Completed
}

func (r *recorder) lastEvent() *Event {
	if len(r.events) > 0 {
		return r.events[len(r.events)-1]
	}
	return nil
}

// Read reads from the net.Conn, recording the interaction.
func (r *recorder) Read(p []byte) (n int, err error) {
	if r.Conn != nil {
		n, err = r.Conn.Read(p)
		r.record(p[:n], false)
		r.recordErr(err, false)
		return n, err
	}

	if r.currentEvent >= len(r.events) {
		return 0, errors.New("Trying to read past all of the events in the recorder! %#v", r)
	}
	event := r.events[r.currentEvent]
	if event.IsWrite {
		return 0, errors.New("Recorder expected Read, got Write! %#v, Event: %#v", r, event)
	}

	if len(p) > len(event.Event) {
		return 0, errors.New("Attempted to read past current event in recorder! Bytes: %s. Recorder %#v, Event; %#v", p, r, event)
	}

	n = copy(p, event.Event)
	event.Event = event.Event[n:]
	if len(event.Event) == 0 {
		r.currentEvent++
	}
	return n, nil
}

// Close the net.Conn, outputting the recording.
func (r *recorder) Close() error {
	if r.Conn != nil {
		err := r.flush()
		if err != nil {
			return err
		}
		return r.Conn.Close()
	}
	if len(r.events) > 0 {
		if r.currentEvent != len(r.events) {
			return errors.New("Didn't read all of the events in the recorder on close! %#v", r)
		}
		if len(r.events[len(r.events)-1].Event) != 0 {
			return errors.New("Left data in an event in the recorder on close! %#v", r)
		}
	}
	return nil
}

// Write to the net.Conn, recording the interaction.
func (r *recorder) Write(b []byte) (n int, err error) {
	if r.Conn != nil {
		n, err = r.Conn.Write(b)
		r.record(b[:n], true)
		r.recordErr(err, true)
		return n, err
	}

	if r.currentEvent >= len(r.events) {
		return 0, errors.New("Trying to write past all of the events in the recorder! %#v", r)
	}
	event := r.events[r.currentEvent]
	if !event.IsWrite {
		return 0, errors.New("Recorder expected Write, got Read! %#v, Event: %#v", r, event)
	}

	if len(b) > len(event.Event) {
		return 0, errors.New("Attempted to write past current event in recorder! Bytes: %s. Recorder %#v, Event; %#v", b, r, event)
	}

	event.Event = event.Event[len(b):]
	if len(event.Event) == 0 {
		r.currentEvent++
	}
	return len(b), nil
}

func (r *recorder) record(data []byte, isWrite bool) {
	if len(data) == 0 {
		return
	}

	event := r.lastEvent()
	if event == nil || event.Completed || event.IsWrite != isWrite {
		event = newEvent(isWrite)
		r.events = append(r.events, event)
	}

	event.Event = append(event.Event, data...)
	event.Completed = bytes.HasSuffix(data, encoding.EndMessage)
}

func (r *recorder) recordErr(err error, isWrite bool) {
	if err == nil {
		return
	}

	event := r.lastEvent()
	if event == nil || event.Completed || event.IsWrite != isWrite {
		event = newEvent(isWrite)
		r.events = append(r.events, event)
	}
	event.Error = err
	event.Completed = true
}

func (r *recorder) load(name string) error {
	path := filepath.Join("recordings", name+".json")
	file, err := os.OpenFile(path, os.O_RDONLY, 0660)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(&r.events)
}

func (r *recorder) writeRecording() error {
	path := filepath.Join("recordings", r.name+".json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0660)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(r.events)
}

func (r *recorder) flush() error {
	if os.Getenv("RECORD_OUTPUT") != "" {
		return r.writeRecording()
	}
	return nil
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
		IsWrite:   isWrite,
	}
}
