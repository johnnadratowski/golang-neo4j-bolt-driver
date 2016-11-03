package bolt

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
)

// recorder records a given session with Neo4j.
// allows for playback of sessions as well
type recorder struct {
	conn   net.Conn
	name   string
	events []*Event
	cur    int
}

// OpenRecorder opens a *DB that records the session. Name will be the name of
// the gzipped JSON file containing the recorded session.
func OpenRecorder(name, dataSourceName string) (*DB, error) {
	db, err := openPool(&recorder{name: name}, dataSourceName)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Open opens a simulated Neo4j connection using pre-recorded data if name is
// an empty string. Otherwise, it opens up an actual connection using that to
// create a new recording.
func (r *recorder) Open(name string) (driver.Conn, error) {
	conn, err := r.open(name)
	if err != nil {
		return nil, err
	}
	return &sqlConn{conn}, nil
}

// Open opens a simulated Neo4j connection using pre-recorded data if name is
// an empty string. Otherwise, it opens up an actual connection using that to
// create a new recording.
func (r *recorder) OpenNeo(name string) (Conn, error) {
	conn, err := r.open(name)
	if err != nil {
		return nil, err
	}
	return &boltConn{conn}, nil
}

func (r *recorder) open(name string) (*conn, error) {
	if name == "" {
		err := r.load()
		if err != nil {
			return nil, err
		}
		return newConn(r, nil)
	}
	conn, v, err := open(&dialer{}, name)
	if err != nil {
		return nil, err
	}
	r.conn = conn
	return newConn(r, v)
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
	if r.conn != nil {
		n, err = r.conn.Read(p)
		r.record(p[:n], false)
		r.recordErr(err, false)
		return n, err
	}

	if r.cur >= len(r.events) {
		return 0, fmt.Errorf("trying to read past all of the events in the recorder %#v", r)
	}
	event := r.events[r.cur]
	if event.IsWrite {
		return 0, fmt.Errorf("recorder expected Read, got Write %#v, Event: %#v", r, event)
	}

	if len(p) > len(event.Event) {
		return 0, fmt.Errorf("attempted to read past current event in recorder Bytes: %s. Recorder %#v, Event; %#v", p, r, event)
	}

	n = copy(p, event.Event)
	event.Event = event.Event[n:]
	if len(event.Event) == 0 {
		r.cur++
	}
	return n, nil
}

// Close the net.Conn, outputting the recording.
func (r *recorder) Close() error {
	if r.conn != nil {
		err := r.flush()
		if err != nil {
			return err
		}
		return r.conn.Close()
	}
	if len(r.events) > 0 {
		if r.cur != len(r.events) {
			return fmt.Errorf("didn't read all of the events in the recorder on close %#v", r)
		}
		if len(r.events[len(r.events)-1].Event) != 0 {
			return fmt.Errorf("left data in an event in the recorder on close %#v", r)
		}
	}
	return nil
}

// Write to the net.Conn, recording the interaction.
func (r *recorder) Write(b []byte) (n int, err error) {
	if r.conn != nil {
		n, err = r.conn.Write(b)
		r.record(b[:n], true)
		r.recordErr(err, true)
		return n, err
	}

	if r.cur >= len(r.events) {
		return 0, fmt.Errorf("trying to write past all of the events in the recorder %#v", r)
	}
	event := r.events[r.cur]
	if !event.IsWrite {
		return 0, fmt.Errorf("recorder expected Write, got Read %#v, Event: %#v", r, event)
	}

	if len(b) > len(event.Event) {
		return 0, fmt.Errorf("attempted to write past current event in recorder Bytes: %s. Recorder %#v, Event; %#v", b, r, event)
	}

	event.Event = event.Event[len(b):]
	if len(event.Event) == 0 {
		r.cur++
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

func (r *recorder) load() error {
	path := filepath.Join("recordings", r.name+".json")
	file, err := os.OpenFile(path, os.O_RDONLY, 0660)
	if os.IsNotExist(err) {
		return nil
	}
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
	if r.conn != nil {
		return r.conn.LocalAddr()
	}
	return nil
}

func (r *recorder) RemoteAddr() net.Addr {
	if r.conn != nil {
		return r.conn.RemoteAddr()
	}
	return nil
}

func (r *recorder) SetDeadline(t time.Time) error {
	if r.conn != nil {
		return r.conn.SetDeadline(t)
	}
	return nil
}

func (r *recorder) SetReadDeadline(t time.Time) error {
	if r.conn != nil {
		return r.conn.SetReadDeadline(t)
	}
	return nil
}

func (r *recorder) SetWriteDeadline(t time.Time) error {
	if r.conn != nil {
		return r.conn.SetWriteDeadline(t)
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
