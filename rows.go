package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Rows represents results of rows from the DB
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Row objects ARE NOT THREAD SAFE.
// If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []driver.Value) error
}

type boltRows struct {
	closed    bool
	metadata  map[string]interface{}
	statement *boltStmt
	consumed  bool
}

func newRows(metadata map[string]interface{}) *boltRows {
	return &boltRows{
		metadata: metadata,
	}
}

// Columns returns the columns from the result
func (r *boltRows) Columns() []string {
	var fields []string
	if fieldsInt, ok := r.metadata["fields"]; !ok {
		return []string{}
	} else {
		if fields, ok = fieldsInt.([]string); !ok {
			Logger.Printf("Unrecognized fields from success message: %T %#v", fieldsInt, fieldsInt)
			return []string{}
		} else {
			return fields
		}
	}
}

// Close closes the rows
func (r *boltRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.statement.rows = nil
	return nil
}

// Next gets the next row result
func (r *boltRows) Next(dest []driver.Value) error {
	if r.closed {
		return fmt.Errorf("Rows are already closed")
	}

	pullMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(r.statement.conn, r.statement.conn.chunkSize).Encode(pullMessage)
	if err != nil {
		Logger.Printf("An error occurred encoding pull all query: %s", err)
		return fmt.Errorf("An error occurred encoding pull all query: %s", err)
	}

	respInt, err := encoding.NewDecoder(r.statement.conn).Decode()
	if err != nil {
		Logger.Printf("An error occurred decoding pull all query response: %s", err)
		return fmt.Errorf("An error occurred decoding pull all query response: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Got success message: %#v", resp)
		r.consumed = true
		return io.EOF
	case messages.FailureMessage:
		Logger.Printf("Got failure message: %#v", resp)
		err := r.statement.conn.ackFailure(resp)
		if err != nil {
			Logger.Printf("An error occurred acking failure: %s", err)
		}
		return fmt.Errorf("Got failure message: %#v", resp)
	case messages.RecordMessage:
		Logger.Printf("Got record message: %#v", resp)
		dest = make([]driver.Value, len(resp.Fields))
		for i, item := range resp.Fields {
			dest[i] = item
		}
		// TODO: Implement conversion to driver.Value
		return nil
	default:
		return fmt.Errorf("Unrecognized response type: %T Value: %#v", resp, resp)
	}
}

// NextNeo gets the next row result
func (r *boltRows) NextNeo(dest interface{}) (map[string]interface{}, error) {
	if r.closed {
		return nil, fmt.Errorf("Rows are already closed")
	}

	pullMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(r.statement.conn, r.statement.conn.chunkSize).Encode(pullMessage)
	if err != nil {
		Logger.Printf("An error occurred encoding pull all query: %s", err)
		return nil, fmt.Errorf("An error occurred encoding pull all query: %s", err)
	}

	respInt, err := encoding.NewDecoder(r.statement.conn).Decode()
	if err != nil {
		Logger.Printf("An error occurred decoding pull all query response: %s", err)
		return nil, fmt.Errorf("An error occurred decoding pull all query response: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Got success message: %#v", resp)
		r.consumed = true
		return resp.Metadata, io.EOF
	case messages.FailureMessage:
		Logger.Printf("Got failure message: %#v", resp)
		err := r.statement.conn.ackFailure(resp)
		if err != nil {
			Logger.Printf("An error occurred acking failure: %s", err)
		}
		return nil, fmt.Errorf("Got failure message: %#v", resp)
	case messages.RecordMessage:
		Logger.Printf("Got record message: %#v", resp)
		dest = resp.Fields
		// TODO: Implement conversion to driver.Value
		return nil, nil
	default:
		return nil, fmt.Errorf("Unrecognized response type: %T Value: %#v", resp, resp)
	}
}
