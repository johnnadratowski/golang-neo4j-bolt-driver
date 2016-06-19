package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"io"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
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
	// Columns Gets the names of the columns in the returned dataset
	Columns() []string
	// Metadata Gets all of the metadata returned from Neo on query start
	Metadata() map[string]interface{}
	// Close the rows, flushing any existing datastream
	Close() error
	// Next gets the next row result
	Next([]driver.Value) error
	// NextNeo gets the next row result
	// When the rows are completed, returns the success metadata
	// and io.EOF
	NextNeo() ([]interface{}, map[string]interface{}, error)
}

type boltRows struct {
	metadata        map[string]interface{}
	statement       *boltStmt
	closed          bool
	consumed        bool
	finishedConsume bool
}

func newRows(statement *boltStmt, metadata map[string]interface{}) *boltRows {
	return &boltRows{
		statement: statement,
		metadata:  metadata,
	}
}

// Columns returns the columns from the result
func (r *boltRows) Columns() []string {
	fieldsInt, ok := r.metadata["fields"]
	if !ok {
		return []string{}
	}

	fields, ok := fieldsInt.([]string)
	if !ok {
		log.Errorf("Unrecognized fields from success message: %T %#v", fieldsInt, fieldsInt)
		return []string{}
	}

	return fields
}

// Metadata Gets all of the metadata returned from Neo on query start
func (r *boltRows) Metadata() map[string]interface{} {
	return r.metadata
}

// Close closes the rows
func (r *boltRows) Close() error {
	if r.closed {
		return nil
	}

	if !r.consumed {
		// Discard all messages if not consumed

		respInt, err := r.statement.conn.sendDiscardAll()
		if err != nil {
			return errors.Wrap(err, "An error occurred discarding messages on row close")
		}

		switch resp := respInt.(type) {
		case messages.SuccessMessage:
			log.Infof("Got success message: %#v", resp)
		default:
			return errors.New("Unrecognized response type: %T Value: %#v", resp, resp)
		}

	} else if !r.finishedConsume {
		// Clear out all unconsumed messages if we
		// never finished consuming them.
		_, _, err := r.statement.conn.consumeAll()
		if err != nil {
			return errors.Wrap(err, "An error occurred clearing out unconsumed stream")
		}
	}

	r.closed = true
	r.statement.rows = nil
	return nil
}

// Next gets the next row result
func (r *boltRows) Next(dest []driver.Value) error {
	if r.closed {
		return errors.New("Rows are already closed")
	}

	if !r.consumed {
		r.consumed = true
		if err := r.statement.conn.sendPullAll(); err != nil {
			r.finishedConsume = true
			return err
		}
	}

	respInt, err := r.statement.conn.consume()
	if err != nil {
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Got success message: %#v", resp)
		r.finishedConsume = true
		return io.EOF
	case messages.RecordMessage:
		log.Infof("Got record message: %#v", resp)
		dest = make([]driver.Value, len(resp.Fields))
		for i, item := range resp.Fields {
			dest[i] = item
		}
		// TODO: Implement conversion to driver.Value
		return nil
	default:
		return errors.New("Unrecognized response type: %T Value: %#v", resp, resp)
	}
}

// NextNeo gets the next row result
// When the rows are completed, returns the success metadata
// and io.EOF
func (r *boltRows) NextNeo() ([]interface{}, map[string]interface{}, error) {
	if r.closed {
		return nil, nil, errors.New("Rows are already closed")
	}

	if !r.consumed {
		r.consumed = true
		if err := r.statement.conn.sendPullAll(); err != nil {
			r.finishedConsume = true
			return nil, nil, err
		}
	}

	respInt, err := r.statement.conn.consume()
	if err != nil {
		return nil, nil, err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Got success message: %#v", resp)
		r.finishedConsume = true
		return nil, resp.Metadata, io.EOF
	case messages.RecordMessage:
		log.Infof("Got record message: %#v", resp)
		return resp.Fields, nil, nil
	default:
		return nil, nil, errors.New("Unrecognized response type: %T Value: %#v", resp, resp)
	}
}
