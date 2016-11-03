package bolt

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/graph"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

// Rows represents results of rows from the DB.
type rows interface {
	// Metadata returns the metadata for the current row.
	Metadata() map[string]interface{}

	driver.Rows
}

type boltRows struct {
	conn     *conn
	cols     []string
	md       map[string]interface{}
	closed   bool // true if Close successfully called.
	finished bool // true if all rows have been read.
}

// Columns returns the columns from the result
func (r *boltRows) Columns() []string {
	if r.cols != nil {
		return r.cols
	}

	val, ok := r.md["fields"]
	if !ok {
		return nil
	}

	fifc, ok := val.([]interface{})
	if !ok {
		return nil
	}

	cols := make([]string, len(fifc))
	for i, col := range fifc {
		cols[i], ok = col.(string)
		if !ok {
			return nil
		}
	}
	r.cols = cols
	return cols
}

// Metadata returns the metadata for the current row.
func (r *boltRows) Metadata() map[string]interface{} {
	return r.md
}

// Close closes the rows
func (r *boltRows) Close() error {
	if r.closed {
		return nil
	}
	// We haven't read all the rows.
	if !r.finished {
		err := r.conn.dec.Discard()
		if err != nil {
			return err
		}
		r.finished = true
	}
	r.closed = true
	return nil
}

// Next gets the next row result
func (r *boltRows) Next(dest []driver.Value) error {
	data, err := r.next()
	if err != nil {
		return err
	}

	for i, item := range data {
		switch item := item.(type) {
		case driver.Value:
			dest[i] = item
		case []interface{}, map[string]interface{},
			graph.Node, graph.Path,
			graph.Relationship, graph.UnboundRelationship:
			dest[i], err = encoding.Marshal(item)
			if err != nil {
				return err
			}
		default:
			dest[i], err = driver.DefaultParameterConverter.ConvertValue(item)
			if err != nil {
				return err
			}
		}
	}
	return err
}

// NextNeo gets the next row result
// When the rows are completed, returns the success metadata
// and io.EOF
func (r *boltRows) next() ([]interface{}, error) {
	if r.closed {
		return nil, errors.New("rows are already closed")
	}

	resp, err := r.conn.consume()
	if err != nil {
		return nil, err
	}

	switch t := resp.(type) {
	case messages.SuccessMessage:
		r.md = t.Metadata
		r.finished = true
		return nil, io.EOF
	case messages.RecordMessage:
		r.md = nil
		return t.Fields, nil
	default:
		return nil, fmt.Errorf("Unrecognized response type getting next query row: %#v", resp)
	}
}
