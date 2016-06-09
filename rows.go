package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"
)

// Rows represents results of rows from the DB
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []driver.Value) error
}

type boltRows struct {
	closed  bool
	columns []string
}

func newRows(columns []string) Rows {
	// TODO: Implement
	return nil
}

// Columns returns the columns from the result
func (r *boltRows) Columns() []string {
	return r.columns
}

// Close closes the rows
func (r *boltRows) Close() error {
	r.closed = true
	return nil
}

// Next gets the next row result
func (r *boltRows) Next(dest []driver.Value) error {
	if r.closed {
		return fmt.Errorf("Rows are already closed")
	}

	// TODO: Implement

	return nil
}
