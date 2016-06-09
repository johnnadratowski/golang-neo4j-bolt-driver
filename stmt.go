package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"
)

// Stmt represents a statement to run against the database
type Stmt interface {
	Close() error
	NumInput() int
	Exec(args []driver.Value) (driver.Result, error)
	Query(args []driver.Value) (driver.Rows, error)
}

type boltStmt struct {
	query  string
	conn   Conn
	closed bool
}

func newStmt(query string, conn Conn) Stmt {
	return &boltStmt{query: query, conn: conn}
}

// Close Closes the statement. See sql/driver.Stmt.
func (s *boltStmt) Close() error {
	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters. See sql/driver.Stmt.
func (s *boltStmt) NumInput() int {
	return -1 // TODO: Not sure if we should disable this
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
func (s *boltStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.closed {
		return nil, fmt.Errorf("Neo4j Bolt statement already closed")
	}

	// TODO: Implement

	return nil, nil
}

// Exec executes a query that returns data. See sql/driver.Stmt.
func (s *boltStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.closed {
		return nil, fmt.Errorf("Neo4j Bolt statement already closed")
	}

	// TODO: Implement

	return nil, nil
}
