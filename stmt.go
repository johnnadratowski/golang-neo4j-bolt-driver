package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Stmt represents a statement to run against the database
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Stmt objects, and any rows prepared within ARE NOT
// THREAD SAFE.  If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Stmt interface {
	Close() error
	NumInput() int
	Exec(args []driver.Value) (driver.Result, error)
	Query(args []driver.Value) (driver.Rows, error)
	QueryNeo(params map[string]interface{}) (Rows, error)
}

type boltStmt struct {
	query  string
	conn   *boltConn
	closed bool
	rows   *boltRows
}

func newStmt(query string, conn *boltConn) *boltStmt {
	return &boltStmt{query: query, conn: conn}
}

// Close Closes the statement. See sql/driver.Stmt.
func (s *boltStmt) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.conn.statement = nil
	s.conn = nil
	return nil
}

// NumInput returns the number of placeholder parameters. See sql/driver.Stmt.
// Currently will always return -1
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

// Exec executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) ExecNeo(args []driver.Value) (Result, error) {
	if s.closed {
		return nil, fmt.Errorf("Neo4j Bolt statement already closed")
	}

	// TODO: Implement

	return nil, nil
}

// Query executes a query that returns data. See sql/driver.Stmt.
func (s *boltStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.closed {
		return nil, fmt.Errorf("Neo4j Bolt statement already closed")
	}

	params := map[string]interface{}{}
	for _, param := range args {
		paramMap, ok := param.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Expected map[string]interface{} for query args. Got %T. Arg: %#v", param, param)
		}
		for k, v := range paramMap {
			params[k] = v
		}
	}

	return s.QueryNeo(params)
}

// QueryNeo executes a query that returns data. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) QueryNeo(params map[string]interface{}) (Rows, error) {
	if s.rows != nil {
		return nil, fmt.Errorf("Another query is already open")
	}

	runMessage := messages.NewRunMessage(s.query, params)
	err := encoding.NewEncoder(s.conn, s.conn.chunkSize).Encode(runMessage)
	if err != nil {
		Logger.Printf("An error occurred encoding run query: %s", err)
		return nil, fmt.Errorf("An error occurred encoding run query: %s", err)
	}

	respInt, err := encoding.NewDecoder(s.conn).Decode()
	if err != nil {
		Logger.Printf("An error occurred decoding run query response: %s", err)
		return nil, fmt.Errorf("An error occurred decoding run query response: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Got success message: %#v", resp)
		s.rows = newRows(s, resp.Metadata)
		return s.rows, nil
	case messages.FailureMessage:
		Logger.Printf("Got failure message: %#v", resp)
		err := s.conn.ackFailure(resp)
		if err != nil {
			Logger.Printf("An error occurred acking failure: %s", err)
		}
		return nil, fmt.Errorf("Got failure message: %#v", resp)
	default:
		return nil, fmt.Errorf("Unrecognized response type: %T Value: %#v", resp, resp)
	}
}
