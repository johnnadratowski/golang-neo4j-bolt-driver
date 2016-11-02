package bolt

import (
	"database/sql/driver"
	"fmt"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

type boltStmt struct {
	conn   *conn
	query  string
	md     map[string]interface{}
	closed bool
}

// Close Closes the statement.
func (s *boltStmt) Close() error {
	if s.closed {
		return nil
	}
	if s.conn.bad {
		return driver.ErrBadConn
	}
	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters. See sql/driver.Stmt.
// Currently will always return -1
func (s *boltStmt) NumInput() int {
	return -1 // TODO: would need a cypher parser for this. disable for now
}

func (s *boltStmt) exec(args map[string]interface{}) error {
	resp, err := s.conn.sendRunPullAllConsumeRun(s.query, args)
	if err != nil {
		s.closed = true
		return err
	}

	success, ok := resp.(messages.SuccessMessage)
	if !ok {
		s.closed = true
		return fmt.Errorf("unexpected response querying neo from connection: %#v", resp)
	}

	s.md = success.Metadata
	return nil
}

type sqlStmt struct {
	*boltStmt
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
// You must bolt encode a map to pass as []bytes for the driver value
func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return s.boltStmt.Exec(params)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) Exec(params map[string]interface{}) (Result, error) {
	if s.closed {
		return nil, errors.New("Neo4j Bolt statement already closed")
	}
	err := s.exec(params)
	if err != nil {
		return nil, err
	}
	_, pull, err := s.conn.consumeAll()
	if err != nil {
		return nil, err
	}
	success, ok := pull.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response when discarding exec rows: %#v", pull)
	}
	return boltResult{metadata: success.Metadata}, nil
}

// Query executes a query that returns data. See sql/driver.Stmt.
// You must bolt encode a map to pass as []bytes for the driver value
func (s *sqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	err = s.exec(params)
	if err != nil {
		return nil, err
	}
	return &boltRows{conn: s.conn, md: s.md}, nil
}

// Query executes a query that returns data. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) Query(params map[string]interface{}) (rows, error) {
	err := s.exec(params)
	if err != nil {
		return nil, err
	}
	return &boltRows{conn: s.conn, md: s.md}, nil
}
