package bolt

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
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

	conv genericConv
}

// genericConv implements driver.ValueConverter to allow the sql.DB interface
// to work with bolt.
type genericConv struct {
	b     *bytes.Buffer
	e     *encoding.Encoder
	ismap bool
	idx   int
}

// encode returns an encoded v and any errors that may have occurred.
func (g *genericConv) encode(v interface{}) ([]byte, error) {
	if g.b == nil {
		g.b = new(bytes.Buffer)
	}
	if g.e == nil {
		g.e = encoding.NewEncoder(g.b)
	}
	err := g.e.Encode(v)
	if err != nil {
		return nil, err
	}
	m := make([]byte, g.b.Len())
	copy(m, g.b.Bytes())
	g.b.Reset()
	return m, nil
}

// ConvertValue implements driver.ValueConverter.
func (g *genericConv) ConvertValue(v interface{}) (driver.Value, error) {
	if g.idx == 0 {
		m, ok := v.(map[string]interface{})
		if ok {
			g.ismap = true
			return g.encode(m)
		}
	}
	// If our first value was a map then we've finished and any new values are
	// an error.
	if g.ismap {
		return nil, errors.New("if value #0 is map[string]interface{} no other values are allowed")
	}
	// Even entries should be strings (keys).
	if g.idx%2 == 0 {
		key, ok := v.(string)
		if !ok {
			return nil, errors.New("even values must be string keys")
		}
		return key, nil
	}
	// Odd entries can be anything. The sql package handles the driver.Valuer
	// case for us. If v is a valid driver.Value return it. Otherwise, use
	// bolt's encoding and return it as a []byte.
	//
	// TODO: is there something more efficient?
	if driver.IsValue(v) {
		return v, nil
	}
	return g.encode(v)
}

// ColumnConverter implements driver.ColumnConverter.
func (s *sqlStmt) ColumnConverter(idx int) driver.ValueConverter {
	s.conv.idx = idx
	return &s.conv
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

// ExecNeo executes a query that returns no rows.
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
		return nil, fmt.Errorf("Unrecognized response when discarding exec rows: %#v", pull)
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

// Query executes a query that returns data.
func (s *boltStmt) Query(params map[string]interface{}) (rows, error) {
	err := s.exec(params)
	if err != nil {
		return nil, err
	}
	return &boltRows{conn: s.conn, md: s.md}, nil
}
