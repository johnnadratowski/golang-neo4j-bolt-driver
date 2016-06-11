package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"net"
	"time"
)

// Conn represents a connection to Neo4J
type Conn interface {
	Prepare(query string) (driver.Stmt, error)
	Close() error
	Begin() (driver.Tx, error)
}

type boltConn struct {
	connStr       string
	conn          net.Conn
	serverVersion []byte
	initialized   bool
	timeout       time.Duration
	chunkSize     uint16
}

// newBoltConn Creates a new bolt connection
func newBoltConn(connStr string, conn net.Conn, serverVersion []byte, timeoutSeconds int, chunkSize uint16) *boltConn {
	return &boltConn{
		connStr:       connStr,
		conn:          conn,
		serverVersion: serverVersion,
		timeout:       time.Second * time.Duration(timeoutSeconds),
		chunkSize:     chunkSize,
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.conn.Read(b)
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.conn.Write(b)
}

// Prepare prepares a new statement for a query
func (c *boltConn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(query, c), nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	// TODO: Implement

	return nil, nil
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *boltConn) Close() error {
	// TODO: Connection Pooling?
	err := c.conn.Close()
	if err != nil {
		Logger.Print("An error occurred closing the connection", err)
		return err
	}
	return nil
}
