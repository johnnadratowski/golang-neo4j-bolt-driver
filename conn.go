package golangNeo4jBoltDriver

import "net"

// Conn represents a connection to Neo4J
type Conn interface {
	Close() error
}

type boltConn struct {
	connStr       string
	conn          net.Conn
	serverVersion []byte
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
