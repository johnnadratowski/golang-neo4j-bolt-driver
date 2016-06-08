package golang_neo4j_bolt_driver

import "net"

type Conn interface {
	Close() error
}

type boltConn struct {
	connStr       string
	conn          net.Conn
	serverVersion []byte
}

func (c *boltConn) Close() error {
	err := c.conn.Close()
	if err != nil {
		Logger.Print("An error occurred closing the connection", err)
		return err
	}
	return nil
}
