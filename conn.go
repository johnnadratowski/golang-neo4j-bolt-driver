package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net"
	"time"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
	"net/url"
	"strings"
)

// Conn represents a connection to Neo4J
type Conn interface {
	Prepare(query string) (driver.Stmt, error)
	Close() error
	Begin() (driver.Tx, error)
	SetChunkSize(uint16)
	SetTimeout(time.Duration)
}

type boltConn struct {
	connStr       string
	url           *url.URL
	authToken     string
	conn          net.Conn
	serverVersion []byte
	initialized   bool
	timeout       time.Duration
	chunkSize     uint16
}

// newBoltConn Creates a new bolt connection
func newBoltConn(connStr string) (*boltConn, error) {
	url, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	} else if strings.ToLower(url.Scheme) != "bolt" {
		return nil, fmt.Errorf("Unsupported connection string scheme: %s. Driver only supports 'bolt' scheme.", url.Scheme)
	}

	// TODO: TLS Support
	c := &boltConn{
		connStr:   connStr,
		url:       url,
		authToken: url.User.Username(),
		// TODO: Test best default
		// Default to 10 second timeout
		timeout: time.Second * time.Duration(10),
		// TODO: Test best default
		// Default to 2048 byte chunks
		chunkSize: 2048,
	}

	c.conn, err = net.Dial("tcp", "bolt://"+c.url.Host)
	if err != nil {
		Logger.Println("An error occurred connecting:", err)
		return nil, err
	}

	c.Write(magicPreamble)
	c.Write(supportedVersions)

	_, err = c.Read(c.serverVersion)
	if err != nil {
		Logger.Println("An error occurred reading server version:", err)
		return nil, err
	}

	if bytes.Compare(c.serverVersion, noVersionSupported) == 0 {
		Logger.Println("No version supported from server")
		return nil, fmt.Errorf("NO VERSION SUPPORTED")
	}

	if err = encoding.NewEncoder(c, c.chunkSize).Encode(messages.NewInitMessage(ClientID, c.authToken)); err != nil {
		return nil, err
	}

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return nil, err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		return c, nil
	case messages.FailureMessage:
		c.Close()
		return nil, fmt.Errorf("An error occurred initializing Neo4j Bolt Connection: %+v", resp.Metadata)
	default:
		c.Close()
		return nil, fmt.Errorf("Unrecognized response from the server")
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

// Sets the size of the chunks to write to the stream
func (c *boltConn) SetChunkSize(chunkSize uint16) {
	c.chunkSize = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *boltConn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}
