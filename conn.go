package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net"
	"time"

	"net/url"
	"strings"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
	"io"
)

// Conn represents a connection to Neo4J
//
// Conn objects, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.  If you want to use multipe go routines with these objects,
// you should use a driver to create a new conn for each routine.
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
	timeout       time.Duration
	chunkSize     uint16
	closed        bool
}

// newBoltConn Creates a new bolt connection
func newBoltConn(connStr string) (*boltConn, error) {
	url, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	} else if strings.ToLower(url.Scheme) != "bolt" {
		return nil, fmt.Errorf("Unsupported connection string scheme: %s. Driver only supports 'bolt' scheme.", url.Scheme)
	}

	authToken := ""
	if url.User != nil {
		authToken = url.User.Username()
	}

	// TODO: TLS Support
	c := &boltConn{
		connStr:   connStr,
		url:       url,
		authToken: authToken,
		// TODO: Test best default
		// Default to 10 second timeout
		timeout: time.Second * time.Duration(10),
		// TODO: Test best default. Try math.MaxUint16
		// Default to 4096 byte chunks. Same as a golang bufio reader.
		chunkSize:     4096,
		serverVersion: make([]byte, 4),
	}

	err = c.initialize()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *boltConn) initialize() error {
	var err error
	c.conn, err = net.DialTimeout("tcp", c.url.Host, c.timeout)
	if err != nil {
		Logger.Println("An error occurred connecting:", err)
		return err
	}

	numWritten, err := c.Write(magicPreamble)
	if numWritten != 4 {
		Logger.Printf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 4", numWritten)
		if err != nil {
			Logger.Println("An error occurred writing magic preamble:", err)
		}
		c.Close()
		return err
	}

	numWritten, err = c.Write(supportedVersions)
	if numWritten != 16 {
		Logger.Printf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 16", numWritten)
		if err != nil {
			Logger.Println("An error occurred writing supported versions:", err)
		}
		c.Close()
		return err
	}

	numRead, err := c.Read(c.serverVersion)
	if numRead != 4 {
		Logger.Printf("Could not read server version response. Read %d bytes. Expected 4 bytes. Output: %s", numRead, c.serverVersion)
		if err != nil {
			Logger.Println("An error occurred reading server version:", err)
		}
		c.Close()
		return err
	} else if bytes.Compare(c.serverVersion, noVersionSupported) == 0 {
		Logger.Println("No version supported from server")
		c.Close()
		return fmt.Errorf("NO VERSION SUPPORTED")
	}

	initMessage := messages.NewInitMessage(ClientID, c.authToken)
	if err = encoding.NewEncoder(c, c.chunkSize).Encode(initMessage); err != nil {
		Logger.Println("An error occurred reading server version:", err)
		c.Close()
		return err
	}

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		c.Close()
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Successfully initiated Bolt connection:%+v", resp)
		return nil
	case messages.FailureMessage:
		Logger.Printf("Got a failure message when initializing connection :%+v", resp)
		c.Close()
		return fmt.Errorf("An error occurred initializing Neo4j Bolt Connection: %+v", resp.Metadata)
	default:
		Logger.Printf("Got an unrecognized message when initializing connection :%+v", resp)
		c.Close()
		return fmt.Errorf("Unrecognized response from the server: %#v", resp)
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err = c.conn.Read(b)
	TraceLogger.Printf("Read %d bytes from stream:\n\n%s\n", n, SprintByteHex(b))
	if err != nil && err != io.EOF {
		Logger.Println("An error occurred reading from stream: ", err)
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	n, err = c.conn.Write(b)
	TraceLogger.Printf("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, SprintByteHex(b[:n]))
	if err != nil {
		Logger.Println("An error occurred writing to stream: ", err)
	}
	return n, err
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *boltConn) Close() error {
	c.closed = true
	// TODO: Connection Pooling?
	err := c.conn.Close()

	if err != nil {
		Logger.Print("An error occurred closing the connection", err)
		return err
	}
	return nil
}

// Prepare prepares a new statement for a query
func (c *boltConn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, fmt.Errorf("Connection already closed")
	}
	return newStmt(query, c), nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	if c.closed {
		return nil, fmt.Errorf("Connection already closed")
	}
	// TODO: Implement

	return nil, nil
}

// Sets the size of the chunks to write to the stream
func (c *boltConn) SetChunkSize(chunkSize uint16) {
	c.chunkSize = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *boltConn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}
