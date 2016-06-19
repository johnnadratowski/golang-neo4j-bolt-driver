package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"net"
	"time"

	"net/url"
	"strings"

	"io"
	"math"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Conn represents a connection to Neo4J
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
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
	transaction   *boltTx
	statement     *boltStmt
}

// newBoltConn Creates a new bolt connection
func newBoltConn(connStr string) (*boltConn, error) {
	url, err := url.Parse(connStr)
	if err != nil {
		return nil, fmt.Errorf("An error occurred parsing bolt URL: %s", err)
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
		// TODO: Test best default.
		chunkSize:     math.MaxUint16,
		serverVersion: make([]byte, 4),
	}

	err = c.initialize()
	if err != nil {
		return nil, fmt.Errorf("An error occurred initializing connection: %s", err)
	}

	return c, nil
}

func (c *boltConn) handShake() error {

	numWritten, err := c.Write(magicPreamble)
	if numWritten != 4 {
		Logger.Printf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 4", numWritten)
		if err != nil {
			Logger.Printf("An error occurred writing magic preamble: %s", err)
			err = fmt.Errorf("An error occurred writing magic preamble: %s", err)
		}
		return err
	}

	numWritten, err = c.Write(supportedVersions)
	if numWritten != 16 {
		Logger.Printf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 16", numWritten)
		if err != nil {
			Logger.Printf("An error occurred writing supported versions: %s", err)
			err = fmt.Errorf("An error occurred writing supported versions: %s", err)
		}
		return err
	}

	numRead, err := c.Read(c.serverVersion)
	if numRead != 4 {
		Logger.Printf("Could not read server version response. Read %d bytes. Expected 4 bytes. Output: %s", numRead, c.serverVersion)
		if err != nil {
			Logger.Printf("An error occurred reading server version: %s", err)
			err = fmt.Errorf("An error occurred reading server version: %s", err)
		}
		return err
	} else if bytes.Equal(c.serverVersion, noVersionSupported) {
		Logger.Println("No version supported from server")
		return fmt.Errorf("Server responded with no supported version")
	}

	return nil
}

func (c *boltConn) initialize() error {
	var err error
	c.conn, err = net.DialTimeout("tcp", c.url.Host, c.timeout)
	if err != nil {
		Logger.Printf("An error occurred connecting: %s", err)
		return fmt.Errorf("An error occurred dialing to neo4j: %s", err)
	}

	if err = c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			Logger.Printf("An error occurred closing connection: %s", e)
		}
		return err
	}

	respInt, err := c.sendInit()
	if err != nil {
		if e := c.Close(); e != nil {
			Logger.Printf("An error occurred closing connection: %s", e)
		}
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Successfully initiated Bolt connection: %+v", resp)
		return nil
	default:
		Logger.Printf("Got an unrecognized message when initializing connection :%+v", resp)
		if e := c.Close(); e != nil {
			Logger.Printf("An error occurred closing connection: %s", e)
		}
		return fmt.Errorf("Unrecognized response from the server: %#v", resp)
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, fmt.Errorf("An error occurred setting read deadline: %s", err)
	}
	n, err = c.conn.Read(b)
	TraceLogger.Printf("Read %d bytes from stream:\n\n%s\n", n, sprintByteHex(b))
	if err != nil && err != io.EOF {
		Logger.Printf("An error occurred reading from stream: %s", err)
		err = fmt.Errorf("An error occurred reading from stream: %s", err)
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, fmt.Errorf("An error occurred setting write deadline: %s", err)
	}
	n, err = c.conn.Write(b)
	TraceLogger.Printf("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, sprintByteHex(b[:n]))
	if err != nil {
		Logger.Printf("An error occurred writing to stream: %s", err)
		err = fmt.Errorf("An error occurred writing to stream: %s", err)
	}
	return n, err
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *boltConn) Close() error {
	if c.closed {
		return nil
	}

	if c.transaction != nil {
		if err := c.transaction.Rollback(); err != nil {
			return err
		}
	}

	if c.statement != nil {
		if err := c.statement.Close(); err != nil {
			return err
		}
	}

	if c.transaction != nil {
		if err := c.transaction.Rollback(); err != nil {
			return fmt.Errorf("Error rolling back transaction when closing connection: %s", err)
		}
	}

	// TODO: Connection Pooling?
	err := c.conn.Close()
	c.closed = true
	if err != nil {
		Logger.Printf("An error occurred closing the connection %s", err)
		return fmt.Errorf("An error occurred closing the connection: %s", err)
	}

	return nil
}

func (c *boltConn) ackFailure(failure messages.FailureMessage) error {
	Logger.Printf("Acknowledging Failure: %#v", failure)

	// TODO: Try RESET on failures?

	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(ack)
	if err != nil {
		return fmt.Errorf("An error occurred encoding ack failure message: %s", err)
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return fmt.Errorf("An error occurred decoding ack failure message response: %s", err)
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			Logger.Printf("Got ignored message when acking failure: %#v", resp)
			continue
		case messages.SuccessMessage:
			Logger.Printf("Got success message when acking failure: %#v", resp)
			return nil
		case messages.FailureMessage:
			Logger.Printf("Got failure message when acking failure: %#v", resp)
			return c.reset()
		default:
			Logger.Printf("Got unrecognized response from acking failure: %#v", resp)
			err := c.Close()
			if err != nil {
				Logger.Printf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
		}
	}
}

func (c *boltConn) reset() error {
	Logger.Printf("Resetting session")

	reset := messages.NewResetMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(reset)
	if err != nil {
		return fmt.Errorf("An error occurred encoding reset message: %s", err)
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return fmt.Errorf("An error occurred decoding reset message response: %s", err)
		}

		switch resp := respInt.(type) {
		case messages.IgnoredMessage:
			Logger.Printf("Got ignored message when resetting session: %#v", resp)
			continue
		case messages.SuccessMessage:
			Logger.Printf("Got success message when resetting session: %#v", resp)
			return nil
		case messages.FailureMessage:
			Logger.Printf("Got failure message when resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				Logger.Printf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Error resetting session: %#v. CLOSING SESSION!", resp)
		default:
			Logger.Printf("Got unrecognized response from resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				Logger.Printf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
		}
	}
}

// Prepare prepares a new statement for a query
func (c *boltConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) PrepareNeo(query string) (Stmt, error) {
	return c.prepare(query)
}

func (c *boltConn) prepare(query string) (Stmt, error) {
	if c.statement != nil {
		return nil, fmt.Errorf("An open statement already exists")
	}
	if c.closed {
		return nil, fmt.Errorf("Connection already closed")
	}
	c.statement = newStmt(query, c)
	return c.statement, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	if c.transaction != nil {
		return nil, fmt.Errorf("An open transaction already exists")
	}
	if c.closed {
		return nil, fmt.Errorf("Connection already closed")
	}

	respInt, err := c.sendRun("BEGIN", nil)
	if err != nil {
		return nil, fmt.Errorf("An error occurred beginning transaction: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Got success message beginning transaction: %#v", resp)
		return newTx(c), nil
	default:
		return nil, fmt.Errorf("Unrecognized response type beginning transaction: %T Value: %#v", resp, resp)
	}
}

// Sets the size of the chunks to write to the stream
func (c *boltConn) SetChunkSize(chunkSize uint16) {
	c.chunkSize = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *boltConn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

func (c *boltConn) consume() (interface{}, error) {
	Logger.Println("Consuming response from bolt stream")

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return respInt, err
	}

	if failure, isFail := respInt.(messages.FailureMessage); isFail {
		Logger.Printf("Got failure message: %#v", failure)
		err := c.ackFailure(failure)
		if err != nil {
			Logger.Printf("An error occurred acking failure: %s", err)
			return nil, err
		}
		return failure, fmt.Errorf("Got failure message: %#v", failure)
	}
	return respInt, err
}

func (c *boltConn) consumeAll() ([]interface{}, interface{}, error) {
	Logger.Println("Consuming all responses until success/failure")

	responses := []interface{}{}
	for {
		respInt, err := c.consume()
		if err != nil {
			Logger.Printf("An error occurred consuming all from stream: %s", err)
			return nil, respInt, err
		}

		if success, isSuccess := respInt.(messages.SuccessMessage); isSuccess {
			Logger.Printf("Got success message: %#v", success)
			return responses, success, nil
		}

		responses = append(responses, respInt)
	}
}

func (c *boltConn) sendInit() (interface{}, error) {
	Logger.Printf("Sending INIT Message. ClientID: %s AuthToken: %s", ClientID, c.authToken)

	initMessage := messages.NewInitMessage(ClientID, c.authToken)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(initMessage); err != nil {
		Logger.Printf("An error occurred sending init message: %s", err)
		return nil, fmt.Errorf("An error occurred sending init message: %s", err)
	}

	return c.consume()
}

func (c *boltConn) sendRun(query string, args map[string]interface{}) (interface{}, error) {
	Logger.Printf("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(runMessage); err != nil {
		Logger.Printf("An error occurred running query: %s", err)
		return nil, fmt.Errorf("An error occurred running query: %s", err)
	}

	return c.consume()
}

func (c *boltConn) sendPullAll() error {
	Logger.Println("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(pullAllMessage)
	if err != nil {
		Logger.Printf("An error occurred encoding pull all query: %s", err)
		return fmt.Errorf("An error occurred encoding pull all query: %s", err)
	}

	return nil
}

func (c *boltConn) sendDiscardAll() (interface{}, error) {
	Logger.Println("Sending DISCARD_ALL message")

	discardAllMessage := messages.NewDiscardAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(discardAllMessage)
	if err != nil {
		Logger.Printf("An error occurred encoding discard all query: %s", err)
		return nil, fmt.Errorf("An error occurred encoding discard all query: %s", err)
	}

	return c.consume()
}
