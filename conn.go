package bolt

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"net"
	"time"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/log"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

const DefaultChunkSize = math.MaxUint16

// Conn represents a connection to Neo4J implementing a Neo-friendly interface.
// Some of the features of this interface implement Neo-specific features
// unavailable in the sql/driver compatible interface
//
// Conn objects, and any prepared statements/transactions within are not
// thread safe. If you want to use multipe go routines with these objects you
// should use a driver to create a new conn for each routine.
type Conn interface {
	// Prepare prepares a neo4j specific statement.
	Prepare(query string) (stmt, error)

	// Query queries using the Neo4j-specific interface.
	Query(query string, params map[string]interface{}) (rows, error)

	// Exec executes a query using the Neo4j-specific interface.
	Exec(query string, params map[string]interface{}) (Result, error)

	// Close closes the connection.
	Close() error

	// Begin starts a new transaction.
	Begin() (driver.Tx, error)

	// SetChunkSize is used to set the max chunk size of the
	// bytes to send to Neo4j at once.
	SetChunkSize(uint16)

	// SetTimeout sets the read/write timeouts for the
	// connection to Neo4j.
	SetTimeout(time.Duration)
}

type status uint8

const (
	idle status = iota
	transaction
	statement
)

type conn struct {
	conn    net.Conn
	dec     *encoding.Decoder
	enc     *encoding.Encoder
	timeout time.Duration
	size    uint16
	status  status
	bad     bool
}

func (c *conn) decode() (interface{}, error) {
	if c.dec == nil {
		c.dec = encoding.NewDecoder(c)
	}
	if !c.dec.More() {
		return nil, io.EOF
	}
	return c.dec.Decode()
}

func (c *conn) encode(v interface{}) error {
	if c.enc == nil {
		c.enc = encoding.NewEncoder(c)
		c.enc.SetChunkSize(c.size)
	}
	return c.enc.Encode(v)
}

type boltConn struct {
	*conn
}

func newConn(netcn net.Conn, v values) (*conn, error) {
	timeout, err := parseTimeout(v.get("timeout"))
	if err != nil {
		return nil, err
	}

	c := &conn{conn: netcn, timeout: timeout, size: DefaultChunkSize}
	if err := c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			return nil, e
		}
		return nil, err
	}

	resp, err := c.sendInit(v.get("username"), v.get("password"))
	if err != nil {
		if e := c.Close(); e != nil {
			return nil, e
		}
		return nil, err
	}

	_, ok := resp.(messages.SuccessMessage)
	if !ok {
		if e := c.Close(); e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("unrecognized response from the server: %#v", resp)
	}
	return c, nil
}

func (c *conn) handShake() error {
	_, err := c.Write(handShake)
	if err != nil {
		return err
	}
	var vers [4]byte
	_, err = io.ReadFull(c, vers[:])
	if err != nil {
		return err
	}
	if vers == noVersionSupported {
		return fmt.Errorf("Server responded with no supported version")
	}
	return nil
}

// Read reads the data from the underlying connection
func (c *conn) Read(b []byte) (n int, err error) {
	err = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Read(b)
}

// Write writes the data to the underlying connection
func (c *conn) Write(b []byte) (n int, err error) {
	err = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Write(b)
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *conn) Close() error {
	if c.bad {
		return driver.ErrBadConn
	}
	err := c.conn.Close()
	c.bad = err == nil
	return err
}

func (c *conn) ackFailure(failure messages.FailureMessage) error {
	ack := messages.NewAckFailureMessage()
	err := c.encode(ack)
	if err != nil {
		return fmt.Errorf("An error occurred encoding ack failure message")
	}

	for {
		resp, err := c.decode()
		if err != nil {
			return fmt.Errorf("An error occurred decoding ack failure message response")
		}

		switch resp := resp.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when acking failure: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when acking failure: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when acking failure: %#v", resp)
			return c.reset()
		default:
			log.Errorf("Got unrecognized response from acking failure: %#v", resp)
			err := c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
		}
	}
}

func (c *conn) reset() error {
	log.Info("Resetting session")

	reset := messages.NewResetMessage()
	err := c.encode(reset)
	if err != nil {
		return fmt.Errorf("An error occurred encoding reset message")
	}

	for {
		resp, err := c.decode()
		if err != nil {
			return fmt.Errorf("An error occurred decoding reset message response")
		}

		switch resp := resp.(type) {
		case messages.IgnoredMessage:
			log.Infof("Got ignored message when resetting session: %#v", resp)
			continue
		case messages.SuccessMessage:
			log.Infof("Got success message when resetting session: %#v", resp)
			return nil
		case messages.FailureMessage:
			log.Errorf("Got failure message when resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Error resetting session: %#v. CLOSING SESSION!", resp)
		default:
			log.Errorf("Got unrecognized response from resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return fmt.Errorf("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
		}
	}
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *conn) Prepare(query string) (stmt, error) {
	return c.prepare(query)
}

func (c *conn) prepare(query string) (*boltStmt, error) {
	if c.bad {
		return nil, ErrClosed
	}
	return &boltStmt{conn: c, query: query}, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *conn) Begin() (driver.Tx, error) {
	if c.bad {
		return nil, ErrClosed
	}

	successInt, pullInt, err := c.sendRunPullAllConsumeSingle("BEGIN", nil)
	if err != nil {
		return nil, fmt.Errorf("An error occurred beginning transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return nil, fmt.Errorf("Unrecognized response type beginning transaction: %#v", success)
	}

	log.Infof("Got success message beginning transaction: %#v", success)

	success, ok = pullInt.(messages.SuccessMessage)
	if !ok {
		return nil, fmt.Errorf("Unrecognized response type pulling transaction:  %#v", success)
	}

	log.Infof("Got success message pulling transaction: %#v", success)

	return newTx(c), nil
}

// Sets the size of the chunks to write to the stream
func (c *conn) SetChunkSize(chunkSize uint16) {
	c.size = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *conn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

func (c *conn) consume() (interface{}, error) {
	resp, err := c.decode()
	if err != nil {
		return resp, err
	}
	if failure, ok := resp.(messages.FailureMessage); ok {
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, nil
	}
	return resp, err
}

func (c *conn) consumeAll() ([]interface{}, interface{}, error) {
	var responses []interface{}
	for {
		resp, err := c.consume()
		if err != nil {
			return nil, resp, err
		}
		smg, ok := resp.(messages.SuccessMessage)
		if ok {
			return responses, smg, nil
		}
		responses = append(responses, resp)
	}
}

func (c *conn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
	log.Info("Consuming all responses %d times until success/failure", mult)

	responses := make([][]interface{}, mult)
	successes := make([]interface{}, mult)
	for i := 0; i < mult; i++ {

		resp, success, err := c.consumeAll()
		if err != nil {
			return responses, successes, err
		}

		responses[i] = resp
		successes[i] = success
	}

	return responses, successes, nil
}

func (c *conn) sendInit(user, pass string) (interface{}, error) {
	initMessage := messages.NewInitMessage(ClientID, user, pass)
	err := c.encode(initMessage)
	if err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) run(query string, args map[string]interface{}) error {
	runMessage := messages.NewRunMessage(query, args)
	return c.encode(runMessage)
}

func (c *conn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.run(query, args); err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) pullAll() error {
	pullAllMessage := messages.NewPullAllMessage()
	return c.encode(pullAllMessage)
}

func (c *conn) pullAllConsume() (interface{}, error) {
	if err := c.pullAll(); err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.run(query, args)
	if err != nil {
		return err
	}
	return c.pullAll()
}

func (c *conn) sendRunPullAllConsumeRun(query string, args map[string]interface{}) (interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendRunPullAllConsumeSingle(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, err
	}

	pullSuccess, err := c.consume()
	return runSuccess, pullSuccess, err
}

func (c *conn) sendRunPullAllConsumeAll(query string, args map[string]interface{}) (interface{}, interface{}, []interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, nil, nil, err
	}

	runSuccess, err := c.consume()
	if err != nil {
		return runSuccess, nil, nil, err
	}

	records, pullSuccess, err := c.consumeAll()
	return runSuccess, pullSuccess, records, err
}

func (c *conn) sendDiscardAll() error {
	msg := messages.NewDiscardAllMessage()
	return c.encode(msg)
}

func (c *conn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.run(query, args)
	if err != nil {
		return err
	}
	return c.sendDiscardAll()
}

func (c *conn) sendRunDiscardAllConsume(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	runResp, err := c.sendRunConsume(query, args)
	if err != nil {
		return runResp, nil, err
	}
	discardResp, err := c.sendDiscardAllConsume()
	return runResp, discardResp, err
}

var ErrClosed = fmt.Errorf("connection already closed")

func (c *conn) query(query string, args map[string]interface{}) (*boltRows, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	stmt := &boltStmt{conn: c, query: query}
	err := stmt.exec(args)
	if err != nil {
		return nil, err
	}
	return &boltRows{conn: c, md: stmt.md}, nil
}
