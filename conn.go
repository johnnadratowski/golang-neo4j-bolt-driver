package bolt

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

var ErrInFailedTransaction = errors.New("bolt: Could not complete operation in a failed transaction")

func errBadResp(action string, v interface{}) error {
	return fmt.Errorf("unrecognized response while %s: %#v", action, v)
}

// Conn is a connection to Neo4J. It's interface is similar to that of sql.DB,
// with some minor modifications for ease of use. In particular, the variadic
// arguments in Query, Exec, etc. have been replaced with map[string]interface{}.
type Conn interface {
	// Prepare returns a prepared statement, bound to this connection.
	Prepare(query string) (stmt, error)

	// Query queries using the Neo4j-specific interface.
	Query(query string, params map[string]interface{}) (rows, error)

	// Exec executes a query using the Neo4j-specific interface.
	Exec(query string, params map[string]interface{}) (Result, error)

	// Close closes the current connection, invalidating any transactions
	// and statements.
	Close() error

	// Begin starts a new transaction.
	Begin() (driver.Tx, error)

	// SetChunkSize sets the maximum chunk size for writes to Neo4j.
	SetChunkSize(uint16)

	// SetTimeout sets the read and write timeouts for the connection.
	SetTimeout(time.Duration)
}

type status uint8

const (
	statusIdle    status = iota // idle
	statusInTx                  // in a transaction
	statusInBadTx               // in a bad transaction
)

type conn struct {
	conn net.Conn

	// dec and enc should not be used outright--use the decode and encode
	// methods instead
	dec *encoding.Decoder
	enc *encoding.Encoder

	timeout time.Duration
	size    uint16
	status  status
	bad     bool
}

// decode returns the next message from the connection if it exists. It returns
// io.EOF when the stream has finished.
func (c *conn) decode() (interface{}, error) {
	if c.dec == nil {
		c.dec = encoding.NewDecoder(c)
	}
	if !c.dec.More() {
		return nil, io.EOF
	}
	return c.dec.Decode()
}

// encode writes the bolt-encoded form of v to the connection.
func (c *conn) encode(v interface{}) error {
	if c.enc == nil {
		c.enc = encoding.NewEncoder(c)
		c.enc.SetChunkSize(c.size)
	}
	return c.enc.Encode(v)
}

// newConn creates a new Neo4j connection using the provided values.
func newConn(netcn net.Conn, v values) (*conn, error) {
	timeout, err := parseTimeout(v.get("timeout"))
	if err != nil {
		return nil, err
	}

	c := &conn{conn: netcn, timeout: timeout, size: encoding.DefaultChunkSize}
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

// handshake completes the bolt protocol's version handshake.
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
		return fmt.Errorf("server responded with no supported version")
	}
	return nil
}

// Read implements io.Reader with conn's timeout.
func (c *conn) Read(b []byte) (n int, err error) {
	err = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Read(b)
}

// Write implements io.Writer with conn's timeout.
func (c *conn) Write(b []byte) (n int, err error) {
	err = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Write(b)
}

// Close closes the connection.
func (c *conn) Close() error {
	if c.bad {
		return driver.ErrBadConn
	}
	c.status = statusIdle
	err := c.conn.Close()
	c.bad = err == nil
	return err
}

// ackFailure responds to a failure message allowing the connection to proceed.
// https://github.com/neo4j-contrib/boltkit/blob/b2739a15871aae8469363b0298f8765a4ec77a9a/boltkit/driver.py#L662
func (c *conn) ackFailure() error {
	err := c.encode(messages.NewAckFailureMessage())
	if err != nil {
		return err
	}

	for {
		resp, err := c.decode()
		if err != nil {
			return err
		}

		switch resp := resp.(type) {
		case messages.IgnoredMessage:
			// OK
		case messages.SuccessMessage:
			return nil
		case messages.FailureMessage:
			return c.reset()
		default:
			c.Close()
			return fmt.Errorf("got unrecognized response from acking failure: %#v ", resp)
		}
	}
}

// reset clears the connection.
// https://github.com/neo4j-contrib/boltkit/blob/b2739a15871aae8469363b0298f8765a4ec77a9a/boltkit/driver.py#L672
func (c *conn) reset() error {
	err := c.encode(messages.NewResetMessage())
	if err != nil {
		return err
	}

	for {
		resp, err := c.decode()
		if err != nil {
			return err
		}

		switch resp := resp.(type) {
		case messages.IgnoredMessage:
			// OK
		case messages.SuccessMessage:
			return nil
		case messages.FailureMessage:
			c.Close()
			return fmt.Errorf("error resetting session: %#v ", resp)
		default:
			c.Close()
			return fmt.Errorf("got unrecognized response from resetting session: %#v ", resp)
		}
	}
}

// Prepare prepares a new statement for a query.
func (c *conn) Prepare(query string) (stmt, error) {
	return c.prepare(query)
}

func (c *conn) prepare(query string) (*boltStmt, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	return &boltStmt{conn: c, query: query}, nil
}

type txQuery string

const (
	begin    txQuery = "BEGIN"
	commit   txQuery = "COMMIT"
	rollback txQuery = "ROLLBACK"
)

func (t txQuery) verb() string {
	switch t {
	case begin:
		return "beginning"
	case commit:
		return "committing"
	case rollback:
		return "rolling back"
	default:
		return "unknown"
	}
}

// transac executes the given transaction query.
func (c *conn) transac(query txQuery) error {
	switch query {
	case begin, commit, rollback:
	default:
		return fmt.Errorf("invalid transaction query: %q", query)
	}

	sifc, pifc, err := c.sendRunPullAllConsumeSingle(string(query), nil)
	if err != nil {
		return err
	}

	success, ok := sifc.(messages.SuccessMessage)
	if !ok {
		return errBadResp(query.verb()+" transaction", success)
	}

	pull, ok := pifc.(messages.SuccessMessage)
	if !ok {
		c.status = statusInBadTx
		return errBadResp("pulling transaction response", pull)
	}
	return nil
}

func (c *conn) checktx(intx bool) error {
	if (c.status == statusInTx || c.status == statusInBadTx) != intx {
		c.bad = true
		return fmt.Errorf("unexpected transaction status: %v", c.status)
	}
	return nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *conn) Begin() (driver.Tx, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	err := c.checktx(false)
	if err != nil {
		return nil, err
	}
	err = c.transac(begin)
	if err != nil {
		return nil, err
	}
	c.status = statusInTx
	return c, nil
}

// Commit commits and closes the transaction.
func (c *conn) Commit() error {
	if c.bad {
		return driver.ErrBadConn
	}
	err := c.checktx(true)
	if err != nil {
		return err
	}
	if c.status == statusInBadTx {
		if err := c.Rollback(); err != nil {
			return err
		}
		return ErrInFailedTransaction
	}
	return c.transac(commit)
}

// Rollback rolls back and closes the transaction
func (c *conn) Rollback() error {
	if c.bad {
		return errors.New("transaction already closed")
	}
	err := c.checktx(true)
	if err != nil {
		return err
	}
	err = c.transac(rollback)
	if err != nil {
		return err
	}
	c.status = statusIdle
	return nil
}

// Sets the size of the chunks to write to the stream
func (c *conn) SetChunkSize(chunkSize uint16) {
	c.size = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *conn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

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

// consume returns the next value from the connection, acknowledging any
// failures that occurred.
func (c *conn) consume() (interface{}, error) {
	resp, err := c.decode()
	if err != nil {
		return resp, err
	}
	if fail, ok := resp.(messages.FailureMessage); ok {
		err := c.ackFailure()
		if err != nil {
			return nil, err
		}
		return fail, nil
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
	return c.encode(messages.NewPullAllMessage())
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
