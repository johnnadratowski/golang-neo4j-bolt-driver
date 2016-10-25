package golangNeo4jBoltDriver

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
	// PrepareNeo prepares a neo4j specific statement.
	PrepareNeo(query string) (Stmt, error)

	// PreparePipeline prepares a neo4j specific pipeline statement
	// Useful for running multiple queries at the same time.
	PreparePipeline(query ...string) (PipelineStmt, error)

	// QueryNeo queries using the Neo4j-specific interface.
	QueryNeo(query string, params map[string]interface{}) (Rows, error)

	// QueryNeoAll queries using the Neo4j-specific interface and returns all row data and output metadata.
	QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error)

	// QueryPipeline queries using the Neo4j-specific interface
	// pipelining multiple statements.
	QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error)

	// ExecNeo executes a query using the Neo4j-specific interface.
	ExecNeo(query string, params map[string]interface{}) (Result, error)

	// ExecPipeline executes a query using the Neo4j-specific interface
	// pipelining multiple statements.
	ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error)

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
	timeout time.Duration
	size    uint16
	status  status
	closed  bool

	transaction *boltTx
	statement   *boltStmt
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
			return fmt.Errorf("Error rolling back transaction when closing connection")
		}
	}

	if c.pool != nil {
		// If using connection pooling, don't close connection, just reclaim it
		c.pool.reclaim(c)
		return nil
	}

	err := c.conn.Close()
	c.closed = err == nil
	return err
}

func (c *conn) ackFailure(failure messages.FailureMessage) error {
	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.size).Encode(ack)
	if err != nil {
		return fmt.Errorf("An error occurred encoding ack failure message")
	}

	for {
		resp, err := encoding.NewDecoder(c).Decode()
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
	err := encoding.NewEncoder(c, c.size).Encode(reset)
	if err != nil {
		return fmt.Errorf("An error occurred encoding reset message")
	}

	for {
		resp, err := encoding.NewDecoder(c).Decode()
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

// Prepare prepares a new statement for a query
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *conn) PrepareNeo(query string) (Stmt, error) {
	return c.prepare(query)
}

// PreparePipeline prepares a new pipeline statement for a query.
func (c *conn) PreparePipeline(queries ...string) (PipelineStmt, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}
	c.statement = newPipelineStmt(queries, c)
	return c.statement, nil
}

func (c *conn) prepare(query string) (*boltStmt, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}
	c.statement = newStmt(query, c)
	return c.statement, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *conn) Begin() (driver.Tx, error) {
	if c.transaction != nil {
		return nil, fmt.Errorf("An open transaction already exists")
	}
	if c.statement != nil {
		return nil, fmt.Errorf("Cannot open a transaction when you already have an open statement")
	}
	if c.closed {
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
	resp, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return resp, err
	}
	if failure, ok := resp.(messages.FailureMessage); ok {
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, err
	}
	return resp, err
}

func (c *conn) consumeAll() ([]interface{}, interface{}, error) {
	log.Info("Consuming all responses until success/failure")

	var responses []interface{}
	for {
		resp, err := c.consume()
		if err != nil {
			return nil, resp, err
		}

		if success, isSuccess := resp.(messages.SuccessMessage); isSuccess {
			log.Infof("Got success message: %#v", success)
			return responses, success, nil
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
	err := encoding.NewEncoder(c, c.size).Encode(initMessage)
	if err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendRun(query string, args map[string]interface{}) error {
	log.Infof("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.size).Encode(runMessage); err != nil {
		return fmt.Errorf("An error occurred running query")
	}
	return nil
}

func (c *conn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.sendRun(query, args); err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendPullAll() error {
	log.Infof("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.size).Encode(pullAllMessage)
	if err != nil {
		return fmt.Errorf("An error occurred encoding pull all query")
	}

	return nil
}

func (c *conn) sendPullAllConsume() (interface{}, error) {
	if err := c.sendPullAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *conn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}
	return c.sendPullAll()
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
	return encoding.NewEncoder(c, c.size).Encode(msg)
}

func (c *conn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}
	return c.consume()
}

func (c *conn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
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

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.queryNeo(query, params)
}

func (c *conn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.queryNeo(query, params)
}

func (c *conn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rows, err := c.queryNeo(query, params)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	data, metadata, err := rows.All()
	return data, rows.metadata, metadata, err
}

var ErrClosed = fmt.Errorf("connection already closed")
var ErrOpen = fmt.Errorf("open connection already exists")

func (c *conn) queryNeo(query string, params map[string]interface{}) (*boltRows, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}

	c.statement = newStmt(query, c)

	// Pipeline the run + pull all for this
	successResp, err := c.sendRunPullAllConsumeRun(c.statement.query, params)
	if err != nil {
		return nil, err
	}
	success, ok := successResp.(messages.SuccessMessage)
	if !ok {
		return nil, fmt.Errorf("Unexpected response querying neo from connection: %#v", successResp)
	}

	c.statement.rows = newQueryRows(c.statement, success.Metadata)
	return c.statement.rows, nil
}

func (c *conn) QueryPipeline(queries []string, params ...map[string]interface{}) (PipelineRows, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}

	c.statement = newPipelineStmt(queries, c)
	rows, err := c.statement.QueryPipeline(params...)
	if err != nil {
		return nil, err
	}

	// Since we're not exposing the statement,
	// tell the rows to close it when they are closed
	rows.(*boltRows).closeStatement = true
	return rows, nil
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
// You must bolt encode a map to pass as []bytes for the driver value
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.Exec(args)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (c *conn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.ExecNeo(params)
}

func (c *conn) ExecPipeline(queries []string, params ...map[string]interface{}) ([]Result, error) {
	if c.statement != nil {
		return nil, ErrOpen
	}
	if c.closed {
		return nil, ErrClosed
	}

	stmt := newPipelineStmt(queries, c)
	defer stmt.Close()

	return stmt.ExecPipeline(params...)
}
