package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"net"
	"time"

	"net/url"
	"strings"

	"io"
	"math"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
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
	PrepareNeo(query string) (Stmt, error)
	PreparePipeline(query ...string) (PipelineStmt, error)
	Query(query string, args []driver.Value) (driver.Rows, error)
	QueryNeo(query string, params map[string]interface{}) (Rows, error)
	QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error)
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
		return nil, errors.Wrap(err, "An error occurred parsing bolt URL")
	} else if strings.ToLower(url.Scheme) != "bolt" {
		return nil, errors.New("Unsupported connection string scheme: %s. Driver only supports 'bolt' scheme.", url.Scheme)
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
		return nil, errors.Wrap(err, "An error occurred initializing connection")
	}

	return c, nil
}

func (c *boltConn) handShake() error {

	numWritten, err := c.Write(magicPreamble)
	if numWritten != 4 {
		log.Errorf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 4", numWritten)
		if err != nil {
			err = errors.Wrap(err, "An error occurred writing magic preamble")
		}
		return err
	}

	numWritten, err = c.Write(supportedVersions)
	if numWritten != 16 {
		log.Errorf("Couldn't write expected bytes for magic preamble. Written: %d. Expected: 16", numWritten)
		if err != nil {
			err = errors.Wrap(err, "An error occurred writing supported versions")
		}
		return err
	}

	numRead, err := c.Read(c.serverVersion)
	if numRead != 4 {
		log.Errorf("Could not read server version response. Read %d bytes. Expected 4 bytes. Output: %s", numRead, c.serverVersion)
		if err != nil {
			err = errors.Wrap(err, "An error occurred reading server version")
		}
		return err
	} else if bytes.Equal(c.serverVersion, noVersionSupported) {
		return errors.New("Server responded with no supported version")
	}

	return nil
}

func (c *boltConn) initialize() error {
	var err error
	c.conn, err = net.DialTimeout("tcp", c.url.Host, c.timeout)
	if err != nil {
		return errors.Wrap(err, "An error occurred dialing to neo4j")
	}

	if err = c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	respInt, err := c.sendInit()
	if err != nil {
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully initiated Bolt connection: %+v", resp)
		return nil
	default:
		log.Errorf("Got an unrecognized message when initializing connection :%+v", resp)
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return errors.New("Unrecognized response from the server: %#v", resp)
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, errors.Wrap(err, "An error occurred setting read deadline")
	}

	n, err = c.conn.Read(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Read %d bytes from stream:\n\n%s\n", n, sprintByteHex(b))
	}

	if err != nil && err != io.EOF {
		err = errors.Wrap(err, "An error occurred reading from stream")
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, errors.Wrap(err, "An error occurred setting write deadline")
	}

	n, err = c.conn.Write(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, sprintByteHex(b[:n]))
	}

	if err != nil {
		err = errors.Wrap(err, "An error occurred writing to stream")
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
			return errors.Wrap(err, "Error rolling back transaction when closing connection")
		}
	}

	// TODO: Connection Pooling?
	err := c.conn.Close()
	c.closed = true
	if err != nil {
		return errors.Wrap(err, "An error occurred closing the connection")
	}

	return nil
}

func (c *boltConn) ackFailure(failure messages.FailureMessage) error {
	log.Infof("Acknowledging Failure: %#v", failure)

	// TODO: Try RESET on failures?

	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(ack)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding ack failure message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding ack failure message response")
		}

		switch resp := respInt.(type) {
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
			return errors.New("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
		}
	}
}

func (c *boltConn) reset() error {
	log.Info("Resetting session")

	reset := messages.NewResetMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(reset)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding reset message")
	}

	for {
		respInt, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding reset message response")
		}

		switch resp := respInt.(type) {
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
			return errors.New("Error resetting session: %#v. CLOSING SESSION!", resp)
		default:
			log.Errorf("Got unrecognized response from resetting session: %#v", resp)
			err = c.Close()
			if err != nil {
				log.Errorf("An error occurred closing the session: %s", err)
			}
			return errors.New("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
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

// PreparePipeline prepares a new pipeline statement for a query.
func (c *boltConn) PreparePipeline(queries ...string) (PipelineStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newPipelineStmt(queries, c)
	return c.statement, nil
}

func (c *boltConn) prepare(query string) (*boltStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newStmt(query, c)
	return c.statement, nil
}

// Begin begins a new transaction with the Neo4J Database
func (c *boltConn) Begin() (driver.Tx, error) {
	if c.transaction != nil {
		return nil, errors.New("An open transaction already exists")
	}
	if c.statement != nil {
		return nil, errors.New("Cannot open a transaction when you already have an open statement")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	successInt, pullInt, err := c.sendRunPullAllConsumeSingle("BEGIN", nil)
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred beginning transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type beginning transaction: %#v", success)
	}

	log.Infof("Got success message beginning transaction: %#v", success)

	success, ok = pullInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type pulling transaction:  %#v", success)
	}

	log.Infof("Got success message pulling transaction: %#v", success)

	return newTx(c), nil
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
	log.Info("Consuming response from bolt stream")

	respInt, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return respInt, err
	}

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Consumed Response: %#v", respInt)
	}

	if failure, isFail := respInt.(messages.FailureMessage); isFail {
		log.Errorf("Got failure message: %#v", failure)
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, errors.New("Got failure message: %#v", failure)
	}

	return respInt, err
}

func (c *boltConn) consumeAll() ([]interface{}, interface{}, error) {
	log.Info("Consuming all responses until success/failure")

	responses := []interface{}{}
	for {
		respInt, err := c.consume()
		if err != nil {
			return nil, respInt, err
		}

		if success, isSuccess := respInt.(messages.SuccessMessage); isSuccess {
			log.Infof("Got success message: %#v", success)
			return responses, success, nil
		}

		responses = append(responses, respInt)
	}
}

func (c *boltConn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
	log.Info("Consuming all responses %d times until success/failure", mult)

	responses := make([][]interface{}, mult)
	successes := make([]interface{}, mult)
	for i := 0; i < mult; i ++ {

		resp, success, err := c.consumeAll()
		if err != nil {
			return responses, successes, err
		}

		responses[i] = resp
		successes[i] = success
	}

	return responses, successes, nil
}

func (c *boltConn) sendInit() (interface{}, error) {
	log.Infof("Sending INIT Message. ClientID: %s AuthToken: %s", ClientID, c.authToken)

	initMessage := messages.NewInitMessage(ClientID, c.authToken)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(initMessage); err != nil {
		return nil, errors.Wrap(err, "An error occurred sending init message")
	}

	return c.consume()
}

func (c *boltConn) sendRun(query string, args map[string]interface{}) error {
	log.Infof("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(runMessage); err != nil {
		return errors.Wrap(err, "An error occurred running query")
	}

	return nil
}

func (c *boltConn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.sendRun(query, args); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendPullAll() error {
	log.Infof("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(pullAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding pull all query")
	}

	return nil
}

func (c *boltConn) sendPullAllConsume() (interface{}, error) {
	if err := c.sendPullAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendPullAll()
}

func (c *boltConn) sendRunPullAllConsumeRun(query string, args map[string]interface{}) (interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunPullAllConsumeSingle(query string, args map[string]interface{}) (interface{}, interface{}, error) {
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

func (c *boltConn) sendRunPullAllConsumeAll(query string, args map[string]interface{}) (interface{}, interface{}, []interface{}, error) {
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

func (c *boltConn) sendDiscardAll() error {
	log.Infof("Sending DISCARD_ALL message")

	discardAllMessage := messages.NewDiscardAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(discardAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding discard all query")
	}

	return nil
}

func (c *boltConn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *boltConn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendDiscardAll()
}

func (c *boltConn) sendRunDiscardAllConsume(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	runResp, err := c.sendRunConsume(query, args)
	if err != nil {
		return runResp, nil, err
	}

	discardResp, err := c.sendDiscardAllConsume()
	return runResp, discardResp, err
}

func (c *boltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.queryNeo(query, params)
}

func (c *boltConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.queryNeo(query, params)
}

func (c *boltConn) queryNeo(query string, params map[string]interface{}) (*boltRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = newStmt(query, c)

	// Pipeline the run + pull all for this
	successResp, err := c.sendRunPullAllConsumeRun(c.statement.query, params)
	if err != nil {
		return nil, err
	}
	success, ok := successResp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unexpected response querying neo from connection: %#v", successResp)
	}

	c.statement.rows = newQueryRows(c.statement, success.Metadata)
	return c.statement.rows, nil
}

func (c *boltConn) QueryPipeline(queries []string, params ...map[string]interface{}) (PipelineRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	if len(params) != len(queries) {
		return nil, errors.New("Must pass same number of params as there are queries")
	}

	c.statement = newPipelineStmt(queries, c)
	return c.statement.QueryPipeline(params...)
}
