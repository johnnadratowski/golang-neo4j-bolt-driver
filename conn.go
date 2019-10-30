package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"net/url"
	"strings"

	"io"
	"math"

	"crypto/tls"
	"crypto/x509"
	"strconv"

	"github.com/mindstand/golang-neo4j-bolt-driver/encoding"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"github.com/mindstand/golang-neo4j-bolt-driver/log"
	"github.com/mindstand/golang-neo4j-bolt-driver/structures/messages"
)

// Conn represents a connection to Neo4J
//
// Implements a neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Conn objects, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.  If you want to use multipe go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Conn interface {
	// PrepareNeo prepares a neo4j specific statement
	PrepareNeo(query string) (Stmt, error)
	// PreparePipeline prepares a neo4j specific pipeline statement
	// Useful for running multiple queries at the same time
	PreparePipeline(query ...string) (PipelineStmt, error)
	// QueryNeo queries using the neo4j-specific interface
	QueryNeo(query string, params map[string]interface{}) (Rows, error)
	// QueryNeoAll queries using the neo4j-specific interface and returns all row data and output metadata
	QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error)
	// QueryPipeline queries using the neo4j-specific interface
	// pipelining multiple statements
	QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error)
	// ExecNeo executes a query using the neo4j-specific interface
	ExecNeo(query string, params map[string]interface{}) (Result, error)
	// ExecPipeline executes a query using the neo4j-specific interface
	// pipelining multiple statements
	ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error)
	// Close closes the connection
	Close() error
	// Begin starts a new transaction
	Begin() (driver.Tx, error)
	// SetChunkSize is used to set the max chunk size of the
	// bytes to send to Neo4j at once
	SetChunkSize(uint16)
	// SetTimeout sets the read/write timeouts for the
	// connection to Neo4j
	SetTimeout(time.Duration)
}

type BoltConn struct {
	connStr       string
	url           *url.URL
	user          string
	password      string
	conn          net.Conn
	connErr       error
	serverVersion []byte
	timeout       time.Duration
	chunkSize     uint16
	closed        bool
	useTLS        bool
	certFile      string
	caCertFile    string
	keyFile       string
	tlsNoVerify   bool
	readOnly      bool
	transaction   *boltTx
	statement     *boltStmt
}

func createBoltConn(connStr string) (*BoltConn, error) {
	conn := &BoltConn{
		connStr:       connStr,
		connErr:       nil,
		timeout:       time.Second * time.Duration(60),
		chunkSize:     math.MaxUint16,
		serverVersion: make([]byte, 4),
	}

	err := conn.initialize()
	if err != nil {
		return nil, err
	}

	return conn, nil
}

//url, is in a cluster, error
func (c *BoltConn) parseURL() (*url.URL, error) {
	user := ""
	password := ""
	url, err := url.Parse(c.connStr)
	if err != nil {
		return url, errors.Wrap(err, "An error occurred parsing bolt URL")
	} else if strings.ToLower(url.Scheme) != "bolt" && strings.ToLower(url.Scheme) != "bolt+routing" {
		return url, errors.New("Unsupported connection string scheme: %s. Driver only supports 'bolt' and 'bolt+routing' scheme.", url.Scheme)
	}

	if url.User != nil {
		c.user = url.User.Username()
		var isSet bool
		c.password, isSet = url.User.Password()
		if !isSet {
			return url, errors.New("Must specify password when passing user")
		}
	}

	timeout := url.Query().Get("timeout")
	if timeout != "" {
		timeoutInt, err := strconv.Atoi(timeout)
		if err != nil {
			return url, errors.New("Invalid format for timeout: %s.  Must be integer", timeout)
		}

		c.timeout = time.Duration(timeoutInt) * time.Second
	}

	useTLS := url.Query().Get("tls")
	c.useTLS = strings.HasPrefix(strings.ToLower(useTLS), "t") || useTLS == "1"

	if c.useTLS {
		c.certFile = url.Query().Get("tls_cert_file")
		c.keyFile = url.Query().Get("tls_key_file")
		c.caCertFile = url.Query().Get("tls_ca_cert_file")
		noVerify := url.Query().Get("tls_no_verify")
		c.tlsNoVerify = strings.HasPrefix(strings.ToLower(noVerify), "t") || noVerify == "1"
	}

	log.Trace("Bolt Host: ", url.Host)
	log.Trace("Timeout: ", c.timeout)
	log.Trace("User: ", user)
	log.Trace("Password: ", password)
	log.Trace("TLS: ", c.useTLS)
	log.Trace("TLS No Verify: ", c.tlsNoVerify)
	log.Trace("Cert File: ", c.certFile)
	log.Trace("Key File: ", c.keyFile)
	log.Trace("CA Cert File: ", c.caCertFile)

	return url, nil
}

func (c *BoltConn) createConn() (net.Conn, error) {

	var err error
	c.url, err = c.parseURL()
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred parsing the conn URL")
	}

	var conn net.Conn
	if c.useTLS {
		config, err := c.tlsConfig()
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred setting up TLS configuration")
		}
		conn, err = tls.Dial("tcp", c.url.Host, config)
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred dialing to neo4j")
		}
	} else {
		conn, err = net.DialTimeout("tcp", c.url.Host, c.timeout)
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred dialing to neo4j")
		}
	}

	return conn, nil
}

func (c *BoltConn) tlsConfig() (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: tls.VersionTLS10,
		MaxVersion: tls.VersionTLS12,
	}

	if c.caCertFile != "" {
		// Load CA cert - usually for self-signed certificates
		caCert, err := ioutil.ReadFile(c.caCertFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		config.RootCAs = caCertPool
	}

	if c.certFile != "" {
		if c.keyFile == "" {
			return nil, errors.New("If you're providing a cert file, you must also provide a key file")
		}

		cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
		if err != nil {
			return nil, err
		}

		config.Certificates = []tls.Certificate{cert}
	}

	if c.tlsNoVerify {
		config.InsecureSkipVerify = true
	}

	return config, nil
}

func (c *BoltConn) handShake() error {

	numWritten, err := c.Write(handShake)
	if numWritten != 20 {
		log.Errorf("Couldn't write expected bytes for magic preamble + supported versions. Written: %d. Expected: 4", numWritten)
		if err != nil {
			err = errors.Wrap(err, "An error occurred writing magic preamble + supported versions")
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

func (c *BoltConn) initialize() error {
	var err error

	c.conn, err = c.createConn()
	if err != nil {
		// Return the connection back into the pool
		if e := c.Close(); e != nil {
			log.Errorf("An error occurred closing connection: %s", e)
		}
		return err
	}

	if err := c.handShake(); err != nil {
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
	default:
		log.Errorf("Got an unrecognized message when initializing connection :%+v", resp)
		c.connErr = errors.New("Unrecognized response from the server: %#v", resp)
		c.Close()
		return driver.ErrBadConn
	}

	return nil
}

// Read reads the data from the underlying connection
func (c *BoltConn) Read(b []byte) (n int, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting read deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Read(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Read %d bytes from stream:\n\n%s\n", n, sprintByteHex(b))
	}

	if err != nil && err != io.EOF {
		c.connErr = errors.Wrap(err, "An error occurred reading from stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Write writes the data to the underlying connection
func (c *BoltConn) Write(b []byte) (n int, err error) {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		c.connErr = errors.Wrap(err, "An error occurred setting write deadline")
		return 0, driver.ErrBadConn
	}

	n, err = c.conn.Write(b)

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Wrote %d of %d bytes to stream:\n\n%s\n", len(b), n, sprintByteHex(b[:n]))
	}

	if err != nil {
		c.connErr = errors.Wrap(err, "An error occurred writing to stream")
		err = driver.ErrBadConn
	}
	return n, err
}

// Close closes the connection
// Driver may allow for pooling in the future, keeping connections alive
func (c *BoltConn) Close() error {

	if c.closed {
		return nil
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

	if c.conn == nil {
		return errors.New("can not close nil connection")
	}

	err := c.conn.Close()
	c.closed = true
	if err != nil {
		c.connErr = errors.Wrap(err, "An error occurred closing the connection")
		return driver.ErrBadConn
	}

	return nil
}

func (c *BoltConn) ackFailure(failure messages.FailureMessage) error {
	log.Infof("Acknowledging Failure: %#v", failure)

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
			c.connErr = errors.New("Got unrecognized response from acking failure: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

func (c *BoltConn) reset() error {
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
			return errors.Wrap(resp, "Error resetting session. CLOSING SESSION!")
		default:
			log.Errorf("Got unrecognized response from resetting session: %#v", resp)
			c.connErr = errors.New("Got unrecognized response from resetting session: %#v. CLOSING SESSION!", resp)
			c.Close()
			return driver.ErrBadConn
		}
	}
}

// Prepare prepares a new statement for a query
func (c *BoltConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Prepare prepares a new statement for a query. Implements a Neo-friendly alternative to sql/driver.
func (c *BoltConn) PrepareNeo(query string) (Stmt, error) {
	return c.prepare(query)
}

// PreparePipeline prepares a new pipeline statement for a query.
func (c *BoltConn) PreparePipeline(queries ...string) (PipelineStmt, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}
	c.statement = newPipelineStmt(queries, c)
	return c.statement, nil
}

func (c *BoltConn) prepare(query string) (*boltStmt, error) {
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
func (c *BoltConn) Begin() (driver.Tx, error) {
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
func (c *BoltConn) SetChunkSize(chunkSize uint16) {
	c.chunkSize = chunkSize
}

// Sets the timeout for reading and writing to the stream
func (c *BoltConn) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

func (c *BoltConn) consume() (interface{}, error) {
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
		return failure, errors.Wrap(failure, "Neo4J reported a failure for the query")
	}

	return respInt, err
}

func (c *BoltConn) consumeAll() ([]interface{}, interface{}, error) {
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

func (c *BoltConn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
	log.Infof("Consuming all responses %d times until success/failure", mult)

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

func (c *BoltConn) sendInit() (interface{}, error) {
	log.Infof("Sending INIT Message. ClientID: %s User: %s", ClientID, c.user)

	initMessage := messages.NewInitMessage(ClientID, c.user, c.password)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(initMessage); err != nil {
		return nil, errors.Wrap(err, "An error occurred sending init message")
	}

	return c.consume()
}

func (c *BoltConn) sendRun(query string, args map[string]interface{}) error {
	log.Infof("Sending RUN message: query %s (args: %#v)", query, args)
	runMessage := messages.NewRunMessage(query, args)
	if err := encoding.NewEncoder(c, c.chunkSize).Encode(runMessage); err != nil {
		return errors.Wrap(err, "An error occurred running query")
	}

	return nil
}

func (c *BoltConn) sendRunConsume(query string, args map[string]interface{}) (interface{}, error) {
	if err := c.sendRun(query, args); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *BoltConn) sendPullAll() error {
	log.Infof("Sending PULL_ALL message")

	pullAllMessage := messages.NewPullAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(pullAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding pull all query")
	}

	return nil
}

func (c *BoltConn) sendPullAllConsume() (interface{}, error) {
	if err := c.sendPullAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *BoltConn) sendRunPullAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendPullAll()
}

func (c *BoltConn) sendRunPullAllConsumeRun(query string, args map[string]interface{}) (interface{}, error) {
	err := c.sendRunPullAll(query, args)
	if err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *BoltConn) sendRunPullAllConsumeSingle(query string, args map[string]interface{}) (interface{}, interface{}, error) {
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

func (c *BoltConn) sendRunPullAllConsumeAll(query string, args map[string]interface{}) (interface{}, interface{}, []interface{}, error) {
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

func (c *BoltConn) sendDiscardAll() error {
	log.Infof("Sending DISCARD_ALL message")

	discardAllMessage := messages.NewDiscardAllMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(discardAllMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding discard all query")
	}

	return nil
}

func (c *BoltConn) sendDiscardAllConsume() (interface{}, error) {
	if err := c.sendDiscardAll(); err != nil {
		return nil, err
	}

	return c.consume()
}

func (c *BoltConn) sendRunDiscardAll(query string, args map[string]interface{}) error {
	err := c.sendRun(query, args)
	if err != nil {
		return err
	}

	return c.sendDiscardAll()
}

func (c *BoltConn) sendRunDiscardAllConsume(query string, args map[string]interface{}) (interface{}, interface{}, error) {
	runResp, err := c.sendRunConsume(query, args)
	if err != nil {
		return runResp, nil, err
	}

	discardResp, err := c.sendDiscardAllConsume()
	return runResp, discardResp, err
}

func (c *BoltConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.queryNeo(query, params)
}

func (c *BoltConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
	return c.queryNeo(query, params)
}

func (c *BoltConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rows, err := c.queryNeo(query, params)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	data, metadata, err := rows.All()
	return data, rows.metadata, metadata, err
}

func (c *BoltConn) queryNeo(query string, params map[string]interface{}) (*boltRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	c.statement = nil

	c.statement = newStmt(query, c)

	// Pipeline the run + pull all for this
	successResp, err := c.sendRunPullAllConsumeRun(c.statement.query, params)
	if err != nil {
		if wErr := c.statement.Close(); wErr != nil {
			return nil, fmt.Errorf("statement close failed, %v, %w", wErr, err)
		}
		return nil, err
	}
	success, ok := successResp.(messages.SuccessMessage)
	if !ok {
		if wErr := c.statement.Close(); wErr != nil {
			return nil, fmt.Errorf("unexpected response querying neo from connection: %#v, %w", successResp, wErr)
		}
		return nil, errors.New("Unexpected response querying neo from connection: %#v", successResp)
	}

	c.statement.rows = newQueryRows(c.statement, success.Metadata)

	return c.statement.rows, nil
}

func (c *BoltConn) QueryPipeline(queries []string, params ...map[string]interface{}) (PipelineRows, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
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
func (c *BoltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.Exec(args)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (c *BoltConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newStmt(query, c)
	defer stmt.Close()

	return stmt.ExecNeo(params)
}

func (c *BoltConn) ExecPipeline(queries []string, params ...map[string]interface{}) ([]Result, error) {
	if c.statement != nil {
		return nil, errors.New("An open statement already exists")
	}
	if c.closed {
		return nil, errors.New("Connection already closed")
	}

	stmt := newPipelineStmt(queries, c)
	defer stmt.Close()

	return stmt.ExecPipeline(params...)
}
