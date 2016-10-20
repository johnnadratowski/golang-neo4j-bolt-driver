package golangNeo4jBoltDriver

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/log"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

// Conn represents a connection to Neo4J, implementing a Neo-friendly interface.
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

type boltConn struct {
	connStr       string
	url           *url.URL
	conn          net.Conn
	serverVersion [4]byte
	timeout       time.Duration
	chunkSize     uint16
	closed        bool
	useTLS        bool
	certFile      string
	caCertFile    string
	keyFile       string
	tlsNoVerify   bool
	transaction   *boltTx
	statement     *boltStmt
	poolDriver    DriverPool
}

func createBoltConn(connStr string) *boltConn {
	return &boltConn{
		connStr:   connStr,
		timeout:   time.Second * time.Duration(60),
		chunkSize: math.MaxUint16,
	}
}

// newBoltConn Creates a new bolt connection
func newBoltConn(connStr string, rec *recorder) (*boltConn, error) {
	c := createBoltConn(connStr)
	err := c.initialize(rec)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// newPooledBoltConn Creates a new bolt connection with a pooled driver
func newPooledBoltConn(connStr string, driver DriverPool) (*boltConn, error) {
	c := createBoltConn(connStr)
	c.poolDriver = driver
	return c, nil
}

func (c *boltConn) parseURL() (err error) {
	c.url, err = url.Parse(c.connStr)
	if err != nil {
		return err
	}

	if strings.ToLower(c.url.Scheme) != "bolt" {
		return errors.New("unsupported connection string scheme: %s. Driver only supports 'bolt' scheme.", c.url.Scheme)
	}

	if c.url.User != nil {
		_, ok := c.url.User.Password()
		if !ok {
			return errors.New("must specify password when passing user")
		}
	}

	timeout := c.url.Query().Get("timeout")
	if timeout != "" {
		timeoutInt, err := strconv.Atoi(timeout)
		if err != nil {
			return errors.New("invalid format for timeout: %s. Must be integer", timeout)
		}
		c.timeout = time.Duration(timeoutInt) * time.Second
	}

	useTLS := c.url.Query().Get("tls")
	c.useTLS = strings.HasPrefix(strings.ToLower(useTLS), "t") || useTLS == "1"

	if c.useTLS {
		c.certFile = c.url.Query().Get("tls_cert_file")
		c.keyFile = c.url.Query().Get("tls_key_file")
		c.caCertFile = c.url.Query().Get("tls_ca_cert_file")
		noVerify := c.url.Query().Get("tls_no_verify")
		c.tlsNoVerify = strings.HasPrefix(strings.ToLower(noVerify), "t") || noVerify == "1"
	}

	log.Trace("Bolt Host: ", c.url.Host)
	log.Trace("Timeout: ", c.timeout)
	log.Trace("TLS: ", c.useTLS)
	log.Trace("TLS No Verify: ", c.tlsNoVerify)
	log.Trace("Cert File: ", c.certFile)
	log.Trace("Key File: ", c.keyFile)
	log.Trace("CA Cert File: ", c.caCertFile)

	return nil
}

func (c *boltConn) createConn() (net.Conn, error) {
	err := c.parseURL()
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

func (c *boltConn) tlsConfig() (*tls.Config, error) {
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
			return nil, errors.New("cert file requires a key file")
		}
		cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
	}

	config.InsecureSkipVerify = c.tlsNoVerify
	return config, nil
}

func (c *boltConn) handShake() error {
	_, err := c.Write(handShake)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(c, c.serverVersion[:])
	if err != nil {
		return err
	}
	if c.serverVersion == noVersionSupported {
		return errors.New("Server responded with no supported version")
	}
	return nil
}

func (c *boltConn) initialize(rec *recorder) (err error) {
	// Handle recorder. If there is no conn string, assume we're playing back a recording.
	// If there is a recorder and a conn string, assume we're recording the connection
	// Else, just create the conn normally.
	if c.connStr == "" && rec != nil {
		c.conn = rec
	} else if rec != nil {
		rec.Conn, err = c.createConn()
		if err != nil {
			return err
		}
		c.conn = rec
	} else {
		c.conn, err = c.createConn()
		if err != nil {
			return err
		}
	}

	if err = c.handShake(); err != nil {
		if e := c.Close(); e != nil {
			return err
		}
		return err
	}

	resp, err := c.sendInit()
	if err != nil {
		if e := c.Close(); e != nil {
			return err
		}
		return err
	}

	switch resp := resp.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully initiated Bolt connection: %+v", resp)
		return nil
	default:
		log.Errorf("Got an unrecognized message when initializing connection :%+v", resp)
		if e := c.Close(); e != nil {
			return err
		}
		return errors.New("Unrecognized response from the server: %#v", resp)
	}
}

// Read reads the data from the underlying connection
func (c *boltConn) Read(b []byte) (n int, err error) {
	err = c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Read(b)
}

// Write writes the data to the underlying connection
func (c *boltConn) Write(b []byte) (n int, err error) {
	err = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if err != nil {
		return 0, err
	}
	return c.conn.Write(b)
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

	if c.poolDriver != nil {
		// If using connection pooling, don't close connection, just reclaim it
		c.poolDriver.reclaim(c)
		return nil
	}

	err := c.conn.Close()
	c.closed = err == nil
	return err
}

func (c *boltConn) ackFailure(failure messages.FailureMessage) error {
	log.Infof("Acknowledging Failure: %#v", failure)

	ack := messages.NewAckFailureMessage()
	err := encoding.NewEncoder(c, c.chunkSize).Encode(ack)
	if err != nil {
		return errors.Wrap(err, "An error occurred encoding ack failure message")
	}

	for {
		resp, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding ack failure message response")
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
		resp, err := encoding.NewDecoder(c).Decode()
		if err != nil {
			return errors.Wrap(err, "An error occurred decoding reset message response")
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

	resp, err := encoding.NewDecoder(c).Decode()
	if err != nil {
		return resp, err
	}

	if log.GetLevel() >= log.TraceLevel {
		log.Tracef("Consumed Response: %#v", resp)
	}

	if failure, isFail := resp.(messages.FailureMessage); isFail {
		log.Errorf("Got failure message: %#v", failure)
		err := c.ackFailure(failure)
		if err != nil {
			return nil, err
		}
		return failure, errors.New("Got failure message: %#v", failure)
	}

	return resp, err
}

func (c *boltConn) consumeAll() ([]interface{}, interface{}, error) {
	log.Info("Consuming all responses until success/failure")

	responses := []interface{}{}
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

func (c *boltConn) consumeAllMultiple(mult int) ([][]interface{}, []interface{}, error) {
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

func (c *boltConn) sendInit() (interface{}, error) {
	var user, pass string
	if info := c.url.User; info != nil {
		user = info.Username()
		pass, _ = info.Password()
	}
	initMessage := messages.NewInitMessage(ClientID, user, pass)
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

func (c *boltConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rows, err := c.queryNeo(query, params)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	data, metadata, err := rows.All()
	return data, rows.metadata, metadata, err
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
func (c *boltConn) Exec(query string, args []driver.Value) (driver.Result, error) {
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
func (c *boltConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
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

func (c *boltConn) ExecPipeline(queries []string, params ...map[string]interface{}) ([]Result, error) {
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
