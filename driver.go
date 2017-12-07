package golangNeo4jBoltDriver

import (
	"database/sql"
	"database/sql/driver"
	"sync"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
)

var (
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	handShake          = append(magicPreamble, supportedVersions...)
	noVersionSupported = []byte{0x00, 0x00, 0x00, 0x00}
	// Version is the current version of this driver
	Version = "1.0"
	// ClientID is the id of this client
	ClientID = "GolangNeo4jBolt/" + Version
)

// Driver is a driver allowing connection to Neo4j
// The driver allows you to open a new connection to Neo4j
//
// Implements sql/driver, but also includes its own more neo-friendly interface.
// Some of the features of this interface implement neo-specific features
// unavailable in the sql/driver compatible interface
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type Driver interface {
	// Open opens a sql.driver compatible connection. Used internally
	// by the go sql interface
	Open(string) (driver.Conn, error)
	// OpenNeo opens a Neo-specific connection. This should be used
	// directly when not using the golang sql interface
	OpenNeo(string) (Conn, error)
}

type boltDriver struct {
	recorder *recorder
}

// NewDriver creates a new Driver object
func NewDriver() Driver {
	return &boltDriver{}
}

// Open opens a new Bolt connection to the Neo4J database
func (d *boltDriver) Open(connStr string) (driver.Conn, error) {
	return newBoltConn(connStr, d) // Never use pooling when using SQL driver
}

// Open opens a new Bolt connection to the Neo4J database. Implements a Neo-friendly alternative to sql/driver.
func (d *boltDriver) OpenNeo(connStr string) (Conn, error) {
	return newBoltConn(connStr, d)
}

// DriverPool is a driver allowing connection to Neo4j with support for connection pooling
// The driver allows you to open a new connection to Neo4j
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type DriverPool interface {
	// OpenPool opens a Neo-specific connection.
	OpenPool() (Conn, error)
	reclaim(*boltConn) error
}

// ClosableDriverPool like the DriverPool but with a closable function
type ClosableDriverPool interface {
	DriverPool
	Close() error
}

type boltDriverPool struct {
	connStr  string
	maxConns int
	pool     chan *boltConn
	connRefs []*boltConn
	refLock  sync.Mutex
	closed   bool
}

// NewDriverPool creates a new Driver object with connection pooling
func NewDriverPool(connStr string, max int) (DriverPool, error) {
	return createDriverPool(connStr, max)
}

// NewClosableDriverPool create a closable driver pool
func NewClosableDriverPool(connStr string, max int) (ClosableDriverPool, error) {
	return createDriverPool(connStr, max)
}

func createDriverPool(connStr string, max int) (*boltDriverPool, error) {
	d := &boltDriverPool{
		connStr:  connStr,
		maxConns: max,
		pool:     make(chan *boltConn, max),
	}

	for i := 0; i < max; i++ {
		conn, err := newPooledBoltConn(connStr, d)
		if err != nil {
			return nil, err
		}

		d.pool <- conn
	}

	return d, nil
}

// OpenNeo opens a new Bolt connection to the Neo4J database.
func (d *boltDriverPool) OpenPool() (Conn, error) {
	// For each connection request we need to block in case the Close function is called. This gives us a guarantee
	// when closing the pool no new connections are made.
	d.refLock.Lock()
	defer d.refLock.Unlock()
	if !d.closed {
		conn := <-d.pool
		if conn.conn == nil {
			if err := conn.initialize(); err != nil {
				// Return the connection back into the pool
				d.pool <- conn
				return nil, err
			}
			d.connRefs = append(d.connRefs, conn)
		}
		return conn, nil
	} else {
		return nil, errors.New("Driver pool has been closed")
	}
}

// Close all connections in the pool
func (d *boltDriverPool) Close() error {
	// Lock the connection ref so no new connections can be added
	d.refLock.Lock()
	defer d.refLock.Unlock()
	for _, conn := range d.connRefs {
		// Remove the reference to the pool, to allow a clean up of the connection
		conn.poolDriver = nil
		err := conn.Close()
		if err != nil {
			d.closed = true
			return err
		}
	}
	// Mark the pool as closed to stop any new connections
	d.closed = true
	return nil
}

func (d *boltDriverPool) reclaim(conn *boltConn) error {
	var newConn *boltConn
	var err error
	if conn.connErr != nil || conn.closed {
		newConn, err = newPooledBoltConn(d.connStr, d)
		if err != nil {
			return err
		}
	} else {
		// sneakily swap out connection so a reference to
		// it isn't held on to
		newConn = &boltConn{}
		*newConn = *conn
	}

	d.pool <- newConn
	conn = nil

	return nil
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
