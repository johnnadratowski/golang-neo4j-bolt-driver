package golangNeo4jBoltDriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"github.com/mindstand/golang-neo4j-bolt-driver/log"
	"strings"
	"sync"
	"time"
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
	return createBoltConn(connStr) // Never use pooling when using SQL driver
}

// Open opens a new Bolt connection to the Neo4J database. Implements a Neo-friendly alternative to sql/driver.
func (d *boltDriver) OpenNeo(connStr string) (Conn, error) {
	return createBoltConn(connStr)
}

type DriverMode int

const (
	ReadOnlyMode  DriverMode = 0
	ReadWriteMode DriverMode = 1
)

// DriverPool is a driver allowing connection to Neo4j with support for connection pooling
// The driver allows you to open a new connection to Neo4j
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads.  The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type DriverPool interface {
	// Open opens a Neo-specific connection.
	Open(mode DriverMode) (*BoltConn, error)
	Reclaim(*BoltConn) error
}

// ClosableDriverPool like the DriverPool but with a closable function
type ClosableDriverPool interface {
	DriverPool
	Close() error
}

type boltDriverPool struct {
	connStr  string
	maxConns int
	pool     *pool.ObjectPool
	refLock  sync.Mutex
	closed   bool
}

// NewDriverPool creates a new Driver object with connection pooling
func NewDriverPool(connStr string, max int) (DriverPool, error) {
	//check if its bolt or bolt+routing
	if strings.Contains(connStr, "bolt+routing") {
		return createRoutingDriverPool(connStr, max)
	}

	return createDriverPool(connStr, max)
}

// NewClosableDriverPool create a closable driver pool
func NewClosableDriverPool(connStr string, max int) (ClosableDriverPool, error) {
	//check if its bolt or bolt+routing
	if strings.Contains(connStr, "bolt+routing") {
		return createRoutingDriverPool(connStr, max)
	}

	return createDriverPool(connStr, max)
}

func createDriverPool(connStr string, max int) (*boltDriverPool, error) {
	dPool := pool.NewObjectPool(context.Background(), pool.NewPooledObjectFactorySimple(getPoolFunc([]string{connStr}, false)), &pool.ObjectPoolConfig{
		LIFO:                     true,
		MaxTotal:                 max,
		MaxIdle:                  max,
		MinIdle:                  0,
		TestOnCreate:             true,
		TestOnBorrow:             true,
		TestOnReturn:             true,
		TestWhileIdle:            true,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     pool.DefaultMinEvictableIdleTime,
		SoftMinEvictableIdleTime: pool.DefaultSoftMinEvictableIdleTime,
		NumTestsPerEvictionRun:   3,
		TimeBetweenEvictionRuns:  0,
	})

	return &boltDriverPool{
		connStr:  connStr,
		maxConns: max,
		pool:     dPool,
	}, nil
}

// Open opens a returns a Bolt connection from the pool to the Neo4J database.
func (d *boltDriverPool) Open(DriverMode) (*BoltConn, error) {
	// For each connection request we need to block in case the Close function is called. This gives us a guarantee
	// when closing the pool no new connections are made.
	d.refLock.Lock()
	defer d.refLock.Unlock()
	if !d.closed {
		connObj, err := d.pool.BorrowObject(context.Background())
		if err != nil {
			return nil, err
		}

		conn, ok := connObj.(*BoltConn)
		if !ok {
			return nil, errors.New("unable to cast to *BoltConn")
		}

		//check to make sure the connection is open
		if connectionNilOrClosed(conn) {
			//if it isn't, reset it
			err := conn.initialize()
			if err != nil {
				return nil, err
			}

			conn.closed = false
			conn.connErr = nil
			conn.statement = nil
			conn.transaction = nil
		}

		return conn, nil
	}
	return nil, errors.New("Driver pool has been closed")
}

func connectionNilOrClosed(conn *BoltConn) bool {
	if conn.conn == nil { //nil check before attempting read
		return true
	}
	err := conn.conn.SetReadDeadline(time.Now())
	if err != nil {
		log.Error("Bad Connection state detected", err) //the error caught here could be a io.EOF or a timeout, either way we want to log the error & return true
		return true
	}

	zero := make([]byte, 0)

	_, err = conn.conn.Read(zero) //read zero bytes to validate connection is still alive
	if err != nil {
		log.Error("Bad Connection state detected", err) //the error caught here could be a io.EOF or a timeout, either way we want to log the error & return true
		return true
	}

	//check if there were any connection errors
	if conn.connErr != nil {
		return true
	}

	return false
}

// Close all connections in the pool
func (d *boltDriverPool) Close() error {
	// Lock the connection ref so no new connections can be added
	d.refLock.Lock()
	defer d.refLock.Unlock()

	d.pool.Close(context.Background())

	d.closed = true

	return nil
}

func (d *boltDriverPool) Reclaim(conn *BoltConn) error {
	if conn == nil {
		return errors.New("cannot reclaim nil connection")
	}
	if connectionNilOrClosed(conn) {
		err := conn.initialize()
		if err != nil {
			return err
		}

		conn.closed = false
		conn.connErr = nil
		conn.statement = nil
		conn.transaction = nil
	} else {
		if conn.statement != nil {
			if !conn.statement.closed {
				if conn.statement.rows != nil && !conn.statement.rows.closed {
					err := conn.statement.rows.Close()
					if err != nil {
						return err
					}
				}
			}

			conn.statement = nil
		}

		if conn.transaction != nil {
			if !conn.transaction.closed {
				err := conn.transaction.Rollback()
				if err != nil {
					return err
				}
			}

			conn.transaction = nil
		}
	}
	return d.pool.ReturnObject(context.Background(), conn)
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
