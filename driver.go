package golangNeo4jBoltDriver

import (
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"log"
)

var (
	// Logger is the default logger to use for the driver. Defaults to discarding logs.
	Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	// TraceLogger is the low-level debugging logger to use for the driver. Defaults to discarding logs.
	TraceLogger       = log.New(ioutil.Discard, "", log.LstdFlags)
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
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
	Open(string) (driver.Conn, error)
	OpenNeo(string) (Conn, error)
}

type boltDriver struct{}

// NewDriver creates a new Driver object
func NewDriver() Driver {
	return &boltDriver{}
}

// Open opens a new Bolt connection to the Neo4J database
func (b *boltDriver) Open(connStr string) (driver.Conn, error) {
	return newBoltConn(connStr)
}

// Open opens a new Bolt connection to the Neo4J database. Implements a Neo-friendly alternative to sql/driver.
func (b *boltDriver) OpenNeo(connStr string) (Conn, error) {
	return newBoltConn(connStr)
}
func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
