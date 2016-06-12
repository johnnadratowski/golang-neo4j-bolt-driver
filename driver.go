package golangNeo4jBoltDriver

import (
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"log"
	"os"
)

var (
	// Logger is the logger to use for the driver. Defaults to discarding logs.
	Logger            = log.New(ioutil.Discard, "", log.LstdFlags)
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

func init() {
	if os.Getenv("BOLT_DRIVER_LOG") != "" {
		Logger = log.New(os.Stderr, "[NEO4J BOLT DRIVER] ", log.LstdFlags)
	}
}

// Driver is a driver allowing connection to Neo4j
type Driver interface {
	Open(string) (driver.Conn, error)
}

type boltDriver struct {}

// NewDriver creates a new Driver object
func NewDriver() Driver {
	return &boltDriver{}
}

// Open opens a new Bolt connection to the Neo4J database
func (b *boltDriver) Open(connStr string) (driver.Conn, error) {
	return newBoltConn(connStr, "")
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
