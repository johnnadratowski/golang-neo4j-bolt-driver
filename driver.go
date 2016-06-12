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
	connLogger        = log.New(ioutil.Discard, "", log.LstdFlags)
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
	SetCredentials(string)
	SetTimeout(int)
	SetChunkSize(uint16)
}

type boltDriver struct {
	credentials    string
	timeoutSeconds int
	chunkSize      uint16
}

// NewDriver creates a new Driver object
func NewDriver(credentials string, timeoutSeconds int, chunkSize uint16) Driver {
	return &boltDriver{
		timeoutSeconds: timeoutSeconds,
		chunkSize:      chunkSize,
	}
}

// Open opens a new Bolt connection to the Neo4J database
func (b *boltDriver) Open(connStr string) (driver.Conn, error) {
	return newBoltConn(connStr)
}

// Set the credentials for the conns made by this driver
func (b *boltDriver) SetCredentials(credentials string) {
	b.credentials = credentials
}

// Set the timeout for connections made using this driver
func (b *boltDriver) SetTimeout(timeoutSeconds int) {
	b.timeoutSeconds = timeoutSeconds
}

// Set the chunk size for requests made by this driver's conns
func (b *boltDriver) SetChunkSize(chunkSize uint16) {
	b.chunkSize = chunkSize
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
}
