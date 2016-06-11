package golangNeo4jBoltDriver

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
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
	if b.chunkSize == 0 {
		// TODO: Test best default
		// Default to 2048 byte chunks
		b.chunkSize = 2048
	}

	if b.timeoutSeconds == 0 {
		// TODO: Test best default
		// Default to 10 second timeout
		b.timeoutSeconds = 10
	}

	conn, serverVersion, err := b.initializeConn(connStr)
	if err != nil {
		return nil, err
	}

	c := newBoltConn(connStr, conn, serverVersion, b.timeoutSeconds, b.chunkSize)
	return c, nil
}

func (b *boltDriver) initializeConn(connStr string) (net.Conn, []byte, error) {
	conn, err := net.Dial("tcp", connStr)
	if err != nil {
		Logger.Println("An error occurred connecting:", err)
		return nil, nil, err
	}

	conn.Write(magicPreamble)
	conn.Write(supportedVersions)

	var serverVersion []byte
	_, err = conn.Read(serverVersion)
	if err != nil {
		Logger.Println("An error occurred reading server version:", err)
		return nil, nil, err
	}

	if bytes.Compare(serverVersion, noVersionSupported) == 0 {
		Logger.Println("No version supported from server")
		return nil, nil, fmt.Errorf("NO VERSION SUPPORTED")
	}

	if err = encoding.NewEncoder(conn, b.chunkSize).Encode(messages.NewInitMessage(ClientID, b.credentials)); err != nil {
		return nil, nil, err
	}

	return conn, serverVersion, nil
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
