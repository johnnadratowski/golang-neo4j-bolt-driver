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

type boltDriver struct {
}

// NewDriver creates a new Driver object
func NewDriver() Driver {
	return &boltDriver{}
}

// Open opens a new Bolt connection to the Neo4J database
func (b *boltDriver) Open(connStr string) (driver.Conn, error) {
	var err error
	c := &boltConn{connStr: connStr, serverVersion: make([]byte, 4)}
	c.conn, err = net.Dial("tcp", c.connStr)
	if err != nil {
		Logger.Println("An error occurred connecting:", err)
		return nil, err
	}

	c.conn.Write(magicPreamble)
	c.conn.Write(supportedVersions)

	_, err = c.conn.Read(c.serverVersion)
	if err != nil {
		Logger.Println("An error occurred reading server version:", err)
		return nil, err
	}

	if bytes.Compare(c.serverVersion, noVersionSupported) == 0 {
		Logger.Println("No version supported from server")
		return nil, fmt.Errorf("NO VERSION SUPPORTED")
	}

	return c, nil
}

func init() {
	sql.Register("neo4j-cypher", &boltDriver{})
}
