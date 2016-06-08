package golang_neo4j_bolt_driver

import (
	"bytes"
	"io/ioutil"
	"log"
	"net"
	"os"
	"fmt"
)

var (
	Logger            = log.New(ioutil.Discard, "", log.LstdFlags)
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	noVersionSupported = []byte{0x00, 0x00, 0x00, 0x00}
	version = "1.0"
	clientID = "GolangNeo4jBolt/" + version
)

func init() {
	if os.Getenv("BOLT_DRIVER_LOG") != "" {
		Logger = log.New(os.Stderr, "[NEO4J BOLT DRIVER] ", log.LstdFlags)
	}
}

type Driver interface {
	Open(string) (Conn, error)
}

type boltDriver struct {
}

func NewDriver() Driver {
	return &boltDriver{}
}

func (b *boltDriver) Open(connStr string) (Conn, error) {
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
