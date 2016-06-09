package golangNeo4jBoltDriver

import (
	"bytes"
	"log"
	"net"
	"os"
	"testing"
)

var (
	driver        Driver
	neo4jConnStr  = ""
	usingMock     = false
	serverPort    = "57354"
	clientConnStr = "localhost:" + serverPort
	listener      net.Listener
	serverFunc    func(net.Conn)

	mockServerVersion = []byte{0x00, 0x00, 0x00, 0x01}
)

func TestMain(m *testing.M) {
	neo4jConnStr = os.Getenv("NEO4J_BOLT")
	if neo4jConnStr == "" {
		log.Println("Using local server mocks for tests")
		usingMock = true
		var err error
		listener, err = net.Listen("tcp", clientConnStr)
		if err != nil {
			log.Fatal("Error starting mock server", err)
		}
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Fatal("An error occurred accepting connection on test server", err)
				}

				if serverFunc == nil {
					log.Fatal("No handler for server func")
				} else {
					serverFunc(conn)
				}

			}
		}()

	} else {
		log.Println("Using NEO4J for tests:", neo4jConnStr)
		clientConnStr = neo4jConnStr
		usingMock = false
	}

	driver = NewDriver()
	m.Run()
}

func TestBoltDriver_Open(t *testing.T) {
	if usingMock {
		serverFunc = func(c net.Conn) {
			c.Write(mockServerVersion)
		}
	}

	c, err := driver.Open(clientConnStr)
	if err != nil {
		t.Fatal("An error occurred opening conn", err)
	}

	if bytes.Compare(c.(*boltConn).serverVersion, mockServerVersion) != 0 {
		t.Fatal("Invalid server version", c.(*boltConn).serverVersion)
	}

	c.Close()
	if usingMock {
		serverFunc = nil
	}
}
