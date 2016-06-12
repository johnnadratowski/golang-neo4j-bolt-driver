package golangNeo4jBoltDriver

import (
	"log"
	"os"
	"testing"
)

var (
	neo4jConnStr = ""
)

func TestMain(m *testing.M) {
	neo4jConnStr = os.Getenv("NEO4J_BOLT")
	if neo4jConnStr != "" {
		log.Println("Using NEO4J for tests:", neo4jConnStr)
	} else {
		log.Fatal("Must give NEO4J_BOLT environment variable")
	}

	m.Run()
}

func TestBoltDriver_Open(t *testing.T) {
	// TODO: implement
}
