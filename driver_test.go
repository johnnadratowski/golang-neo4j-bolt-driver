package golangNeo4jBoltDriver

import (
	"os"
	"testing"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
)

var (
	neo4jConnStr = ""
)

func TestMain(m *testing.M) {
	neo4jConnStr = os.Getenv("NEO4J_BOLT")
	if neo4jConnStr != "" {
		log.Info("Using NEO4J for tests:", neo4jConnStr)
	} else {
		log.Fatal("Must give NEO4J_BOLT environment variable")
	}

	log.SetLevel(os.Getenv("BOLT_DRIVER_LOG"))

	os.Exit(m.Run())
}
