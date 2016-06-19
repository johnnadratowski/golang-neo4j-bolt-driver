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

	output := m.Run()

	clearNeo()

	os.Exit(output)
}

func clearNeo() {
	driver := NewDriver()
	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		panic("Error getting conn to clear DB")
	}

	stmt, err := conn.PrepareNeo(`MATCH (n) DETACH DELETE n`)
	if err != nil {
		panic("Error getting stmt to clear DB")
	}
	defer stmt.Close()

	_, err = stmt.ExecNeo(nil)
	if err != nil {
		panic("Error running query to clear DB")
	}
}
