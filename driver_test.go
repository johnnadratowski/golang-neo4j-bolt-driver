package golangNeo4jBoltDriver

import (
	"os"
	"testing"

	"time"

	"sync"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
)

var (
	neo4jConnStr = ""
)

func TestMain(m *testing.M) {
	log.SetLevel(os.Getenv("BOLT_DRIVER_LOG"))

	neo4jConnStr = os.Getenv("NEO4J_BOLT")
	if neo4jConnStr != "" {
		log.Info("Using NEO4J for tests:", neo4jConnStr)
	} else if os.Getenv("ENSURE_NEO4J_BOLT") != "" {
		log.Fatal("Must give NEO4J_BOLT environment variable")
	}

	if neo4jConnStr != "" {
		// If we're using a DB for testing neo, clear it out after all the test runs
		clearNeo()
	}

	output := m.Run()

	if neo4jConnStr != "" {
		// If we're using a DB for testing neo, clear it out after all the test runs
		clearNeo()
	}

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

func TestBoltDriverPool_OpenNeo(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	pool, err := NewDriverPool(neo4jConnStr, 25)
	if err != nil {
		t.Fatalf("An error occurred opening driver pool: %#v", err)
	}

	now := time.Now().Unix()
	for i := 0; i < 25; i++ {
		go func() {
			c, err := pool.OpenPool()
			if err != nil {
				t.Fatalf("An error occurred opening conn from pool: %#v", err)
			}
			defer c.Close()
			time.Sleep(time.Millisecond * time.Duration(200))
		}()
	}

	c, err := pool.OpenPool()
	if !(time.Now().Unix()-now < 200) {
		t.Fatalf("An error occurred opening conn from pool at end: %#v", err)
	}
	defer c.Close()
}

func TestBoltDriverPool_Concurrent(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	driver, err := NewDriverPool(neo4jConnStr, 2)
	if err != nil {
		t.Fatalf("An error occurred opening driver pool: %#v", err)
	}

	one := make(chan bool)
	two := make(chan bool)
	three := make(chan bool)
	four := make(chan bool)
	five := make(chan bool)
	six := make(chan bool)
	seven := make(chan bool)
	go func() {
		defer wg.Done()

		conn, err := driver.OpenPool()
		if err != nil {
			t.Fatalf("An error occurred opening conn: %s", err)
		}
		defer conn.Close()

		data, _, _, err := conn.QueryNeoAll(`MATCH (n) RETURN n`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		log.Info("FIRST: WRITE OUT 1")
		one <- true
		log.Info("FIRST: FINISHED WRITE OUT 1")
		log.Info("FIRST: WAIT ON 2")
		<-two
		log.Info("FIRST: FINISHED WAIT ON 2")

		if len(data) != 0 {
			log.Panicf("Expected no data: %#v", data)
		}

		data, _, _, err = conn.QueryNeoAll(`MATCH (n) RETURN n`, nil)
		if err != nil {
			log.Panicf("An error occurred querying neo: %s", err)
		}

		log.Infof("data: %#v", data)
		if len(data) != 1 {
			log.Panicf("Expected no data: %#v", data)
		}

		log.Info("FIRST: WRITE OUT 3")
		three <- true
		log.Info("FIRST: FINISHED WRITE OUT 3")
		log.Info("FIRST: WAIT ON 4")
		<-four
		log.Info("FIRST: FINISHED WAIT ON 4")

		data, _, _, err = conn.QueryNeoAll(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			log.Panicf("An error occurred querying neo: %s", err)
		}

		if len(data) != 1 {
			log.Panicf("Expected no data: %#v", data)
		}

		log.Info("FIRST: WRITE OUT 5")
		five <- true
		log.Info("FIRST: FINISHED WRITE OUT 5")
		log.Info("FIRST: WAIT ON 6")
		<-six
		log.Info("FIRST: FINISHED WAIT ON 6")

		data, _, _, err = conn.QueryNeoAll(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			log.Panicf("An error occurred querying neo: %s", err)
		}

		if len(data) != 0 {
			log.Panicf("Expected no data: %#v", data)
		}

		log.Info("FIRST: WRITE OUT 7")
		seven <- true
		log.Info("FIRST: FINISHED WRITE OUT 7")
	}()

	go func() {
		log.Info("SECOND: WAIT ON 1")
		<-one
		log.Info("SECOND: FINISHED WAIT ON 1")
		defer wg.Done()

		conn, err := driver.OpenPool()
		if err != nil {
			log.Panicf("An error occurred opening conn: %s", err)
		}
		defer conn.Close()

		_, err = conn.ExecNeo(`CREATE (f:FOO)`, nil)
		if err != nil {
			log.Panicf("An error occurred creating f neo: %s", err)
		}

		log.Info("SECOND: WRITE OUT 2")
		two <- true
		log.Info("SECOND: FINISHED WRITE OUT 2")
		log.Info("SECOND: WAIT ON 3")
		<-three
		log.Info("SECOND: FINISHED WAIT ON 3")

		_, err = conn.ExecNeo(`MATCH (f:FOO) CREATE UNIQUE (f)-[b:BAR]->(c:BAZ)`, nil)
		if err != nil {
			log.Panicf("An error occurred creating f neo: %s", err)
		}

		log.Info("SECOND: WRITE OUT 4")
		four <- true
		log.Info("SECOND: FINISHED WRITE OUT 4")
		log.Info("SECOND: WAIT ON 5")
		<-five
		log.Info("SECOND: FINISHED WAIT ON 5")

		_, err = conn.ExecNeo(`MATCH (:FOO)-[b:BAR]->(:BAZ) DELETE b`, nil)
		if err != nil {
			log.Panicf("An error occurred creating f neo: %s", err)
		}

		_, err = conn.ExecNeo(`MATCH (n) DETACH DELETE n`, nil)
		if err != nil {
			log.Panicf("An error occurred creating f neo: %s", err)
		}

		log.Info("SECOND: WRITE OUT 6")
		six <- true
		log.Info("SECOND: FINISHED WRITE OUT 6")
		log.Info("SECOND: WAIT ON 7")
		<-seven
		log.Info("SECOND: FINISHED WAIT ON 7")

	}()

	wg.Wait()
}

func TestBoltDriverPool_ReclaimBadConn(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	driver, err := NewDriverPool(neo4jConnStr, 1)
	if err != nil {
		t.Fatalf("An error occurred opening driver pool: %#v", err)
	}

	conn, err := driver.OpenPool()
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	_, err = conn.ExecNeo(`CREATE (f:FOO)`, nil)
	if err != nil {
		t.Fatalf("An error occurred creating f neo: %s", err)
	}

	err = conn.(*boltConn).conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing underlying connection: %s", err)
	}

	_, err = conn.ExecNeo(`CREATE (f:FOO)`, nil)
	if err == nil {
		t.Fatal("An error should have occurred when trying to make a call with a closed connection")
	} else if conn.(*boltConn).connErr == nil {
		t.Fatal("A connection error should have been associated to the conn after a bad connection")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Got an error closing a bad connection: %s", err)
	}

	conn, err = driver.OpenPool()
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	_, err = conn.ExecNeo(`CREATE (f:FOO)`, nil)
	if err != nil {
		t.Fatalf("A new conn should have been established for an old conn that had an error. However, we got an error: %s", err)
	}
}
