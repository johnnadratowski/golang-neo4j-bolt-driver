package golangNeo4jBoltDriver

import (
	"fmt"
	"os"
	"testing"

	"time"

	"sync"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/log"
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

	output := m.Run()

	if neo4jConnStr != "" {
		// If we're using a DB for testing neo, clear it out after all the test runs
		clearNeo()
	}

	os.Exit(output)
}

func clearNeo() {
	conn, err := OpenNeo(neo4jConnStr)
	if err != nil {
		panic(fmt.Sprintf("error getting conn to clear DB: %s\n", err))
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

		log.Info("1")
		one <- true
		<-two

		if len(data) != 0 {
			t.Fatalf("Expected no data: %#v", data)
		}

		data, _, _, err = conn.QueryNeoAll(`MATCH (n) RETURN n`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		log.Infof("data: %#v", data)
		if len(data) != 1 {
			t.Fatalf("Expected no data: %#v", data)
		}

		log.Info("3")
		three <- true
		<-four

		data, _, _, err = conn.QueryNeoAll(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		if len(data) != 1 {
			t.Fatalf("Expected no data: %#v", data)
		}

		log.Info("5")
		five <- true
		<-six

		data, _, _, err = conn.QueryNeoAll(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		if len(data) != 0 {
			t.Fatalf("Expected no data: %#v", data)
		}

		log.Info("7")
		seven <- true
	}()

	go func() {
		<-one
		defer wg.Done()

		conn, err := driver.OpenPool()
		if err != nil {
			t.Fatalf("An error occurred opening conn: %s", err)
		}
		defer conn.Close()

		_, err = conn.ExecNeo(`CREATE (f:FOO)`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("2")
		two <- true
		<-three

		_, err = conn.ExecNeo(`MATCH (f:FOO) CREATE UNIQUE (f)-[b:BAR]->(c:BAZ)`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("4")
		four <- true
		<-five

		_, err = conn.ExecNeo(`MATCH (:FOO)-[b:BAR]->(:BAZ) DELETE b`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		_, err = conn.ExecNeo(`MATCH (n) DETACH DELETE n`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("6")
		six <- true
		<-seven

	}()

	wg.Wait()
}
