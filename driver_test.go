package bolt

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"sync"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/log"
)

var (
	neo4jConnStr = ""
)

func expect(t *testing.T, r *Rows, n int) {
	if r == nil {
		if n != 0 {
			t.Fatalf("wanted %d rows, got nil r", n)

		}
		return
	}
	defer r.Close()
	var i int
	for r.Next() {
		i++
	}
	if err := r.Err(); err != nil && (err != sql.ErrNoRows && n != 0) {
		t.Fatal(err)
	}
	if i != n {
		t.Fatalf("expected %d rows, got %d", n, i)
	}
}

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
	db, err := OpenNeo(neo4jConnStr)
	if err != nil {
		panic(fmt.Sprintf("error getting conn to clear DB: %s\n", err))
	}

	stmt, err := db.Prepare(`MATCH (n) DETACH DELETE n`)
	if err != nil {
		panic("Error getting stmt to clear DB")
	}
	defer stmt.Close()

	_, err = stmt.Exec(nil)
	if err != nil {
		panic("Error running query to clear DB")
	}
}

func TestBoltDriverPool_OpenNeo(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	pool, err := OpenPool(boltName, neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening driver pool: %#v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoltDriverPool_Concurrent(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	db, err := OpenPool(boltName, neo4jConnStr)
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

		rows, err := db.Query(`MATCH (n) RETURN n`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		log.Info("1")
		one <- true
		<-two

		expect(t, rows, 0)

		rows, err = db.Query(`MATCH (n) RETURN n`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		expect(t, rows, 1)

		log.Info("3")
		three <- true
		<-four

		rows, err = db.Query(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		expect(t, rows, 1)

		log.Info("5")
		five <- true
		<-six

		rows, err = db.Query(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			t.Fatalf("An error occurred querying neo: %s", err)
		}

		expect(t, rows, 9)

		log.Info("7")
		seven <- true
	}()

	go func() {
		<-one
		defer wg.Done()

		_, err = db.Exec(`CREATE (f:FOO)`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("2")
		two <- true
		<-three

		_, err = db.Exec(`MATCH (f:FOO) CREATE UNIQUE (f)-[b:BAR]->(c:BAZ)`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("4")
		four <- true
		<-five

		_, err = db.Exec(`MATCH (:FOO)-[b:BAR]->(:BAZ) DELETE b`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		_, err = db.Exec(`MATCH (n) DETACH DELETE n`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		log.Info("6")
		six <- true
		<-seven

	}()

	wg.Wait()
}
