package bolt

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

var (
	neo4jConnStr = ""
)

func expect(t *testing.T, r *Rows, n int) {
	if r == nil {
		if n != 0 {
			panic(fmt.Sprintf("wanted %d rows, got nil r", n))
		}
		return
	}
	var err error
	defer func() {
		err2 := r.Close()
		if err2 != nil {
			if err != nil {
				panic(fmt.Sprintf("two errors (close and orig): %#v %#v", err2, err))
			}
			panic(err2)
		}
	}()
	var i int
	for r.Next() {
		i++
	}
	if err = r.Err(); err != nil && (err != sql.ErrNoRows && n != 0) {
		panic(err)
	}
	if i != n {
		panic(fmt.Sprintf("expected %d rows, got %d", n, i))
	}
}

func TestMain(m *testing.M) {
	neo4jConnStr = os.Getenv("NEO4J_BOLT")
	if neo4jConnStr != "" {
		if testing.Verbose() {
			fmt.Println("Using NEO4J for tests:", neo4jConnStr)
		}
	} else if os.Getenv("ENSURE_NEO4J_BOLT") != "" {
		fmt.Println("Must give NEO4J_BOLT environment variable")
		os.Exit(1)
	}

	if neo4jConnStr != "" {
		// If we're using a DB for testing neo, clear it out after all the test runs
		defer clearNeo()
	}

	output := m.Run()

	os.Exit(output)
}

func clearNeo() {
	db, err := OpenPool(boltName, neo4jConnStr)
	if err != nil {
		panic(fmt.Sprintf("error getting conn to clear DB: %s\n", err))
	}
	defer db.Close()

	_, err = db.Exec(`MATCH (n) DETACH DELETE n`, nil)
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

	db, err := OpenPool(boltName, neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening driver pool: %#v", err)
	}

	var s struct{}

	one := make(chan struct{})
	two := make(chan struct{})
	three := make(chan struct{})
	four := make(chan struct{})
	five := make(chan struct{})
	six := make(chan struct{})
	seven := make(chan struct{})
	ech := make(chan error)
	done := make(chan struct{})

	go func() {
		rows, err := db.Query(`MATCH (n) RETURN n`, nil)
		if err != nil {
			ech <- err
			return
		}
		err = rows.Close()
		if err != nil {
			ech <- err
			return
		}

		t.Log("1")
		one <- s
		<-two

		expect(t, rows, 0)

		rows, err = db.Query(`MATCH (n) RETURN n`, nil)
		if err != nil {
			ech <- err
			return
		}

		expect(t, rows, 1)

		t.Log("3")
		three <- s
		<-four

		rows, err = db.Query(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			ech <- err
			return
		}

		expect(t, rows, 1)

		t.Log("5")
		five <- s
		<-six

		rows, err = db.Query(`MATCH path=(:FOO)-[:BAR]->(:BAZ) RETURN path`, nil)
		if err != nil {
			ech <- err
			return
		}

		expect(t, rows, 0)

		t.Log("7")
		seven <- s
	}()

	go func() {
		<-one

		_, err := db.Exec(`CREATE (f:FOO)`, nil)
		if err != nil {
			ech <- err
			return
		}

		t.Log("2")
		two <- s
		<-three

		_, err = db.Exec(`MATCH (f:FOO) CREATE UNIQUE (f)-[b:BAR]->(c:BAZ)`, nil)
		if err != nil {
			ech <- err
			return
		}

		t.Log("4")
		four <- s
		<-five

		_, err = db.Exec(`MATCH (:FOO)-[b:BAR]->(:BAZ) DELETE b`, nil)
		if err != nil {
			t.Fatalf("An error occurred creating f neo: %s", err)
		}

		_, err = db.Exec(`MATCH (n) DETACH DELETE n`, nil)
		if err != nil {
			ech <- err
			return
		}

		t.Log("6")
		six <- s
		<-seven
		done <- s
	}()

	for {
		select {
		case err := <-ech:
			t.Fatal(err)
		case <-done:
			err = db.Close()
			if err != nil {
				t.Fatal(err)
			}
			return
		}
	}
}
