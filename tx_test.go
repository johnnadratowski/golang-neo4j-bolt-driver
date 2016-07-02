package golangNeo4jBoltDriver

import (
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"io"
	"testing"
)

func TestBoltTx_Commit(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltTx_Commit", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("An error occurred beginning transaction: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	if num, err := result.RowsAffected(); num != 5 {
		t.Fatalf("Expected 5 rows affected: %#v err: %#v", result.Metadata(), err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("An error occurred committing transaction: %s", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("An error occurred closing statement")
	}

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(graph.Node).Labels[0] != "FOO" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[1].(graph.Relationship).Type != "TO" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[2].(graph.Node).Labels[0] != "BAR" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[3].(graph.Relationship).Type != "FROM" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[4].(graph.Node).Labels[0] != "BAZ" {
		t.Fatalf("Unexpected return data: %s", err)
	}

	// Closing in middle of record stream
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO)-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) DELETE f, b, c, d, e`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	_, err = stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltTx_Rollback(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltTx_Rollback", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("An error occurred beginning transaction: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	if num, err := result.RowsAffected(); num != 5 {
		t.Fatalf("Expected 5 rows affected: %#v err: %#v", result.Metadata(), err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("An error occurred committing transaction: %s", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("An error occurred closing statement")
	}

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected error returned from getting next rows: %s", err)
	}

	if len(output) != 0 {
		t.Fatalf("Unexpected return data: %s", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("An error occurred closing statement")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}
