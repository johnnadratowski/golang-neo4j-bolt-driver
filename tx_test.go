package bolt

import (
	"testing"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/graph"
)

func TestBoltTx_Commit(t *testing.T) {
	// Records session for testing
	driver := newRecorder(t, "TestBoltTx_Commit", neo4jConnStr)

	tx, err := driver.Begin()
	if err != nil {
		t.Fatalf("an error occurred beginning transaction: %s", err)
	}

	stmt, err := tx.Prepare(`CREATE (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("an error occurred preparing statement: %s", err)
	}

	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatalf("an error occurred querying Neo: %s", err)
	}

	if num, err := result.RowsAffected(); num != 5 {
		t.Fatalf("expected 5 rows affected: %#v err: %#v", result.Metadata(), err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("an error occurred closing statement")
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("an error occurred committing transaction: %s", err)
	}

	stmt, err = driver.Prepare(`MATCH (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("an error occurred preparing statement: %s", err)
	}

	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("an error occurred querying Neo: %s", err)
	}

	output := ifcs(5)
	for rows.Next() {
		rows.Scan(output...)
	}
	deref(output...)

	if err := rows.Err(); err != nil {
		t.Fatalf("an error occurred getting next row: %s", err)
	}

	err = rows.Close()
	if err != nil {
		t.Fatal(err)
	}

	if output[0].(graph.Node).Labels[0] != "FOO" {
		t.Fatalf("unexpected return data: %s", err)
	}
	if output[1].(graph.Relationship).Type != "TO" {
		t.Fatalf("unexpected return data: %s", err)
	}
	if output[2].(graph.Node).Labels[0] != "BAR" {
		t.Fatalf("unexpected return data: %s", err)
	}
	if output[3].(graph.Relationship).Type != "FROM" {
		t.Fatalf("unexpected return data: %s", err)
	}
	if output[4].(graph.Node).Labels[0] != "BAZ" {
		t.Fatalf("unexpected return data: %s", err)
	}

	// Closing in middle of record stream
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err = driver.Prepare(`MATCH (f:FOO)-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) DELETE f, b, c, d, e`)
	if err != nil {
		t.Fatalf("an error occurred preparing delete statement: %s", err)
	}

	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("an error occurred on delete query to Neo: %s", err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = driver.Close()
	if err != nil {
		t.Fatalf("error closing connection: %s", err)
	}
}

func TestBoltTx_Rollback(t *testing.T) {
	// Records session for testing
	driver := newRecorder(t, "TestBoltTx_Rollback", neo4jConnStr)

	tx, err := driver.Begin()
	if err != nil {
		t.Fatalf("an error occurred beginning transaction: %s", err)
	}

	stmt, err := tx.Prepare(`CREATE (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("an error occurred preparing statement: %s", err)
	}

	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatalf("an error occurred querying Neo: %s", err)
	}

	if num, err := result.RowsAffected(); num != 5 {
		t.Fatalf("expected 5 rows affected: %#v err: %#v", result.Metadata(), err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("an error occurred closing statement")
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("an error occurred committing transaction: %s", err)
	}

	stmt, err = driver.Prepare(`MATCH (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
	if err != nil {
		t.Fatalf("an error occurred preparing statement: %s", err)
	}

	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("an error occurred querying Neo: %s", err)
	}

	expect(t, rows, 0)

	err = stmt.Close()
	if err != nil {
		t.Fatalf("an error occurred closing statement")
	}

	err = driver.Close()
	if err != nil {
		t.Fatalf("error closing connection: %s", err)
	}
}
