package golangNeo4jBoltDriver

import (
	"io"
	"reflect"
	"testing"
)

func TestBoltConn_Close(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_Close", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !conn.(*boltConn).closed {
		t.Errorf("Conn not closed at end of test")
	}
}

func TestBoltConn_SelectOne(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_SelectOne", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	rows, err := conn.QueryNeo("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	expectedMetadata := map[string]interface{}{
		"fields": []interface{}{"1"},
	}
	if !reflect.DeepEqual(rows.Metadata(), expectedMetadata) {
		t.Fatalf("Unexpected success metadata. Expected %#v. Got: %#v", expectedMetadata, rows.Metadata())
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(int64) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %d", output)
	}

	_, metadata, err := rows.NextNeo()
	expectedMetadata = map[string]interface{}{"type": "r"}
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	} else if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Fatalf("Metadata didn't match expected. Expected %#v. Got: %#v", expectedMetadata, metadata)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_SelectAll(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_SelectAll", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	results, err := conn.ExecNeo("CREATE (f:NODE {a: 1}), (b:NODE {a: 2})", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}
	affected, err := results.RowsAffected()
	if err != nil {
		t.Fatalf("An error occurred getting rows affected: %s", err)
	}
	if affected != int64(2) {
		t.Fatalf("Incorrect number of rows affected: %d", affected)
	}

	data, rowMetadata, metadata, err := conn.QueryNeoAll("MATCH (n:NODE) RETURN n.a ORDER BY n.a", nil)
	if data[0][0] != int64(1) {
		t.Fatalf("Incorrect data returned for first row: %#v", data[0])
	}
	if data[1][0] != int64(2) {
		t.Fatalf("Incorrect data returned for second row: %#v", data[1])
	}

	if rowMetadata["fields"].([]interface{})[0] != "n.a" {
		t.Fatalf("Unexpected column metadata: %#v", rowMetadata)
	}

	if metadata["type"].(string) != "r" {
		t.Fatalf("Unexpected request metadata: %#v", metadata)
	}

	affected, err = results.RowsAffected()
	if err != nil {
		t.Fatalf("An error occurred getting rows affected: %s", err)
	}
	if affected != int64(2) {
		t.Fatalf("Incorrect number of rows affected: %d", affected)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}
