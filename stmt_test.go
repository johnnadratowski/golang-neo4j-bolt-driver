package golangNeo4jBoltDriver

import (
	"io"
	"reflect"
	"testing"
)

func TestBoltStmt_SelectOne(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo("RETURN 1;")
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
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

	if output[0].(int8) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %d", output)
	}

	_, metadata, err := rows.NextNeo()
	expectedMetadata = map[string]interface {}{"type":"r"}
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	} else if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Fatalf("Metadata didn't match expected. Expected %#v. Got: %#v", expectedMetadata, metadata)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !conn.closed {
		t.Errorf("Conn not closed at end of test")
	}
}

func TestBoltStmt_CreateNode(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo("RETURN 1;")
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.ExecNeo(nil)
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

	if output[0].(int8) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %d", output)
	}

	_, metadata, err := rows.NextNeo()
	expectedMetadata = map[string]interface {}{"type":"r"}
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	} else if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Fatalf("Metadata didn't match expected. Expected %#v. Got: %#v", expectedMetadata, metadata)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !conn.closed {
		t.Errorf("Conn not closed at end of test")
	}
}
