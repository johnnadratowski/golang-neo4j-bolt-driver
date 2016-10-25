package golangNeo4jBoltDriver

import (
	"io"
	"reflect"
	"testing"
)

func TestBoltConn_parseURL(t *testing.T) {
	v := make(values)

	err := parseURL(v, "bolt://john@foo:1234")
	if err == nil {
		t.Fatal("Expected error from missing password")
	}

	err = parseURL(v, "bolt://john:password@foo:7687")
	if err != nil {
		t.Fatal("Should not error on valid url")
	}

	if v.get("username") != "john" {
		t.Fatal("Expected user to be 'john'")
	}
	if v.get("password") != "password" {
		t.Fatal("Expected password to be 'password'")
	}

	err = parseURL(v, "bolt://john:password@foo:7687?tls=true")
	if err != nil {
		t.Fatal("Should not error on valid url")
	}
	if v.get("tls") != "true" {
		t.Fatal("Expected to use TLS")
	}

	err = parseURL(v, "bolt://john:password@foo:7687?tls=true&tls_no_verify=1&tls_ca_cert_file=ca&tls_cert_file=cert&tls_key_file=key")
	if err != nil {
		t.Fatal("Should not error on valid url")
	}
	if v.get("tls") != "true" {
		t.Fatal("Expected to use TLS")
	}
	if v.get("tls_no_verify") != "1" {
		t.Fatal("Expected to use TLS with no verification")
	}
	if v.get("tls_ca_cert_file") != "ca" {
		t.Fatal("Expected ca cert file 'ca'")
	}
	if v.get("tls_cert_file") != "cert" {
		t.Fatal("Expected cert file 'cert'")
	}
	if v.get("tls_key_file") != "key" {
		t.Fatal("Expected key file 'key'")
	}
}

func TestBoltConn_Close(t *testing.T) {
	// Records session for testing
	rec := NewRecorder("TestBoltConn_Close")

	cn, err := rec.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	err = cn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !cn.(*conn).closed {
		t.Error("Conn not closed at end of test")
	}
}

func TestBoltConn_SelectOne(t *testing.T) {
	// Records session for testing
	rec := NewRecorder("TestBoltConn_SelectOne")

	conn, err := rec.OpenNeo(neo4jConnStr)
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
	// Records session for testing
	rec := NewRecorder("TestBoltConn_SelectAll")

	conn, err := rec.OpenNeo(neo4jConnStr)
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

	results, err = conn.ExecNeo("MATCH (n:NODE) DELETE n", nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
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

func TestBoltConn_Ignored(t *testing.T) {
	// Records session for testing
	rec := NewRecorder("TestBoltConn_Ignored")

	conn, err := rec.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err = conn.ExecNeo("syntax error", map[string]interface{}{"foo": 1, "bar": 2.2})
	if err == nil {
		t.Fatal("Expected an error on syntax error.")
	}

	data, _, _, err := conn.QueryNeoAll("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("Got error when running next query after a failure: %#v", err)
	}

	if data[0][0].(int64) != 1 {
		t.Fatalf("Expected different data from output: %#v", data)
	}
}

func TestBoltConn_IgnoredPipeline(t *testing.T) {
	// Records session for testing
	rec := NewRecorder("TestBoltConn_IgnoredPipeline")

	conn, _ := rec.OpenNeo(neo4jConnStr)
	defer conn.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err := conn.ExecPipeline([]string{"syntax error", "syntax error", "syntax error"}, nil)
	if err == nil {
		t.Fatal("Expected an error on syntax error.")
	}

	data, _, _, err := conn.QueryNeoAll("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("Got error when running next query after a failure: %#v", err)
	}

	if data[0][0].(int64) != 1 {
		t.Fatalf("Expected different data from output: %#v", data)
	}
}
