package golangNeo4jBoltDriver

import (
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	blog "github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
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
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_Ignored", neo4jConnStr)

	conn, _ := driver.OpenNeo(neo4jConnStr)
	defer conn.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err := conn.ExecNeo("syntax error", map[string]interface{}{"foo": 1, "bar": 2.2})
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
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_IgnoredPipeline", neo4jConnStr)

	conn, _ := driver.OpenNeo(neo4jConnStr)
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

func TestBoltConn_ExecPipeline(t *testing.T) {
	blog.SetLevel("")
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltConn_IgnoredPipeline", neo4jConnStr)

	conn, _ := driver.OpenNeo(neo4jConnStr)
	d, _ := time.ParseDuration("5s")
	conn.SetTimeout(d)
	defer conn.Close()
	start := time.Now()

	query := "MERGE (c:Contact {id: {id}}) SET c.www = {www}, c.full_name = {full_name} " +
		"WITH c " +
		"OPTIONAL MATCH (m)-[existing:MANAGER]->(c) " +
		"DELETE m, existing " +
		"MERGE (c)<-[:MANAGER]-(:Manager { manager: {a} }) " +
		"MERGE (c)<-[:MANAGER]-(:Manager { manager: {b} }) " +
		"MERGE (c)<-[:MANAGER]-(:Manager { manager: {c} })"

	var queries []string
	var queryParams []map[string]interface{}

	loopLimit := 8000
	i := 0
	for {
		queryParam := make(map[string]interface{})
		queryParam["id"] = "nothing unique here"
		queryParam["www"] = "http://www.kylehq.com"
		queryParam["full_name"] = "Kyle Clarke name drop yo!"
		queryParam["a"] = "foo"
		queryParam["b"] = "bar"
		queryParam["c"] = "baz"
		queryParams = append(queryParams, queryParam)
		queries = append(queries, query)

		i++
		if i > loopLimit {
			break
		}
	}

	pipeline, err := conn.PreparePipeline(queries...)
	if err != nil {
		t.Fatalf("Got an error on prepare pipeline query: %#v", err)
	}

	_, err = pipeline.ExecPipeline(queryParams...)
	if err != nil {
		t.Fatalf("Got error on Exec pipeline: %#v", err)
	}

	fmt.Println(fmt.Sprintf("Pipeline took %f seconds to complete for %d iterations. Is this acceptable?", time.Since(start).Seconds(), loopLimit))

	// Clean up
	pipeline.Close()
}
