package golangNeo4jBoltDriver

import (
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"strconv"

	"database/sql"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
)

func TestBoltStmt_SelectOne(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectOne", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
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

func TestBoltStmt_SelectMany(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectMany", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`RETURN 1, 34234.34323, "string", [1, "2", 3, true, null], true, null;`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	expectedMetadata := map[string]interface{}{
		"fields": []interface{}{"1", "34234.34323", "\"string\"", "[1, \"2\", 3, true, null]", "true", "null"},
	}
	if !reflect.DeepEqual(rows.Metadata(), expectedMetadata) {
		t.Fatalf("Unexpected success metadata. Expected %#v. Got: %#v", expectedMetadata, rows.Metadata())
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(int64) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %#v", output[0])
	}
	if output[1].(float64) != 34234.34323 {
		t.Fatalf("Unexpected output. Expected 34234.34323. Got: %#v", output[1])
	}
	if output[2].(string) != "string" {
		t.Fatalf("Unexpected output. Expected string. Got: %#v", output[2])
	}
	if !reflect.DeepEqual(output[3].([]interface{}), []interface{}{int64(1), "2", int64(3), true, interface{}(nil)}) {
		t.Fatalf("Unexpected output. Expected []interface{}{1, '2', 3, true, nil}. Got: %#v", output[3])
	}
	if !output[4].(bool) {
		t.Fatalf("Unexpected output. Expected true. Got: %#v", output[4])
	}
	if output[5] != nil {
		t.Fatalf("Unexpected output. Expected nil. Got: %#v", output[5])
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

func TestBoltStmt_InvalidArgs(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_InvalidArgs", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}}) RETURN f`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	args := map[string]interface{}{
		"a": 1,
		"b": 34234.34323,
		"c": "string",
		"d": []interface{}{int64(1), "2", int64(3), true, interface{}(nil)},
		"e": true,
		"f": nil,
	}
	_, err = stmt.QueryNeo(args)

	expected := "Collections containing mixed types can not be stored in properties"
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("Did not recieve expected error: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_ExecNeo(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_ExecNeo", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}, g: {g}, h: {h}})-[b:BAR]->(c:BAZ)`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": "foo",
		"b": 1,
		"c": true,
		"d": nil,
		"e": []interface{}{1, 2, 3},
		"f": 3.4,
		"g": -1,
		"h": false,
	}
	result, err := stmt.ExecNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting rows affected: %s", err)
	}

	expected := int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from create node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) SET f.a = "bar";`)
	if err != nil {
		t.Fatalf("An error occurred preparing update statement: %s", err)
	}

	result, err = stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on update query to Neo: %s", err)
	}

	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting update rows affected: %s", err)
	}

	expected = int64(0)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from update node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO)-[b:BAR]->(c:BAZ) DELETE f, b, c`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	result, err = stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting delete rows affected: %s", err)
	}

	expected = int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_CreateArgs(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_CreateArgs", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}}) RETURN f.a, f.b, f.c, f.d, f.e, f.f`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	args := map[string]interface{}{
		"a": 1,
		"b": 34234.34323,
		"c": "string",
		"d": []interface{}{1, 2, 3},
		"e": true,
		"f": nil,
	}
	rows, err := stmt.QueryNeo(args)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(int64) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %#v", output[0])
	}
	if output[1].(float64) != 34234.34323 {
		t.Fatalf("Unexpected output. Expected 34234.34323. Got: %#v", output[1])
	}
	if output[2].(string) != "string" {
		t.Fatalf("Unexpected output. Expected string. Got: %#v", output[2])
	}
	if !reflect.DeepEqual(output[3].([]interface{}), []interface{}{int64(1), int64(2), int64(3)}) {
		t.Fatalf("Unexpected output. Expected []interface{}{1, 2, 3}. Got: %#v", output[3])
	}
	if !output[4].(bool) {
		t.Fatalf("Unexpected output. Expected true. Got: %#v", output[4])
	}
	if output[5] != nil {
		t.Fatalf("Unexpected output. Expected nil. Got: %#v", output[5])
	}

	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) DELETE f`)
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

func TestBoltStmt_Discard(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_Discard", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "1"}), (b:FOO {a: "2"}) RETURN f, b`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	// Closing stmt should discard stream when it wasn't yet consumed
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) RETURN f.a ORDER BY f.a`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err = stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "1" {
		t.Fatalf("Unexpected return data: %s", err)
	}

	// Closing in middle of record stream
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) RETURN f.a ORDER BY f.a`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err = stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err = rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "1" {
		t.Fatalf("Unexpected return data: %#v", output[0])
	}

	output, _, err = rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "2" {
		t.Fatalf("Unexpected return data: %#v", output[0])
	}

	// Ensure we're getting proper data in subsequent queries
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) DELETE f`)
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

func TestBoltStmt_Failure(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_Failure", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "1"}), (b:FOO {a: "2"}) RETURN f, b`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	stmt.Close()

	// Check a failure from an invalid query
	stmt, err = conn.PrepareNeo(`This is an invalid query`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	_, err = stmt.QueryNeo(nil)
	if err == nil {
		t.Fatalf("Invalid query should return an error")
	}

	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) RETURN f.a ORDER BY f.a`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err = stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "1" {
		t.Fatalf("Unexpected return data: %s", err)
	}

	// Closing in middle of record stream
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) RETURN f.a ORDER BY f.a`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	rows, err = stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err = rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "1" {
		t.Fatalf("Unexpected return data: %#v", output[0])
	}

	output, _, err = rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(string) != "2" {
		t.Fatalf("Unexpected return data: %#v", output[0])
	}

	// Ensure we're getting proper data in subsequent queries
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) DELETE f`)
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

func TestBoltStmt_MixedObjects(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_MixedObjects", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN f, b, c, d, e`)
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

func TestBoltStmt_Path(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_Path", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE path=(f:FOO {a: "1"})-[b:TO]->(c:BAR)<-[d:FROM]-(e:BAZ) RETURN path`)
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

	path, ok := output[0].(graph.Path)
	if !ok {
		t.Fatalf("Unrecognized return type for path: %#v", output[0])
	}

	if path.Nodes[0].Labels[0] != "FOO" {
		t.Fatalf("Unexpected node return data 1: %#v", path)
	}
	if path.Nodes[1].Labels[0] != "BAR" {
		t.Fatalf("Unexpected node return data 2: %#v", path)
	}
	if path.Nodes[2].Labels[0] != "BAZ" {
		t.Fatalf("Unexpected node return data 3: %#v", path)
	}
	if path.Relationships[0].Type != "TO" {
		t.Fatalf("Unexpected relationship return data: %#v", path)
	}
	if path.Relationships[1].Type != "FROM" {
		t.Fatalf("Unexpected relationship return data: %#v", path)
	}
	if path.Sequence[0] != 1 {
		t.Fatalf("Unexpected sequence return data: %#v", path)
	}
	if path.Sequence[1] != 1 {
		t.Fatalf("Unexpected sequence return data: %#v", path)
	}
	if path.Sequence[2] != -2 {
		t.Fatalf("Unexpected sequence return data: %#v", path)
	}
	if path.Sequence[3] != 2 {
		t.Fatalf("Unexpected sequence return data: %#v", path)
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

func TestBoltStmt_SingleRel(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SingleRel", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO)-[b:BAR {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}, g: {g}, h: {h}}]->(c:BAZ) return b`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": "foo",
		"b": int64(math.MaxInt64),
		"c": true,
		"d": nil,
		"e": []interface{}{int64(1), int64(2), int64(3)},
		"f": 3.4,
		"g": int32(math.MaxInt32),
		"h": false,
	}
	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(graph.Relationship).Properties["a"].(string) != "foo" {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["b"].(int64) != math.MaxInt64 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !output[0].(graph.Relationship).Properties["c"].(bool) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["d"] != nil {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !reflect.DeepEqual(output[0].(graph.Relationship).Properties["e"].([]interface{}), []interface{}{int64(1), int64(2), int64(3)}) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["f"].(float64) != 3.4 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["g"].(int64) != math.MaxInt32 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["h"].(bool) {
		t.Fatalf("Unexpected return data: %#v", output)
	}

	// Closing in middle of record stream
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO)-[b:BAR]->(c:BAZ) DELETE f, b, c`)
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

func TestBoltStmt_SingleNode(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SingleNode", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}, g: {g}, h: {h}}) return f`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": "foo",
		"b": 1,
		"c": true,
		"d": nil,
		"e": []interface{}{int64(1), int64(2), int64(3)},
		"f": 3.4,
		"g": -1,
		"h": false,
	}
	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(graph.Node).Properties["a"].(string) != "foo" {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["b"].(int64) != 1 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !output[0].(graph.Node).Properties["c"].(bool) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["d"] != nil {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !reflect.DeepEqual(output[0].(graph.Node).Properties["e"].([]interface{}), []interface{}{int64(1), int64(2), int64(3)}) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["f"].(float64) != 3.4 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["g"].(int64) != -1 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["h"].(bool) {
		t.Fatalf("Unexpected return data: %#v", output)
	}

	// Closing in middle of record stream
	stmt.Close()

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) DELETE f`)
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

func TestBoltStmt_SelectIntLimits(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectIntLimits", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	query := `RETURN {min64} as min64, {min32} as min32, {min16} as min16, {min8} as min8, -16, {max8} as max8, {max16} as max16, {max32} as max32, {max64} as max64`
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"min64": math.MinInt64,
		"min32": math.MinInt32,
		"min16": math.MinInt16,
		"min8":  math.MinInt8,
		"max8":  math.MaxInt8,
		"max16": math.MaxInt16,
		"max32": math.MaxInt32,
		"max64": math.MaxInt64,
	}
	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if output[0].(int64) != math.MinInt64 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt64, output[0])
	}
	if output[1].(int64) != math.MinInt32 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt32, output[1])
	}
	if output[2].(int64) != math.MinInt16 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt16, output[2])
	}
	if output[3].(int64) != math.MinInt8 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt8, output[3])
	}
	if output[4].(int64) != -16 {
		t.Fatalf("Unexpected output. Expected -16. Got: %d", output[4])
	}
	if output[5].(int64) != math.MaxInt8 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt8, output[5])
	}
	if output[6].(int64) != math.MaxInt16 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt16, output[6])
	}
	if output[7].(int64) != math.MaxInt32 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt32, output[7])
	}
	if output[8].(int64) != math.MaxInt64 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt64, output[8])
	}

	_, _, err = rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_SelectStringLimits(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectStringLimits", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	query := `RETURN {a} as a, {b} as b, {c} as c, {d} as d`
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": strings.Repeat("-", 15),
		"b": strings.Repeat("-", 16),
		"c": strings.Repeat("-", int(math.MaxUint8)+1),
		"d": strings.Repeat("-", int(math.MaxUint16)+1),
	}
	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if !reflect.DeepEqual(rows.Columns(), []string{"a", "b", "c", "d"}) {
		t.Fatalf("Unexpected columns. %#v", rows.Columns())
	}

	if output[0].(string) != params["a"].(string) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["a"], output[0])
	}
	if output[1].(string) != params["b"].(string) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["b"], output[1])
	}
	if output[2].(string) != params["c"].(string) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["c"], output[2])
	}
	if output[3].(string) != params["d"].(string) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["d"], output[3])
	}

	_, _, err = rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_SelectSliceLimits(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectSliceLimits", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	query := `RETURN {a} as a, {b} as b, {c} as c, {d} as d`
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": make([]interface{}, 15),
		"b": make([]interface{}, 16),
		"c": make([]interface{}, int(math.MaxUint8)+1),
		"d": make([]interface{}, int(math.MaxUint16)+1),
	}
	for _, v := range params {
		for i := range v.([]interface{}) {
			v.([]interface{})[i] = "-"
		}
	}

	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if !reflect.DeepEqual(rows.Columns(), []string{"a", "b", "c", "d"}) {
		t.Fatalf("Unexpected columns. %#v", rows.Columns())
	}

	if !reflect.DeepEqual(output[0].([]interface{}), params["a"].([]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["a"], output[0])
	}
	if !reflect.DeepEqual(output[1].([]interface{}), params["b"].([]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["b"], output[1])
	}
	if !reflect.DeepEqual(output[2].([]interface{}), params["c"].([]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["c"], output[2])
	}
	if !reflect.DeepEqual(output[3].([]interface{}), params["d"].([]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["d"], output[3])
	}

	_, _, err = rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_SelectMapLimits(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_SelectMapLimits", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	query := `RETURN {a} as a, {b} as b, {c} as c, {d} as d`
	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := map[string]interface{}{
		"a": make(map[string]interface{}, 15),
		"b": make(map[string]interface{}, 16),
		"c": make(map[string]interface{}, int(math.MaxUint8)+1),
		"d": make(map[string]interface{}, int(math.MaxUint16)+1),
	}

	for i := 0; i < int(math.MaxUint16)+1; i++ {
		key := strconv.Itoa(i)
		if i <= 15 {
			params["a"].(map[string]interface{})[key] = "-"
		}
		if i <= 16 {
			params["b"].(map[string]interface{})[key] = "-"
		}
		if i <= int(math.MaxUint8)+1 {
			params["c"].(map[string]interface{})[key] = "-"
		}
		if i <= int(math.MaxUint16)+1 {
			params["d"].(map[string]interface{})[key] = "-"
		}
	}

	rows, err := stmt.QueryNeo(params)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if !reflect.DeepEqual(rows.Columns(), []string{"a", "b", "c", "d"}) {
		t.Fatalf("Unexpected columns. %#v", rows.Columns())
	}

	if !reflect.DeepEqual(output[0].(map[string]interface{}), params["a"].(map[string]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["a"], output[0])
	}
	if !reflect.DeepEqual(output[1].(map[string]interface{}), params["b"].(map[string]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["b"], output[1])
	}
	if !reflect.DeepEqual(output[2].(map[string]interface{}), params["c"].(map[string]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["c"], output[2])
	}
	if !reflect.DeepEqual(output[3].(map[string]interface{}), params["d"].(map[string]interface{})) {
		t.Fatalf("Unexpected output. Expected: %#v Got: %#v", params["d"], output[3])
	}

	_, _, err = rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_ManyChunks(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_ManyChunks", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	conn.SetChunkSize(10)
	query := `RETURN "1 2 3 4 5 6 7 8 9 10" as a,  "1 2 3 4 5 6 7 8 9 10" as b, "1 2 3 4 5 6 7 8 9 10" as c`
	stmt, err := conn.PrepareNeo(query)
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

	if !reflect.DeepEqual(rows.Columns(), []string{"a", "b", "c"}) {
		t.Fatalf("Unexpected columns. %#v", rows.Columns())
	}

	if output[0].(string) != "1 2 3 4 5 6 7 8 9 10" {
		t.Fatalf("Unexpected output. %#v", output[0])
	}
	if output[1].(string) != "1 2 3 4 5 6 7 8 9 10" {
		t.Fatalf("Unexpected output. %#v", output[1])
	}
	if output[2].(string) != "1 2 3 4 5 6 7 8 9 10" {
		t.Fatalf("Unexpected output. %#v", output[2])
	}

	_, _, err = rows.NextNeo()
	if err != io.EOF {
		t.Fatalf("Unexpected row closed output. Expected io.EOF. Got: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_PipelineExec(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_PipelineExec", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}})",
		"CREATE (b:BAR {a: {a}, b: {b}})",
		"CREATE (c:BAZ {a: {a}, b: {b}})",
	}

	pipeline, err := conn.PreparePipeline(stmts...)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := []map[string]interface{}{
		map[string]interface{}{
			"a": 1,
			"b": "two",
		},
		map[string]interface{}{
			"a": 2,
			"b": "three",
		},
		map[string]interface{}{
			"a": 3,
			"b": "four",
		},
	}
	results, err := pipeline.ExecPipeline(params...)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	affected, err := results[0].RowsAffected()
	if err != nil {
		t.Fatalf("Error getting rows affected: %s", err)
	}
	expected := int64(1)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from create node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, results[0].Metadata())
	}
	affected, err = results[1].RowsAffected()
	if err != nil {
		t.Fatalf("Error getting rows affected: %s", err)
	}
	expected = int64(1)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from create node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, results[1].Metadata())
	}
	affected, err = results[2].RowsAffected()
	if err != nil {
		t.Fatalf("Error getting rows affected: %s", err)
	}
	expected = int64(1)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from create node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, results[2].Metadata())
	}

	err = pipeline.Close()
	if err != nil {
		t.Fatalf("Error closing statement: %s", err)
	}

	stmt, err := conn.PrepareNeo(`MATCH (f:FOO), (b:BAR), (c:BAZ) return f, b, c;`)
	if err != nil {
		t.Fatalf("An error occurred preparing match statement: %s", err)
	}

	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on match query to Neo: %s", err)
	}

	output, _, err := rows.NextNeo()
	if err != nil {
		t.Fatalf("An error occurred on getting row: %s", err)
	}

	if output[0].(graph.Node).Labels[0] != "FOO" && output[0].(graph.Node).Properties["b"] != int64(1) && output[0].(graph.Node).Properties["a"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[1].(graph.Node).Labels[0] != "BAR" && output[1].(graph.Node).Properties["a"] != int64(2) && output[1].(graph.Node).Properties["b"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[2].(graph.Node).Labels[0] != "BAZ" && output[2].(graph.Node).Properties["a"] != int64(3) && output[2].(graph.Node).Properties["b"] != "four" {
		t.Fatalf("Unexpected return data: %s", err)
	}

	stmt.Close()
	if err != nil {
		t.Fatalf("Error closing statement: %s", err)
	}

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO), (b:BAR), (c:BAZ) DELETE f, b, c`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	affected, err = result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting delete rows affected: %s", err)
	}

	expected = int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_PipelineQuery(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_PipelineQuery", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}}) RETURN f",
		"CREATE (b:BAR {a: {a}, b: {b}}) RETURN b",
		"CREATE (c:BAZ {a: {a}, b: {b}}) RETURN c",
	}

	pipeline, err := conn.PreparePipeline(stmts...)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := []map[string]interface{}{
		map[string]interface{}{
			"a": 1,
			"b": "two",
		},
		map[string]interface{}{
			"a": 2,
			"b": "three",
		},
		map[string]interface{}{
			"a": 3,
			"b": "four",
		},
	}
	rows, err := pipeline.QueryPipeline(params...)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	foo, _, _, err := rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting foo row: %s", err)
	}

	_, _, rows, err = rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting bar rows: %s", err)
	}

	bar, _, _, err := rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting bar row: %s", err)
	}

	_, _, rows, err = rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting baz rows: %s", err)
	}

	baz, _, _, err := rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting baz row: %s", err)
	}

	_, _, nextRows, err := rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting final rows: %s", err)
	}

	if foo[0].(graph.Node).Labels[0] != "FOO" && foo[0].(graph.Node).Properties["b"] != int64(1) && foo[0].(graph.Node).Properties["a"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if bar[0].(graph.Node).Labels[0] != "BAR" && bar[0].(graph.Node).Properties["a"] != int64(2) && bar[0].(graph.Node).Properties["b"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if baz[0].(graph.Node).Labels[0] != "BAZ" && baz[0].(graph.Node).Properties["a"] != int64(3) && baz[0].(graph.Node).Properties["b"] != "four" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if nextRows != nil {
		t.Fatalf("Expected nil rows, got: %#v", rows)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Error closing pipeline rows: %s", err)
	}

	err = pipeline.Close()
	if err != nil {
		t.Fatalf("Error closing statement: %s", err)
	}

	stmt, err := conn.PrepareNeo(`MATCH (f:FOO), (b:BAR), (c:BAZ) DELETE f, b, c`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting delete rows affected: %s", err)
	}

	expected := int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_PipelineQueryCloseBeginning(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_PipelineQueryCloseBeginning", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}}) RETURN f",
		"CREATE (b:BAR {a: {a}, b: {b}}) RETURN b",
		"CREATE (c:BAZ {a: {a}, b: {b}}) RETURN c",
	}

	pipeline, err := conn.PreparePipeline(stmts...)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := []map[string]interface{}{
		map[string]interface{}{
			"a": 1,
			"b": "two",
		},
		map[string]interface{}{
			"a": 2,
			"b": "three",
		},
		map[string]interface{}{
			"a": 3,
			"b": "four",
		},
	}
	rows, err := pipeline.QueryPipeline(params...)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Error closing pipeline rows: %s", err)
	}

	err = pipeline.Close()
	if err != nil {
		t.Fatalf("Error closing statement: %s", err)
	}

	stmt, err := conn.PrepareNeo(`MATCH (f:FOO), (b:BAR), (c:BAZ) DELETE f, b, c`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting delete rows affected: %s", err)
	}

	expected := int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_PipelineQueryCloseMiddle(t *testing.T) {
	driver := NewDriver()

	// Records session for testing
	driver.(*boltDriver).recorder = newRecorder("TestBoltStmt_PipelineQueryCloseMiddle", neo4jConnStr)

	conn, err := driver.OpenNeo(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}}) RETURN f",
		"CREATE (b:BAR {a: {a}, b: {b}}) RETURN b",
		"CREATE (c:BAZ {a: {a}, b: {b}}) RETURN c",
	}

	pipeline, err := conn.PreparePipeline(stmts...)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	params := []map[string]interface{}{
		map[string]interface{}{
			"a": 1,
			"b": "two",
		},
		map[string]interface{}{
			"a": 2,
			"b": "three",
		},
		map[string]interface{}{
			"a": 3,
			"b": "four",
		},
	}
	rows, err := pipeline.QueryPipeline(params...)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	foo, _, _, err := rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting foo row: %s", err)
	}

	_, _, rows, err = rows.NextPipeline()
	if err != nil {
		t.Fatalf("Error getting bar rows: %s", err)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Error closing pipeline rows: %s", err)
	}

	if foo[0].(graph.Node).Labels[0] != "FOO" && foo[0].(graph.Node).Properties["b"] != int64(1) && foo[0].(graph.Node).Properties["a"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}

	err = pipeline.Close()
	if err != nil {
		t.Fatalf("Error closing statement: %s", err)
	}

	stmt, err := conn.PrepareNeo(`MATCH (f:FOO), (b:BAR), (c:BAZ) DELETE f, b, c`)
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred on delete query to Neo: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting delete rows affected: %s", err)
	}

	expected := int64(3)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltStmt_SqlQueryAndExec(t *testing.T) {
	if neo4jConnStr == "" {
		t.Skip("Cannot run this test when in recording mode")
	}

	db, err := sql.Open("neo4j-bolt", neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}
	defer db.Close()

	args := map[string]interface{}{
		"a": 1,
		"b": 34234.34323,
		"c": "string",
		"d": []interface{}{1, 2, 3},
		"e": true,
		"f": nil,
	}
	arg, err := encoding.Marshal(args)
	if err != nil {
		t.Fatalf("An error occurred marshalling args: %s", err)
	}

	stmt, err := db.Prepare(`CREATE path=(f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}})-[b:BAR]->(c:BAZ) RETURN f.a, f.b, f.c, f.d, f.e, f.f, f, b, path`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}
	rows, err := stmt.Query(arg)
	if err != nil {
		t.Fatalf("An error occurred querying statement: %s", err)
	}
	var a int
	var b float64
	var c string
	var d []byte
	var e bool
	var f interface{}
	var node []byte
	var rel []byte
	var path []byte
	if !rows.Next() {
		t.Fatalf("Rows.Next failed")
	}
	err = rows.Scan(&a, &b, &c, &d, &e, &f, &node, &rel, &path)
	if err != nil {
		t.Fatalf("An error occurred scanning row: %s", err)
	}
	defer rows.Close()

	if a != 1 {
		t.Fatalf("Unexpected value for a. Expected: %#v  Got: %#v", 1, a)
	}
	if b != 34234.34323 {
		t.Fatalf("Unexpected value for b. Expected: %#v  Got: %#v", 34234.34323, b)
	}
	if c != "string" {
		t.Fatalf("Unexpected value for c. Expected: %#v  Got: %#v", "string", b)
	}

	dVal, err := encoding.Unmarshal(d)
	if err != nil {
		t.Fatalf("Error occurred decoding item: %s", err)
	}
	if !reflect.DeepEqual(dVal.([]interface{}), []interface{}{int64(1), int64(2), int64(3)}) {
		t.Fatalf("Unexpected value for d. Expected: %#v  Got: %#v", []interface{}{1, 2, 3}, dVal)
	}

	if !e {
		t.Fatalf("Unexpected value for e. Expected: %#v  Got: %#v", true, e)
	}

	if f != nil {
		t.Fatalf("Unexpected value for f. Expected: %#v  Got: %#v", nil, f)
	}

	nodeVal, err := encoding.Unmarshal(node)
	if err != nil {
		t.Fatalf("Error occurred decoding node: %s", err)
	}
	if nodeVal.(graph.Node).Labels[0] != "FOO" {
		t.Fatalf("Unexpected label for node. Expected: %#v  Got: %#v", "FOO", nodeVal)
	}
	if nodeVal.(graph.Node).Properties["a"] != int64(1) {
		t.Fatalf("Unexpected value for node. Expected: %#v  Got: %#v", int64(1), nodeVal)
	}

	relVal, err := encoding.Unmarshal(rel)
	if err != nil {
		t.Fatalf("Error occurred decoding rel: %s", err)
	}
	if relVal.(graph.Relationship).Type != "BAR" {
		t.Fatalf("Unexpected label for node. Expected: %#v  Got: %#v", "FOO", relVal)
	}

	pathVal, err := encoding.Unmarshal(path)
	if err != nil {
		t.Fatalf("Error occurred decoding path: %s", err)
	}
	if pathVal.(graph.Path).Nodes[0].Labels[0] != "FOO" {
		t.Fatalf("Unexpected label for path node 0. Expected: %#v  Got: %#v", "FOO", pathVal)
	}
	if pathVal.(graph.Path).Nodes[1].Labels[0] != "BAZ" {
		t.Fatalf("Unexpected label for path node 1. Expected: %#v  Got: %#v", "BAZ", pathVal)
	}
	if pathVal.(graph.Path).Relationships[0].Type != "BAR" {
		t.Fatalf("Unexpected label for path relationship 1. Expected: %#v  Got: %#v", "BAR", pathVal)
	}
	if pathVal.(graph.Path).Sequence[0] != 1 {
		t.Fatalf("Unexpected label for path sequence 0. Expected: %#v  Got: %#v", 1, pathVal)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("An error occurred closing statement: %s", err)
	}

	stmt, err = db.Prepare(`MATCH (f:FOO)-[b:BAR]->(c:BAZ) DELETE f, b, c`)

	result, err := stmt.Exec()
	if err != nil {
		t.Fatalf("An error occurred preparing delete statement: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("An error occurred getting affected rows: %s", err)
	}
	if affected != 3 {
		t.Fatalf("Expected to delete 3 items, got %#v", affected)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("An error occurred closing statement: %s", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}
