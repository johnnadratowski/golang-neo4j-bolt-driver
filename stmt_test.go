package golangNeo4jBoltDriver

import (
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"strconv"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
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
	conn, err := newBoltConn(neo4jConnStr)
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

	if output[0].(int8) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %#v", output[0])
	}
	if output[1].(float64) != 34234.34323 {
		t.Fatalf("Unexpected output. Expected 34234.34323. Got: %#v", output[1])
	}
	if output[2].(string) != "string" {
		t.Fatalf("Unexpected output. Expected string. Got: %#v", output[2])
	}
	if !reflect.DeepEqual(output[3].([]interface{}), []interface{}{int8(1), "2", int8(3), true, interface{}(nil)}) {
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
	conn, err := newBoltConn(neo4jConnStr)
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
		"d": []interface{}{int8(1), "2", int8(3), true, interface{}(nil)},
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

func TestBoltStmt_Exec(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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

	if output[0].(int8) != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %#v", output[0])
	}
	if output[1].(float64) != 34234.34323 {
		t.Fatalf("Unexpected output. Expected 34234.34323. Got: %#v", output[1])
	}
	if output[2].(string) != "string" {
		t.Fatalf("Unexpected output. Expected string. Got: %#v", output[2])
	}
	if !reflect.DeepEqual(output[3].([]interface{}), []interface{}{int8(1), int8(2), int8(3)}) {
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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
		"e": []interface{}{int8(1), int8(2), int8(3)},
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
	if !reflect.DeepEqual(output[0].(graph.Relationship).Properties["e"].([]interface{}), []interface{}{int8(1), int8(2), int8(3)}) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["f"].(float64) != 3.4 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Relationship).Properties["g"].(int32) != math.MaxInt32 {
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
	conn, err := newBoltConn(neo4jConnStr)
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
		"e": []interface{}{int8(1), int8(2), int8(3)},
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
	if output[0].(graph.Node).Properties["b"].(int8) != 1 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !output[0].(graph.Node).Properties["c"].(bool) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["d"] != nil {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if !reflect.DeepEqual(output[0].(graph.Node).Properties["e"].([]interface{}), []interface{}{int8(1), int8(2), int8(3)}) {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["f"].(float64) != 3.4 {
		t.Fatalf("Unexpected return data: %#v", output)
	}
	if output[0].(graph.Node).Properties["g"].(int8) != -1 {
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
	conn, err := newBoltConn(neo4jConnStr)
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
	if output[1].(int32) != math.MinInt32 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt32, output[1])
	}
	if output[2].(int16) != math.MinInt16 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt16, output[2])
	}
	if output[3].(int8) != math.MinInt8 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MinInt8, output[3])
	}
	if output[4].(int8) != -16 {
		t.Fatalf("Unexpected output. Expected -16. Got: %d", output[4])
	}
	if output[5].(int8) != math.MaxInt8 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt8, output[5])
	}
	if output[6].(int16) != math.MaxInt16 {
		t.Fatalf("Unexpected output. Expected %d. Got: %d", math.MaxInt16, output[6])
	}
	if output[7].(int32) != math.MaxInt32 {
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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
	conn, err := newBoltConn(neo4jConnStr)
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

func TestBoltStmt_SelectStructLimits(t *testing.T) {
	t.Skip("Are there any structs that have more than a few fields?")
}

func TestBoltStmt_ManyChunks(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
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
