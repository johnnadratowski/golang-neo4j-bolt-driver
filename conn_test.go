package golangNeo4jBoltDriver

import (
	"database/sql"
	"io"
	"reflect"
	"testing"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
)

func TestBoltConn_Close(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}

	if !conn.closed {
		t.Errorf("Conn not closed at end of test")
	}
}

func TestBoltConn_SelectOne(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
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

func TestBoltConn_PipelineQuery(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}}) RETURN f",
		"CREATE (b:BAR {a: {a}, b: {b}}) RETURN b",
		"CREATE (c:BAZ {a: {a}, b: {b}}) RETURN c",
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

	rows, err := conn.QueryPipeline(stmts, params...)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
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

	if foo[0].(graph.Node).Labels[0] != "FOO" && foo[0].(graph.Node).Properties["b"] != int8(1) && foo[0].(graph.Node).Properties["a"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if bar[0].(graph.Node).Labels[0] != "BAR" && bar[0].(graph.Node).Properties["a"] != int8(2) && bar[0].(graph.Node).Properties["b"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if baz[0].(graph.Node).Labels[0] != "BAZ" && baz[0].(graph.Node).Properties["a"] != int8(3) && baz[0].(graph.Node).Properties["b"] != "four" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if nextRows != nil {
		t.Fatalf("Expected nil rows, got: %#v", rows)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Error closing pipeline rows: %s", err)
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

func TestBoltConn_SqlQueryAndExec(t *testing.T) {
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

	rows, err := db.Query(`CREATE path=(f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}})-[b:BAR]->(c:BAZ) RETURN f.a, f.b, f.c, f.d, f.e, f.f, f, b, path`, arg)
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
	if !reflect.DeepEqual(dVal.([]interface{}), []interface{}{int8(1), int8(2), int8(3)}) {
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
	if nodeVal.(graph.Node).Properties["a"] != int8(1) {
		t.Fatalf("Unexpected value for node. Expected: %#v  Got: %#v", int8(1), nodeVal)
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

	result, err := db.Exec(`MATCH (f:FOO)-[b:BAR]->(c:BAZ) DELETE f, b, c`)
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

	err = db.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_ExecNeo(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
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
	result, err := conn.ExecNeo(`CREATE (f:FOO {a: {a}, b: {b}, c: {c}, d: {d}, e: {e}, f: {f}, g: {g}, h: {h}})-[b:BAR]->(c:BAZ)`, params)
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

	stmt, err := conn.PrepareNeo(`MATCH (f:FOO) SET f.a = "bar";`)
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

func TestBoltConn_PipelineExec(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmts := []string{
		"CREATE (f:FOO {a: {a}, b: {b}})",
		"CREATE (b:BAR {a: {a}, b: {b}})",
		"CREATE (c:BAZ {a: {a}, b: {b}})",
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

	results, err := conn.ExecPipeline(stmts, params...)
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

	if output[0].(graph.Node).Labels[0] != "FOO" && output[0].(graph.Node).Properties["b"] != int8(1) && output[0].(graph.Node).Properties["a"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[1].(graph.Node).Labels[0] != "BAR" && output[1].(graph.Node).Properties["a"] != int8(2) && output[1].(graph.Node).Properties["b"] != "two" {
		t.Fatalf("Unexpected return data: %s", err)
	}
	if output[2].(graph.Node).Labels[0] != "BAZ" && output[2].(graph.Node).Properties["a"] != int8(3) && output[2].(graph.Node).Properties["b"] != "four" {
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
