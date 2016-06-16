package golangNeo4jBoltDriver

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
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
	if output[4].(bool) != true {
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

func TestBoltStmt_SelectIntLimits(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	query := fmt.Sprintf(`RETURN %d, %d, %d, %d, -16, %d, %d, %d, %d`, math.MinInt64, math.MinInt32, math.MinInt16, math.MinInt8, math.MaxInt8, math.MaxInt16, math.MaxInt32, math.MaxInt64)
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

func TestBoltStmt_Exec(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr)
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	stmt, err := conn.PrepareNeo(`CREATE (f:FOO {a: "foo", b: 1, c: true, d: null, e: [1, 2, 3], f: 3.4, g: -1})`)
	if err != nil {
		t.Fatalf("An error occurred preparing statement: %s", err)
	}

	result, err := stmt.ExecNeo(nil)
	if err != nil {
		t.Fatalf("An error occurred querying Neo: %s", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Error getting rows affected: %s", err)
	}

	expected := int64(1)
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

	stmt, err = conn.PrepareNeo(`MATCH (f:FOO) DELETE f`)
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

	expected = int64(1)
	if affected != expected {
		t.Fatalf("Unexpected rows affected from delete node. Expected %#v. Got: %#v. Metadata: %#v", expected, affected, result.Metadata())
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
	if strings.Index(err.Error(), expected) == -1 {
		t.Fatalf("Did not recieve expected error: %s", err)
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
	if output[4].(bool) != true {
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
