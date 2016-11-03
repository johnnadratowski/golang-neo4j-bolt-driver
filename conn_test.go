package bolt

import "testing"

func newRecorder(t *testing.T, name, dsn string) *DB {
	db, err := OpenRecorder(name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

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
	rec := newRecorder(t, "TestBoltConn_Close", neo4jConnStr)

	err := rec.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}
}

func TestBoltConn_SelectOne(t *testing.T) {
	// Records session for testing
	rec := newRecorder(t, "TestBoltConn_SelectOne", neo4jConnStr)

	var out int64
	err := rec.QueryRow("RETURN 1;", nil).Scan(&out)
	if err != nil {
		t.Fatalf("An error occurred getting next row: %s", err)
	}

	if out != 1 {
		t.Fatalf("Unexpected output. Expected 1. Got: %d", out)
	}

	err = rec.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_SelectAll(t *testing.T) {
	// Records session for testing
	rec := newRecorder(t, "TestBoltConn_SelectAll", neo4jConnStr)

	results, err := rec.Exec("CREATE (f:NODE {a: 1}), (b:NODE {a: 2})", nil)
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

	rows, err := rec.Query("MATCH (n:NODE) RETURN n.a ORDER BY n.a", nil)
	if err != nil {
		t.Fatal(err)
	}

	var metadata []map[string]interface{}
	md, err := rows.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	metadata = append(metadata, md)

	var out []int64
	var x int64
	for rows.Next() {
		rows.Scan(&x)
		out = append(out, x)
	}

	md, err = rows.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	metadata = append(metadata, md)

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(out) != 2 {
		t.Fatalf("wanted len == 2, got %d", len(out))
	}

	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	if out[0] != int64(1) {
		t.Fatalf("Incorrect data returned for first row: %#v", out[0])
	}
	if out[1] != int64(2) {
		t.Fatalf("Incorrect data returned for second row: %#v", out[1])
	}

	if metadata[0]["fields"].([]interface{})[0] != "n.a" {
		t.Fatalf("Unexpected column metadata: %#v", metadata)
	}

	if metadata[1]["type"].(string) != "r" {
		t.Fatalf("Unexpected request metadata: %#v", metadata)
	}

	results, err = rec.Exec("MATCH (n:NODE) DELETE n", nil)
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

	err = rec.Close()
	if err != nil {
		t.Fatalf("Error closing connection: %s", err)
	}
}

func TestBoltConn_Ignored(t *testing.T) {
	// Records session for testing
	rec := newRecorder(t, "TestBoltConn_Ignored", neo4jConnStr)

	defer rec.Close()

	// This will make two calls at once - Run and Pull All.  The pull all should be ignored, which is what
	// we're testing.
	_, err := rec.Query("syntax error", map[string]interface{}{"foo": 1, "bar": 2.2})
	if err == nil {
		t.Fatal("Expected an error on syntax error.")
	}

	rows, err := rec.Query("RETURN 1;", nil)
	if err != nil {
		t.Fatalf("Got error when running next query after a failure: %#v", err)
	}
	defer rows.Close()

	var out int64
	for rows.Next() {
		rows.Scan(&out)
	}

	if out != 1 {
		t.Fatalf("Expected different data from output: %#v", out)
	}
}
