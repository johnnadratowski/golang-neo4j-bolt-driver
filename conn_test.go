package golangNeo4jBoltDriver

import "testing"

func TestBoltConn_Close(t *testing.T) {
	conn, err := newBoltConn(neo4jConnStr, "Conn.Close")
	if err != nil {
		t.Fatalf("An error occurred opening conn: %s", err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("An error occurred closing conn: %s", err)
	}
}
