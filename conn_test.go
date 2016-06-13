package golangNeo4jBoltDriver

import "testing"

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
