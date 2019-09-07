package golangNeo4jBoltDriver

import "testing"

func TestRoutingInit(t *testing.T) {
	connStr := "bolt+routing://neo4j:changeme@localhost:7687"

	driver := NewDriver()

	conn, err := driver.OpenNeo(connStr)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	err = conn.Close()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}
