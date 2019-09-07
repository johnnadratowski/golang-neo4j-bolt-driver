package golangNeo4jBoltDriver

import "testing"

func TestRoutingPoolInit(t *testing.T) {
	connStr := "bolt+routing://neo4j:changeme@localhost:7687"

	driver, err := NewClosableDriverPool(connStr, 50)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	rwConn, err := driver.OpenPool(ReadWriteMode)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	tx, err := rwConn.Begin()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	_, err = rwConn.QueryNeo("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": 1, "bar": 2.2})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	err = tx.Commit()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	err = rwConn.Close()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	//attempt to make an invalid connection
	roConn, err := driver.OpenPool(ReadOnlyMode)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	_, err = roConn.QueryNeo("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": 1, "bar": 2.2})
	if err == nil {
		t.Log("that should not have worked")
		t.FailNow()
	}

	err = roConn.Close()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	err = driver.Close()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}
