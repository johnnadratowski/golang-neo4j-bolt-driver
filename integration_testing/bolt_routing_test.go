package integration_testing

import (
	dr "github.com/mindstand/golang-neo4j-bolt-driver"
	"log"
	"sync"
	"testing"
)

func TestBoltRoutingPool(t *testing.T) {
	connStr := "bolt://neo4j:changeme@localhost:7687"

	poolSize := 50

	driver, err := dr.NewClosableDriverPool(connStr, poolSize)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	reads := 0
	writes := 0

	var wg sync.WaitGroup

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			var err error
			conn := &dr.BoltConn{}
			if i%2 == 0 {
				log.Println(i, "write", writes)
				writes++
				conn, err = driver.Open(dr.ReadWriteMode)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				tx, err := conn.Begin()
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

				_, _, _, err = conn.QueryNeoAll("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": i, "bar": 2.2})
				if err != nil {
					panic(err)
				}

				_, err = conn.ExecNeo("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": i * i, "bar": 2.2})
				if err != nil {
					panic(err)
				}

				_, _, _, err = conn.QueryNeoAll("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": i * i, "bar": 2.2})
				if err != nil {
					panic(err)
				}

				err = tx.Commit()
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
			} else {
				log.Println(i, "read", reads)
				reads++
				conn, err = driver.Open(dr.ReadOnlyMode)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

				_, _, _, err := conn.QueryNeoAll("match (n) return n limit 1", nil)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

				_, _, _, err = conn.QueryNeoAll("match (n) return n limit 2", nil)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

				_, err = conn.ExecNeo("match (n) return n limit 3", nil)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

				_, _, _, err = conn.QueryNeoAll("match (n) return n limit 2", nil)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}

			}

			if i%5 == 0 {
				err := conn.Close()
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
			}

			err = driver.Reclaim(conn)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			log.Println("done", i)
			wg.Done()
		}(i, &wg)

	}

	wg.Wait()

	err = driver.Close()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}
