package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"net/url"
	"strings"
	"sync"
)

type BoltRoutingDriver struct{

}

func (b *BoltRoutingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("sql driver is not supported for routing mode")
}

func (b *BoltRoutingDriver) OpenNeo(string) (Conn, error) {
	panic("implement me")
}

type boltRoutingDriverPool struct {
	//must be of the protocol `bolt+routing`
	connStr  string
	maxConns int
	refLock  sync.Mutex
	closed bool

	username string
	password string

	//cluster configuration
	config *clusterConnectionConfig

	//write resources
	writeConns int
	writePool     chan *boltConn
	writeConnRefs []*boltConn

	//read resources
	readConns int
	readPool     chan *boltConn
	readConnRefs []*boltConn
}

func createRoutingDriverPool(connStr string, max int) (*boltRoutingDriverPool, error) {
	if max == 2 {
		return nil, errors.New("max must be at least 2")
	}

	var writeConns int
	var readConns int

	//max conns is even
	if max % 2 == 0 {
		writeConns = max/2
		readConns = max/2
	} else {
		//if given odd, make more write conns
		c := (max - 1) / 2
		writeConns = c + 1
		readConns = c
	}

	u, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	}

	pwd, ok := u.User.Password()
	if !ok {
		pwd = ""
	}

	d := &boltRoutingDriverPool{
		//main stuff
		maxConns: max,
		connStr: connStr,
		username: u.User.Username(),
		password: pwd,
		//write
		writeConns: writeConns,
		//read
		readConns: readConns,
	}

	err = d.refreshConnectionPool()
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (b *boltRoutingDriverPool) refreshConnectionPool() error {
	//get the connection info
	clusterInfoDriver, err := NewDriver().OpenNeo(b.connStr)
	if err != nil {
		return err
	}

	clusterInfo, err := getClusterInfo(clusterInfoDriver)
	if err != nil {
		return err
	}

	b.config = clusterInfo
	b.writePool = make(chan *boltConn, b.writeConns)
	b.readPool = make(chan *boltConn, b.readConns)

	//close original driver
	err = clusterInfoDriver.Close()
	if err != nil {
		return err
	}

	//todo better distribution

	writeConnStr := ""

	for _, l := range b.config.Leaders{
		for _, a := range l.Addresses{
			if strings.Contains(a, "bolt"){
				//retain login info
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != ""{
						pwdStr = fmt.Sprintf(":%s@", b.password)
					}

					writeConnStr = fmt.Sprintf("bolt://%s%s%s:%s", b.username, pwdStr, u.Hostname(), u.Port())
					break
				}

				writeConnStr = a
				break
			}
		}
	}

	//make write pool
	for i := 0; i < b.writeConns; i++ {
		conn, err := newPooledBoltConn(writeConnStr, b)
		if err != nil {
			return err
		}

		conn.readOnly = false

		b.writePool <- conn
	}

	var readUris []string
	//parse followers
	for _, follow := range b.config.Followers {
		for _, a := range follow.Addresses{
			if strings.Contains(a, "bolt") {
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != ""{
						pwdStr = fmt.Sprintf(":%s@", b.password)
					}

					readUris = append(readUris, fmt.Sprintf("bolt://%s%s%s:%s", b.username, pwdStr, u.Hostname(), u.Port()))
					break
				}
				readUris = append(readUris, a)
				break
			}
		}
	}

	//parse replicas
	for _, replica := range b.config.ReadReplicas{
		for _, a := range replica.Addresses {
			if strings.Contains(a, "bolt") {
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != ""{
						pwdStr = fmt.Sprintf(":%s@", b.password)
					}

					readUris = append(readUris, fmt.Sprintf("bolt://%s%s%s:%s", b.username, pwdStr, u.Hostname(), u.Port()))
					break
				}
				readUris = append(readUris, a)
				break
			}
		}
	}

	if readUris == nil || len(readUris) == 0 {
		return errors.New("no read nodes to connect to")
	}

	counter := 0
	for i := 0; i < b.readConns; i++ {
		str := readUris[counter]
		conn, err := newPooledBoltConn(str, b)
		if err != nil {
			return err
		}

		conn.readOnly = true

		b.readPool <- conn

		counter++
		if counter >= len(readUris) {
			counter = 0
		}
	}
	return nil
}

func (b *boltRoutingDriverPool) Close() error {
	b.refLock.Lock()
	defer b.refLock.Unlock()

	for _, conn := range b.writeConnRefs {
		conn.poolDriver = nil
		err := conn.Close()
		if err != nil {
			b.closed = true
			return err
		}
	}

	for _, conn := range b.readConnRefs {
		conn.poolDriver = nil
		err := conn.Close()
		if err != nil {
			b.closed = true
			return err
		}
	}

	b.closed = true
	return nil
}

func (b boltRoutingDriverPool) OpenPool(mode DriverMode) (Conn, error) {
	// For each connection request we need to block in case the Close function is called. This gives us a guarantee
	// when closing the pool no new connections are made.
	b.refLock.Lock()
	defer b.refLock.Unlock()
	if !b.closed {
		if mode == ReadOnlyMode {
			conn := <-b.readPool
			if connectionNilOrClosed(conn) {
				if err := conn.initialize(); err != nil {
					return nil, err
				}
				b.readConnRefs = append(b.readConnRefs, conn)
			}
			return conn, nil
		} else if mode == ReadWriteMode {
			conn := <-b.writePool
			if connectionNilOrClosed(conn) {
				if err := conn.initialize(); err != nil {
					return nil, err
				}
				b.writeConnRefs = append(b.writeConnRefs, conn)
			}
			return conn, nil
		} else {
			return nil, errors.New("invalid driver mode")
		}
	}
	return nil, errors.New("Driver pool has been closed")
}

func (b boltRoutingDriverPool) reclaim(conn *boltConn) error {
	var newConn *boltConn
	var err error
	if conn.connErr != nil || conn.closed {
		newConn, err = newPooledBoltConn(conn.connStr, b)
		if err != nil {
			return err
		}
	} else {
		// sneakily swap out connection so a reference to
		// it isn't held on to
		newConn = &boltConn{}
		*newConn = *conn
	}

	if conn.readOnly {
		b.readPool <- newConn
	} else {
		b.writePool <- newConn
	}
	
	conn = nil

	return nil
}
//
//type BoltRoutingConn struct{
//
//}
//
//func newPooledBoltRoutingConn(connStr string, d DriverPool) (*boltConn, error) {
//	return nil, nil
//}
//
//func (b *BoltRoutingConn) PrepareNeo(query string) (Stmt, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) PreparePipeline(query ...string) (PipelineStmt, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) QueryNeo(query string, params map[string]interface{}) (Rows, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) QueryNeoAll(query string, params map[string]interface{}) ([][]interface{}, map[string]interface{}, map[string]interface{}, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) QueryPipeline(query []string, params ...map[string]interface{}) (PipelineRows, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) ExecNeo(query string, params map[string]interface{}) (Result, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) ExecPipeline(query []string, params ...map[string]interface{}) ([]Result, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) Close() error {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) Begin() (driver.Tx, error) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) SetChunkSize(uint16) {
//	panic("implement me")
//}
//
//func (b *BoltRoutingConn) SetTimeout(time.Duration) {
//	panic("implement me")
//}
