package golangNeo4jBoltDriver

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"net/url"
	"strings"
	"sync"
)

type boltRoutingDriverPool struct {
	//must be of the protocol `bolt+routing`
	connStr  string
	maxConns int
	refLock  sync.Mutex
	closed   bool

	username string
	password string

	//cluster configuration
	config *clusterConnectionConfig

	//write resources
	writeConns int
	writePool  *pool.ObjectPool

	//read resources
	readConns int
	readPool  *pool.ObjectPool
}

func createRoutingDriverPool(connStr string, max int) (*boltRoutingDriverPool, error) {
	if max < 2 {
		return nil, errors.New("max must be at least 2")
	}

	var writeConns int
	var readConns int

	//max conns is even
	if max%2 == 0 {
		writeConns = max / 2
		readConns = max / 2
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
		connStr:  connStr,
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

	//close original driver
	err = clusterInfoDriver.Close()
	if err != nil {
		return err
	}

	writeConnStr := ""

	for _, l := range b.config.Leaders {
		for _, a := range l.Addresses {
			if strings.Contains(a, "bolt") {
				//retain login info
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != "" {
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
	//b.writePool
	writeFactory := pool.NewPooledObjectFactorySimple(getPoolFunc([]string{writeConnStr}, false))

	b.writePool = pool.NewObjectPool(context.Background(), writeFactory, &pool.ObjectPoolConfig{
		LIFO:                     true,
		MaxTotal:                 b.writeConns,
		MaxIdle:                  b.writeConns,
		MinIdle:                  0,
		TestOnCreate:             true,
		TestOnBorrow:             true,
		TestOnReturn:             true,
		TestWhileIdle:            true,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     pool.DefaultMinEvictableIdleTime,
		SoftMinEvictableIdleTime: pool.DefaultSoftMinEvictableIdleTime,
		NumTestsPerEvictionRun:   3,
		TimeBetweenEvictionRuns:  0,
	})

	var readUris []string
	//parse followers
	for _, follow := range b.config.Followers {
		for _, a := range follow.Addresses {
			if strings.Contains(a, "bolt") {
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != "" {
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
	for _, replica := range b.config.ReadReplicas {
		for _, a := range replica.Addresses {
			if strings.Contains(a, "bolt") {
				if b.username != "" {
					u, err := url.Parse(a)
					if err != nil {
						return err
					}

					pwdStr := ""
					if b.password != "" {
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

	readFactory := pool.NewPooledObjectFactorySimple(getPoolFunc(readUris, true))
	b.readPool = pool.NewObjectPool(context.Background(), readFactory, &pool.ObjectPoolConfig{
		LIFO:                     true,
		MaxTotal:                 b.readConns,
		MaxIdle:                  b.readConns,
		MinIdle:                  0,
		TestOnCreate:             true,
		TestOnBorrow:             true,
		TestOnReturn:             true,
		TestWhileIdle:            true,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     pool.DefaultMinEvictableIdleTime,
		SoftMinEvictableIdleTime: pool.DefaultSoftMinEvictableIdleTime,
		NumTestsPerEvictionRun:   3,
		TimeBetweenEvictionRuns:  0,
	})

	return nil
}

func (b *boltRoutingDriverPool) Close() error {
	b.refLock.Lock()
	defer b.refLock.Unlock()

	b.writePool.Close(context.Background())
	b.readPool.Close(context.Background())

	b.closed = true
	return nil
}

func (b *boltRoutingDriverPool) Open(mode DriverMode) (*BoltConn, error) {
	// For each connection request we need to block in case the Close function is called. This gives us a guarantee
	// when closing the pool no new connections are made.
	b.refLock.Lock()
	defer b.refLock.Unlock()
	ctx := context.Background()
	if !b.closed {
		conn := &BoltConn{}
		var ok bool

		switch mode {
		case ReadOnlyMode:
			connObj, err := b.readPool.BorrowObject(ctx)
			if err != nil {
				return nil, err
			}

			conn, ok = connObj.(*BoltConn)
			if !ok {
				return nil, errors.New("unable to cast to *BoltConn")
			}
			break
		case ReadWriteMode:
			connObj, err := b.writePool.BorrowObject(ctx)
			if err != nil {
				return nil, err
			}

			conn, ok = connObj.(*BoltConn)
			if !ok {
				return nil, errors.New("unable to cast to *BoltConn")
			}
			break
		default:
			return nil, errors.New("invalid driver mode")
		}

		//check to make sure the connection is open
		if connectionNilOrClosed(conn) {
			//if it isn't, reset it
			err := conn.initialize()
			if err != nil {
				return nil, err
			}

			conn.closed = false
			conn.connErr = nil
			conn.statement = nil
			conn.transaction = nil
		}

		return conn, nil
	}
	return nil, errors.New("Driver pool has been closed")
}

func (b *boltRoutingDriverPool) Reclaim(conn *BoltConn) error {
	if conn == nil {
		return errors.New("cannot reclaim nil connection")
	}
	if connectionNilOrClosed(conn) {
		err := conn.initialize()
		if err != nil {
			return err
		}

		conn.closed = false
		conn.connErr = nil
		conn.statement = nil
		conn.transaction = nil

	} else {
		if conn.statement != nil {
			if !conn.statement.closed {
				if conn.statement.rows != nil && !conn.statement.rows.closed {
					err := conn.statement.rows.Close()
					if err != nil {
						return err
					}
				}
			}

			conn.statement = nil
		}

		if conn.transaction != nil {
			if !conn.transaction.closed {
				err := conn.transaction.Rollback()
				if err != nil {
					return err
				}
			}

			conn.transaction = nil
		}
	}

	if conn.readOnly {
		return b.readPool.ReturnObject(context.Background(), conn)
	} else {
		return b.writePool.ReturnObject(context.Background(), conn)
	}
}
