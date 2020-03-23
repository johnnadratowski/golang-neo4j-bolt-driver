package golangNeo4jBoltDriver

import (
	"context"
	"github.com/mindstand/golang-neo4j-bolt-driver/errors"
	"math/rand"
)

func getPoolFunc(connStrs []string, readonly bool) func(ctx context.Context) (interface{}, error) {
	return func(ctx context.Context) (interface{}, error) {
		if len(connStrs) == 0 {
			return nil, errors.New("no connection strings provided")
		}

		var i int

		if len(connStrs) == 1 {
			i = 0
		} else {
			i = rand.Intn(len(connStrs))
		}

		conn, err := createBoltConn(connStrs[i])
		if err != nil {
			return nil, err
		}
		//set whether or not it is readonly
		conn.readOnly = readonly

		return conn, nil
	}
}
