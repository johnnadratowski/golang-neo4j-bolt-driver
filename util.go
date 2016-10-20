package golangNeo4jBoltDriver

import (
	"database/sql/driver"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
)

// driverArgsToMap turns driver.Value list into a parameter map for Neo4j
// parameters.
func driverArgsToMap(args []driver.Value) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(args))
	for _, arg := range args {
		b, ok := arg.([]byte)
		if !ok {
			return nil, errors.New("You must pass only a gob encoded map to the Exec/Query args")
		}
		ifc, err := encoding.Unmarshal(b)
		if err != nil {
			return nil, err
		}
		m, ok := ifc.(map[string]interface{})
		if !ok {
			return nil, errors.New("wanted map[string]interface{}, got %T", ifc)
		}
		for k, v := range m {
			out[k] = v
		}
	}
	return out, nil
}
