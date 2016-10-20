package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"fmt"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
)

// sprintByteHex returns a formatted string of the byte array in hexadecimal
// with a nicely formatted human-readable output
func sprintByteHex(b []byte) string {
	output := "\t"
	for i, b := range b {
		output += fmt.Sprintf("%x", b)
		if (i+1)%16 == 0 {
			output += "\n\n\t"
		} else if (i+1)%4 == 0 {
			output += "  "
		} else {
			output += " "
		}
	}
	return output + "\n"
}

// driverArgsToMap turns driver.Value list into a parameter map
// for neo4j parameters
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
