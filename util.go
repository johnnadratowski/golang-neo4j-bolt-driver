package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
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
	output += "\n"

	return output
}

// driverArgsToMap turns driver.Value list into a parameter map
// for neo4j parameters
func driverArgsToMap(args []driver.Value) (map[string]interface{}, error) {
	output := map[string]interface{}{}
	for _, arg := range args {
		argBytes, ok := arg.([]byte)
		if !ok {
			return nil, errors.New("You must pass only a gob encoded map to the Exec/Query args")
		}

		m, err := encoding.Unmarshal(argBytes)
		if err != nil {
			return nil, err
		}

		for k, v := range m.(map[string]interface{}) {
			output[k] = v
		}

	}

	return output, nil
}
