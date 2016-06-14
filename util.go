package golangNeo4jBoltDriver

import "fmt"

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
