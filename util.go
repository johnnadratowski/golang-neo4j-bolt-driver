package bolt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"errors"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/encoding"
)

var errNotMap = errors.New("if one argument is passed it must be a bolt-encoded map")

// maybeMap returns whether the []byte could be a bolt-encoded map without
// actually marshalling it into a map.
func maybeMap(m []byte) bool {
	// TODO: make this a function inside encoding?

	minlen := 1 /* marker */ + 2 /* length */ + len(encoding.EndMessage)

	// Must be long enough to hold a marker, length, and ending bytes.
	if len(m) < minlen {
		return false
	}

	// Must have ending bytes.
	if !bytes.HasSuffix(m, encoding.EndMessage) {
		return false
	}

	// Length must be correct.
	if int(binary.BigEndian.Uint16(m)) != len(m)-(2 /* length */ +len(encoding.EndMessage)) {
		return false
	}

	// And, finally, it must have a map marker.
	c := m[2]
	return (c >= encoding.TinyMapMarker &&
		c <= encoding.TinyMapMarker+0x0F) ||
		c == encoding.Map8Marker ||
		c == encoding.Map16Marker ||
		c == encoding.Map32Marker
}

// driverArgsToMap turns driver.Value list into a parameter map for Neo4j
// parameters.
func driverArgsToMap(args []driver.Value) (map[string]interface{}, error) {
	if len(args) == 1 {
		b, ok := args[0].([]byte)
		if !ok || !maybeMap(b) {
			return nil, errNotMap
		}
		v, err := encoding.Unmarshal(b)
		if err != nil {
			return nil, err
		}
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, errNotMap
		}
		return m, nil
	}

	if len(args)%2 != 0 {
		return nil, errors.New("must pass an even number of arguments")
	}

	var r bytes.Reader
	var dec *encoding.Decoder

	out := make(map[string]interface{}, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			return nil, errors.New("even-numbered argument(s) must be a string")
		}
		v := args[i+1]
		b, ok := v.([]byte)
		// If type is []byte we have two cases:
		//
		// 	1.) It's encoded with bolt's protocol, in which case it'll end with
		// 		two 0 bytes.
		//
		// 	2.) It's a regular byte slice, in which case it may or may not end
		// 		with two 0 bytes.
		//
		// 	Handle case #1 first, falling back to plugging the byte slice into
		// 	the map.
		if ok && bytes.HasSuffix(b, encoding.EndMessage) {
			r.Reset(b)
			if dec == nil {
				dec = encoding.NewDecoder(&r)
			}
			v, err := dec.Decode()
			// If err == nil case 1 succeeded.
			if err == nil {
				out[key] = v
				continue
			}
		}
		out[key] = v
	}
	return out, nil
}
