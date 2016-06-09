package messages

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures"
)

const (
	NIL_MARKER = 0xC0

	TRUE_MARKER = 0xC3
	FALSE_MARKER = 0xC2

	INT8_MARKER = 0xC8
	INT16_MARKER = 0xC9
	INT32_MARKER = 0xCA
	INT64_MARKER = 0xCB

	FLOAT_MARKER = 0xC1

	TINYSTRING_MARKER = 0x80
	STRING8_MARKER = 0xD0
	STRING16_MARKER = 0xD1
	STRING32_MARKER = 0xD2

	TINYLIST_MARKER = 0x90
	LIST8_MARKER = 0xD4
	LIST16_MARKER = 0xD5
	LIST32_MARKER = 0xD6

	TINYMAP_MARKER = 0xA0
	MAP8_MARKER = 0xD8
	MAP16_MARKER = 0xD9
	MAP32_MARKER = 0xDA

	TINYSTRUCT_MARKER = 0xB0
	STRUCT8_MARKER = 0xDC
	STRUCT16_MARKER = 0xDD
)

// Encoder encodes objects of different types to the given stream.
// Attempts to support all builtin golang types, when it can be confidently
// mapped to a data type from: http://alpha.neohq.net/docs/server-manual/bolt-serialization.html#bolt-packstream-structures
// (version v3.1.0-M02 at the time of writing this.
//
// Maps and Slices are a special case, where only
// map[string]interface{} and []interface{} are supported.
// The interface for maps and slices may be more permissive in the future.
type Encoder struct {
	io.Writer
}

// NewEncoder Creates a new Encoder object
func NewEncoder(w io.Writer) Encoder {
	return Encoder{Writer: w}
}

// Encode encodes an object to the stream
func (e Encoder) Encode(iVal interface{}) error {

	// TODO: How to handle pointers?
	//if reflect.TypeOf(iVal) == reflect.Ptr {
	//	return Encode(*iVal)
	//}

	var err error
	switch val := iVal.(type) {
	case nil:
		err = e.encodeNil()
	case bool:
		err = e.encodeBool(val)
	case int:
		err = e.encodeInt(int64(val))
	case int8:
		err = e.encodeInt(int64(val))
	case int16:
		err = e.encodeInt(int64(val))
	case int32:
		err = e.encodeInt(int64(val))
	case int64:
		err = e.encodeInt(int64(val))
	case uint:
		err = e.encodeInt(int64(val))
	case uint8:
		err = e.encodeInt(int64(val))
	case uint16:
		err = e.encodeInt(int64(val))
	case uint32:
		err = e.encodeInt(int64(val))
	case uint64:
		// TODO: Bolt docs only mention going up to int64, not uint64
		// So I'll make this fail for now
		if val > math.MaxInt64 {
			return fmt.Errorf("Integer too big: %d. Max integer supported: %d", val, math.MaxInt64)
		}
		err = e.encodeInt(int64(val))
	case float32:
		err = e.encodeFloat(float64(val))
	case float64:
		err = e.encodeFloat(val)
	case string:
		err = e.encodeString(val)
	case []interface{}:
		// TODO: Support specific list types?
		err = e.encodeList(val)
	case map[string]interface{}:
		// TODO: Support keys other than strings?
		// TODO: Support specific map types?
		err = e.encodeMap(val)
	case structures.Structure:
		err = e.encodeStructure(val)
	default:
		// TODO: How to handle rune or byte?
		return fmt.Errorf("Unrecognized type when encoding data for Bolt transport: %T %+v", val, val)
	}

	return err
}

// encodeNil encodes a nil object to the stream
func (e Encoder) encodeNil() error {
	_, err := e.Write([]byte{NIL_MARKER})
	return err
}

// encodeBool encodes a nil object to the stream
func (e Encoder) encodeBool(val bool) error {
	var err error
	if val {
		_, err = e.Write([]byte{TRUE_MARKER})
	} else {
		_, err = e.Write([]byte{FALSE_MARKER})
	}
	return err
}

// encodeInt encodes a nil object to the stream
func (e Encoder) encodeInt(val int64) error {
	var err error
	switch {
	case val >= -9223372036854775808 && val <= -2147483649:
		// Write as INT_64
		if _, err = e.Write([]byte{INT64_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= -2147483648 && val <= -32769:
		// Write as INT_32
		if _, err = e.Write([]byte{INT32_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int32(val))
	case val >= -32768 && val <= -129:
		// Write as INT_16
		if _, err = e.Write([]byte{INT16_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int16(val))
	case val >= -128 && val <= -17:
		// Write as INT_8
		if _, err = e.Write([]byte{INT8_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int8(val))
	case val >= -16 && val <= 127:
		// Write as TINY_INT
		err = binary.Write(e, binary.BigEndian, int8(val))
	case val >= 128 && val <= 32767:
		// Write as INT_16
		if _, err = e.Write([]byte{INT16_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int16(val))
	case val >= 32768 && val <= 2147483647:
		// Write as INT_32
		if _, err = e.Write([]byte{INT32_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int32(val))
	case val >= 2147483648 && val <= 9223372036854775807:
		// Write as INT_64
		if _, err = e.Write([]byte{INT64_MARKER}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	default:
		// Can't happen, but if I change the implementation for uint64
		// I want to catch the case if I missed it
		return fmt.Errorf("String too long to write: %s", val)
	}
	return err
}

// encodeFloat encodes a nil object to the stream
func (e Encoder) encodeFloat(val float64) error {
	if _, err := e.Write([]byte{FLOAT_MARKER}); err != nil {
		return err
	}
	err := binary.Write(e, binary.BigEndian, val)
	return err
}

// encodeString encodes a nil object to the stream
func (e Encoder) encodeString(val string) error {
	var err error
	bytes := []byte(val)

	length := len(bytes)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TINYSTRING_MARKER + length)}); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length >= 16 && length <= 255:
		if _, err := e.Write([]byte{STRING8_MARKER, byte(length)}); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length >= 256 && length <= 65535:
		if _, err := e.Write([]byte{STRING16_MARKER, byte(length)}); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length >= 65536 && length <= 4294967295:
		if _, err := e.Write([]byte{STRING32_MARKER, byte(length)}); err != nil {
			// encodeNil encodes a nil object to the stream
			return err
		}
		_, err = e.Write(bytes)
	default:
		// TODO: Can this happen? Does go have a limit on the length?
		// Quick google turned up nothing
		return fmt.Errorf("String too long to write: %s", val)
	}
	return err
}

// encodeList encodes a nil object to the stream
func (e Encoder) encodeList(val []interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TINYLIST_MARKER + length)}); err != nil {
			return err
		}
	case length >= 16 && length <= 255:
		if _, err := e.Write([]byte{LIST8_MARKER, byte(length)}); err != nil {
			return err
		}
	case length >= 256 && length <= 65535:
		if _, err := e.Write([]byte{LIST16_MARKER, byte(length)}); err != nil {
			return err
		}
	case length >= 65536 && length <= 4294967295:
		if _, err := e.Write([]byte{LIST32_MARKER, byte(length)}); err != nil {
			return err
		}
	default:
		// TODO: Can this happen? Does go have a limit on the length?
		return fmt.Errorf("List too long to write: %+v", val)
	}

	// Encode list values
	for _, item := range val {
		if err := e.Encode(item); err != nil {
			return err
		}
	}

	return nil
}

// encodeMap encodes a nil object to the stream
func (e Encoder) encodeMap(val map[string]interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TINYMAP_MARKER + length)}); err != nil {
			return err
		}
	case length >= 16 && length <= 255:
		if _, err := e.Write([]byte{MAP8_MARKER, byte(length)}); err != nil {
			return err
		}
	case length >= 256 && length <= 65535:
		if _, err := e.Write([]byte{MAP16_MARKER, byte(length)}); err != nil {
			return err
		}
	case length >= 65536 && length <= 4294967295:
		if _, err := e.Write([]byte{MAP32_MARKER, byte(length)}); err != nil {
			return err
		}
	default:
		// TODO: Can this happen? Does go have a limit on the length?
		return fmt.Errorf("Map too long to write: %+v", val)
	}

	// Encode list values
	for k, v := range val {
		if err := e.Encode(k); err != nil {
			return err
		}
		if err := e.Encode(v); err != nil {
			return err
		}
	}

	return nil
}

// encodeStructure encodes a nil object to the stream
func (e Encoder) encodeStructure(val structures.Structure) error {
	e.Write([]byte{byte(val.Signature())})

	fields := val.Fields()
	length := len(fields)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TINYSTRUCT_MARKER + length)}); err != nil {
			return err
		}
	case length >= 16 && length <= 255:
		if _, err := e.Write([]byte{STRUCT8_MARKER, byte(length)}); err != nil {
			return err
		}
	case length >= 256 && length <= 65535:
		if _, err := e.Write([]byte{STRUCT16_MARKER, byte(length)}); err != nil {
			return err
		}
	default:
		// TODO: Can this happen? Does go have a limit on the length?
		return fmt.Errorf("Structure too long to write: %+v", val)
	}

	for _, field := range fields {
		if err := e.Encode(field); err != nil {
			return err
		}
	}

	return nil
}

