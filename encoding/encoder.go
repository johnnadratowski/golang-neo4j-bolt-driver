package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures"
)

const (
	// NilMarker represents the encoding marker byte for a nil object
	NilMarker = 0xC0

	// TrueMarker represents the encoding marker byte for a true boolean object
	TrueMarker = 0xC3
	// FalseMarker represents the encoding marker byte for a false boolean object
	FalseMarker = 0xC2

	// Int8Marker represents the encoding marker byte for a int8 object
	Int8Marker = 0xC8
	// Int16Marker represents the encoding marker byte for a int16 object
	Int16Marker = 0xC9
	// Int32Marker represents the encoding marker byte for a int32 object
	Int32Marker = 0xCA
	// Int64Marker represents the encoding marker byte for a int64 object
	Int64Marker = 0xCB

	// FloatMarker represents the encoding marker byte for a float32/64 object
	FloatMarker = 0xC1

	// TinyStringMarker represents the encoding marker byte for a string object
	TinyStringMarker = 0x80
	// String8Marker represents the encoding marker byte for a string object
	String8Marker = 0xD0
	// String16Marker represents the encoding marker byte for a string object
	String16Marker = 0xD1
	// String32Marker represents the encoding marker byte for a string object
	String32Marker = 0xD2

	// TinySliceMarker represents the encoding marker byte for a slice object
	TinySliceMarker = 0x90
	// Slice8Marker represents the encoding marker byte for a slice object
	Slice8Marker = 0xD4
	// Slice16Marker represents the encoding marker byte for a slice object
	Slice16Marker = 0xD5
	// Slice32Marker represents the encoding marker byte for a slice object
	Slice32Marker = 0xD6

	// TinyMapMarker represents the encoding marker byte for a map object
	TinyMapMarker = 0xA0
	// Map8Marker represents the encoding marker byte for a map object
	Map8Marker = 0xD8
	// Map16Marker represents the encoding marker byte for a map object
	Map16Marker = 0xD9
	// Map32Marker represents the encoding marker byte for a map object
	Map32Marker = 0xDA

	// TinyStructMarker represents the encoding marker byte for a struct object
	TinyStructMarker = 0xB0
	// Struct8Marker represents the encoding marker byte for a struct object
	Struct8Marker = 0xDC
	// Struct16Marker represents the encoding marker byte for a struct object
	Struct16Marker = 0xDD
)

// EndMessage is the data to send to end a message
var EndMessage = []byte{byte(0x00), byte(0x00)}

// Encoder encodes objects of different types to the given stream.
// Attempts to support all builtin golang types, when it can be confidently
// mapped to a data type from: http://alpha.neohq.net/docs/server-manual/bolt-serialization.html#bolt-packstream-structures
// (version v3.1.0-M02 at the time of writing this.
//
// Maps and Slices are a special case, where only
// map[string]interface{} and []interface{} are supported.
// The interface for maps and slices may be more permissive in the future.
type Encoder struct {
	w    io.Writer
	buf  []byte
	n    int
	size int
}

// NewEncoder initializes a new Encoder with the provided chunk size.
func NewEncoder(w io.Writer, size uint16) *Encoder {
	return &Encoder{w: w, buf: make([]byte, size), size: int(size)}
}

// Marshal is used to marshal an object to the bolt interface encoded bytes.
func Marshal(v interface{}) ([]byte, error) {
	var b bytes.Buffer
	err := NewEncoder(&b, math.MaxUint16).Encode(v)
	return b.Bytes(), err
}

// Write writes to the writer. Writes are not necessarily written to the
// underlying Writer until Flush is called.
func (e *Encoder) Write(p []byte) (n int, err error) {
	var m int
	for n < len(p) {
		m = copy(e.buf[e.n:], p[n:])
		e.n += m
		n += m
		if e.n == e.size {
			err = e.writeChunk()
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

func (e *Encoder) writeMarker(marker uint8) error {
	e.buf[e.n] = marker
	e.n++
	if e.n == e.size {
		return e.writeChunk()
	}
	return nil
}

func (e *Encoder) write(v interface{}) error {
	return binary.Write(e, binary.BigEndian, v)
}

func (e *Encoder) Flush() error {
	err := e.writeChunk()
	if err != nil {
		return err
	}
	_, err = e.w.Write(EndMessage)
	return err
}

func (e *Encoder) writeChunk() error {
	if e.n == 0 {
		return nil
	}
	err := binary.Write(e.w, binary.BigEndian, uint16(e.n))
	if err != nil {
		return err
	}
	_, err = e.w.Write(e.buf[:e.n])
	e.n = 0
	return err
}

// Encode encodes an object to the stream
func (e *Encoder) Encode(val interface{}) error {
	err := e.encode(val)
	if err != nil {
		return err
	}
	// Whatever is left in the buffer for the chunk at the end, write it out
	return e.Flush()
}

// Encode encodes an object to the stream
func (e *Encoder) encode(val interface{}) error {
	switch val := val.(type) {
	case nil:
		return e.encodeNil()
	case bool:
		return e.encodeBool(val)
	case int:
		return e.encodeInt(int64(val))
	case int8:
		return e.encodeInt(int64(val))
	case int16:
		return e.encodeInt(int64(val))
	case int32:
		return e.encodeInt(int64(val))
	case int64:
		return e.encodeInt(val)
	case uint:
		return e.encodeInt(int64(val))
	case uint8:
		return e.encodeInt(int64(val))
	case uint16:
		return e.encodeInt(int64(val))
	case uint32:
		return e.encodeInt(int64(val))
	case uint64:
		if val > math.MaxInt64 {
			return errors.New("Integer too big: %d. Max integer supported: %d", val, math.MaxInt64)
		}
		return e.encodeInt(int64(val))
	case float32:
		return e.encodeFloat(float64(val))
	case float64:
		return e.encodeFloat(val)
	case string:
		return e.encodeString(val)
	case []interface{}:
		return e.encodeSlice(val)
	case map[string]interface{}:
		return e.encodeMap(val)
	case structures.Structure:
		return e.encodeStructure(val)
	default:
		return errors.New("Unrecognized type when encoding data for Bolt transport: %T %+v", val, val)
	}
}

func (e *Encoder) encodeNil() error {
	return e.writeMarker(NilMarker)
}

func (e *Encoder) encodeBool(val bool) (err error) {
	if val {
		return e.writeMarker(TrueMarker)
	}
	return e.writeMarker(FalseMarker)
}

func (e *Encoder) encodeInt(val int64) (err error) {
	switch {
	case val >= math.MinInt64 && val < math.MinInt32:
		// Write as INT_64
		if err = e.writeMarker(Int64Marker); err != nil {
			return err
		}
		return e.write(val)
	case val >= math.MinInt32 && val < math.MinInt16:
		// Write as INT_32
		if err = e.writeMarker(Int32Marker); err != nil {
			return err
		}
		return e.write(int32(val))
	case val >= math.MinInt16 && val < math.MinInt8:
		// Write as INT_16
		if err = e.writeMarker(Int16Marker); err != nil {
			return err
		}
		return e.write(int16(val))
	case val >= math.MinInt8 && val < -16:
		// Write as INT_8
		if err = e.writeMarker(Int8Marker); err != nil {
			return err
		}
		return e.write(int8(val))
	case val >= -16 && val <= math.MaxInt8:
		// Write as TINY_INT
		return e.write(int8(val))
	case val > math.MaxInt8 && val <= math.MaxInt16:
		// Write as INT_16
		if err = e.writeMarker(Int16Marker); err != nil {
			return err
		}
		return e.write(int16(val))
	case val > math.MaxInt16 && val <= math.MaxInt32:
		// Write as INT_32
		if err = e.writeMarker(Int32Marker); err != nil {
			return err
		}
		return e.write(int32(val))
	case val > math.MaxInt32 && val <= math.MaxInt64:
		// Write as INT_64
		if err = e.writeMarker(Int64Marker); err != nil {
			return err
		}
		return e.write(val)
	default:
		return errors.New("Int too long to write: %d", val)
	}
}

func (e *Encoder) encodeFloat(val float64) error {
	if err := e.writeMarker(FloatMarker); err != nil {
		return err
	}
	err := e.write(val)
	if err != nil {
		return errors.Wrap(err, "An error occured writing a float to bolt")
	}
	return err
}

func (e *Encoder) encodeString(val string) (err error) {
	bytes := []byte(val)

	length := len(bytes)
	switch {
	case length <= 15:
		if _, err = e.Write([]byte{byte(TinyStringMarker + length)}); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > 15 && length <= math.MaxUint8:
		if err = e.writeMarker(String8Marker); err != nil {
			return err
		}
		if err = e.write(int8(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if err = e.writeMarker(String16Marker); err != nil {
			return err
		}
		if err = e.write(int16(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > math.MaxUint16 && length <= math.MaxUint32:
		if err = e.writeMarker(String32Marker); err != nil {
			return err
		}
		if err = e.write(int32(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	default:
		return errors.New("String too long to write: %s", val)
	}
	return err
}

func (e *Encoder) encodeSlice(val []interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinySliceMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if err := e.writeMarker(Slice8Marker); err != nil {
			return err
		}
		if err := e.write(int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if err := e.writeMarker(Slice16Marker); err != nil {
			return err
		}
		if err := e.write(int16(length)); err != nil {
			return err
		}
	case length >= math.MaxUint16 && length <= math.MaxUint32:
		if err := e.writeMarker(Slice32Marker); err != nil {
			return err
		}
		if err := e.write(int32(length)); err != nil {
			return err
		}
	default:
		return errors.New("Slice too long to write: %+v", val)
	}

	// Encode Slice values
	for _, item := range val {
		if err := e.encode(item); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeMap(val map[string]interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyMapMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if err := e.writeMarker(Map8Marker); err != nil {
			return err
		}
		if err := e.write(int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if err := e.writeMarker(Map16Marker); err != nil {
			return err
		}
		if err := e.write(int16(length)); err != nil {
			return err
		}
	case length >= math.MaxUint16 && length <= math.MaxUint32:
		if err := e.writeMarker(Map32Marker); err != nil {
			return err
		}
		if err := e.write(int32(length)); err != nil {
			return err
		}
	default:
		return errors.New("Map too long to write: %+v", val)
	}

	// Encode Map values
	for k, v := range val {
		if err := e.encode(k); err != nil {
			return err
		}
		if err := e.encode(v); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeStructure(val structures.Structure) error {
	fields := val.AllFields()
	length := len(fields)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyStructMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if err := e.writeMarker(Struct8Marker); err != nil {
			return err
		}
		if err := e.write(int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if err := e.writeMarker(Struct16Marker); err != nil {
			return err
		}
		if err := e.write(int16(length)); err != nil {
			return err
		}
	default:
		return errors.New("Structure too long to write: %+v", val)
	}

	_, err := e.Write([]byte{byte(val.Signature())})
	if err != nil {
		return errors.Wrap(err, "An error occurred writing to encoder a struct field")
	}

	for _, field := range fields {
		if err := e.encode(field); err != nil {
			return errors.Wrap(err, "An error occurred encoding a struct field")
		}
	}
	return nil
}
