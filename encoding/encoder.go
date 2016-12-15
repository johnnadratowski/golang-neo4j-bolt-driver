package encoding

import (
	"encoding/binary"
	"io"
	"math"

	"bytes"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures"
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

var (
	// EndMessage is the data to send to end a message
	EndMessage = []byte{byte(0x00), byte(0x00)}
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
	w         io.Writer
	buf       *bytes.Buffer
	chunkSize uint16
}

// NewEncoder Creates a new Encoder object
func NewEncoder(w io.Writer, chunkSize uint16) Encoder {
	return Encoder{
		w:         w,
		buf:       &bytes.Buffer{},
		chunkSize: chunkSize,
	}
}

// Marshal is used to marshal an object to the bolt interface encoded bytes
func Marshal(v interface{}) ([]byte, error) {
	x := &bytes.Buffer{}
	err := NewEncoder(x, math.MaxUint16).Encode(v)
	return x.Bytes(), err
}

// write writes to the writer.  Buffers the writes using chunkSize.
func (e Encoder) Write(p []byte) (n int, err error) {

	n, err = e.buf.Write(p)
	if err != nil {
		err = errors.Wrap(err, "An error occurred writing to encoder temp buffer")
		return n, err
	}

	length := e.buf.Len()
	for length >= int(e.chunkSize) {
		if err := binary.Write(e.w, binary.BigEndian, e.chunkSize); err != nil {
			return 0, errors.Wrap(err, "An error occured writing chunksize")
		}

		numWritten, err := e.w.Write(e.buf.Next(int(e.chunkSize)))
		if err != nil {
			err = errors.Wrap(err, "An error occured writing a chunk")
		}

		return numWritten, err
	}

	return n, nil
}

// flush finishes the encoding stream by flushing it to the writer
func (e Encoder) flush() error {
	length := e.buf.Len()
	if length > 0 {
		if err := binary.Write(e.w, binary.BigEndian, uint16(length)); err != nil {
			return errors.Wrap(err, "An error occured writing length bytes during flush")
		}

		if _, err := e.buf.WriteTo(e.w); err != nil {
			return errors.Wrap(err, "An error occured writing message bytes during flush")
		}
	}

	_, err := e.w.Write(EndMessage)
	if err != nil {
		return errors.Wrap(err, "An error occurred ending encoding message")
	}
	e.buf.Reset()

	return nil
}

// Encode encodes an object to the stream
func (e Encoder) Encode(iVal interface{}) error {

	err := e.encode(iVal)
	if err != nil {
		return err
	}

	// Whatever is left in the buffer for the chunk at the end, write it out
	return e.flush()
}

// Encode encodes an object to the stream
func (e Encoder) encode(iVal interface{}) error {

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
		err = e.encodeInt(val)
	case uint:
		err = e.encodeInt(int64(val))
	case uint8:
		err = e.encodeInt(int64(val))
	case uint16:
		err = e.encodeInt(int64(val))
	case uint32:
		err = e.encodeInt(int64(val))
	case uint64:
		if val > math.MaxInt64 {
			return errors.New("Integer too big: %d. Max integer supported: %d", val, int64(math.MaxInt64))
		}
		err = e.encodeInt(int64(val))
	case float32:
		err = e.encodeFloat(float64(val))
	case float64:
		err = e.encodeFloat(val)
	case string:
		err = e.encodeString(val)
	case []interface{}:
		err = e.encodeSlice(val)
	case map[string]interface{}:
		err = e.encodeMap(val)
	case structures.Structure:
		err = e.encodeStructure(val)
	default:
		return errors.New("Unrecognized type when encoding data for Bolt transport: %T %+v", val, val)
	}

	return err
}

func (e Encoder) encodeNil() error {
	_, err := e.Write([]byte{NilMarker})
	return err
}

func (e Encoder) encodeBool(val bool) error {
	var err error
	if val {
		_, err = e.Write([]byte{TrueMarker})
	} else {
		_, err = e.Write([]byte{FalseMarker})
	}
	return err
}

func (e Encoder) encodeInt(val int64) error {
	var err error
	switch {
	case val >= math.MinInt64 && val < math.MinInt32:
		// Write as INT_64
		if _, err = e.Write([]byte{Int64Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	case val >= math.MinInt32 && val < math.MinInt16:
		// Write as INT_32
		if _, err = e.Write([]byte{Int32Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int32(val))
	case val >= math.MinInt16 && val < math.MinInt8:
		// Write as INT_16
		if _, err = e.Write([]byte{Int16Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int16(val))
	case val >= math.MinInt8 && val < -16:
		// Write as INT_8
		if _, err = e.Write([]byte{Int8Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int8(val))
	case val >= -16 && val <= math.MaxInt8:
		// Write as TINY_INT
		err = binary.Write(e, binary.BigEndian, int8(val))
	case val > math.MaxInt8 && val <= math.MaxInt16:
		// Write as INT_16
		if _, err = e.Write([]byte{Int16Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int16(val))
	case val > math.MaxInt16 && val <= math.MaxInt32:
		// Write as INT_32
		if _, err = e.Write([]byte{Int32Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, int32(val))
	case val > math.MaxInt32 && val <= math.MaxInt64:
		// Write as INT_64
		if _, err = e.Write([]byte{Int64Marker}); err != nil {
			return err
		}
		err = binary.Write(e, binary.BigEndian, val)
	default:
		return errors.New("Int too long to write: %d", val)
	}
	if err != nil {
		return errors.Wrap(err, "An error occured writing an int to bolt")
	}
	return err
}

func (e Encoder) encodeFloat(val float64) error {
	if _, err := e.Write([]byte{FloatMarker}); err != nil {
		return err
	}

	err := binary.Write(e, binary.BigEndian, val)
	if err != nil {
		return errors.Wrap(err, "An error occured writing a float to bolt")
	}

	return err
}

func (e Encoder) encodeString(val string) error {
	var err error
	bytes := []byte(val)

	length := len(bytes)
	switch {
	case length <= 15:
		if _, err = e.Write([]byte{byte(TinyStringMarker + length)}); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > 15 && length <= math.MaxUint8:
		if _, err = e.Write([]byte{String8Marker}); err != nil {
			return err
		}
		if err = binary.Write(e, binary.BigEndian, int8(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if _, err = e.Write([]byte{String16Marker}); err != nil {
			return err
		}
		if err = binary.Write(e, binary.BigEndian, int16(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	case length > math.MaxUint16 && int64(length) <= math.MaxUint32:
		if _, err = e.Write([]byte{String32Marker}); err != nil {
			return err
		}
		if err = binary.Write(e, binary.BigEndian, int32(length)); err != nil {
			return err
		}
		_, err = e.Write(bytes)
	default:
		return errors.New("String too long to write: %s", val)
	}
	return err
}

func (e Encoder) encodeSlice(val []interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinySliceMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if _, err := e.Write([]byte{Slice8Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if _, err := e.Write([]byte{Slice16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int16(length)); err != nil {
			return err
		}
	case length >= math.MaxUint16 && int64(length) <= math.MaxUint32:
		if _, err := e.Write([]byte{Slice32Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int32(length)); err != nil {
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

func (e Encoder) encodeMap(val map[string]interface{}) error {
	length := len(val)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyMapMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if _, err := e.Write([]byte{Map8Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if _, err := e.Write([]byte{Map16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int16(length)); err != nil {
			return err
		}
	case length >= math.MaxUint16 && int64(length) <= math.MaxUint32:
		if _, err := e.Write([]byte{Map32Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int32(length)); err != nil {
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

func (e Encoder) encodeStructure(val structures.Structure) error {

	fields := val.AllFields()
	length := len(fields)
	switch {
	case length <= 15:
		if _, err := e.Write([]byte{byte(TinyStructMarker + length)}); err != nil {
			return err
		}
	case length > 15 && length <= math.MaxUint8:
		if _, err := e.Write([]byte{Struct8Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int8(length)); err != nil {
			return err
		}
	case length > math.MaxUint8 && length <= math.MaxUint16:
		if _, err := e.Write([]byte{Struct16Marker}); err != nil {
			return err
		}
		if err := binary.Write(e, binary.BigEndian, int16(length)); err != nil {
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
