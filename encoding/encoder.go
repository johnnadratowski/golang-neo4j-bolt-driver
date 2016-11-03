package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

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
// mapped to a data type from:
// http://alpha.neohq.net/docs/server-manual/bolt-serialization.html#bolt-packstream-structures
// (version v3.1.0-M02 at the time of writing this.
//
// Maps and Slices are a special case, where only map[string]interface{} and
// []interface{} are supported. The interface for maps and slices may be more
// permissive in the future.
type Encoder struct {
	w *chunkWriter
}

const DefaultChunkSize = math.MaxUint16

// NewEncoder initializes a new Encoder with the provided chunk size.
func NewEncoder(w io.Writer) *Encoder {
	const size = DefaultChunkSize
	return &Encoder{w: &chunkWriter{w: w, buf: make([]byte, size), size: size}}
}

// SetChunkSize sets the Encoder's chunk size. It flushes any pending writes
// using the new chunk size.
func (e *Encoder) SetChunkSize(size uint16) error {
	if e.w.size == int(size) {
		return nil
	}
	e.w.size = int(size)

	// Create a new buffer if necessary.
	if e.w.size > len(e.w.buf) {
		e.w.buf = make([]byte, e.w.size)
		return nil
	}

	// Flush what we have so far if our current chunk is >= size.
	for e.w.n >= e.w.size {
		e.w.n = e.w.size
		err := e.w.writeChunk()
		if err != nil {
			return err
		}
		// Slide our buffer down.
		e.w.n = copy(e.w.buf[:e.w.size], e.w.buf[e.w.n:])
	}
	return nil
}

// Marshal is used to marshal an object to the bolt interface encoded bytes.
func Marshal(v interface{}) ([]byte, error) {
	var b bytes.Buffer
	err := NewEncoder(&b).Encode(v)
	return b.Bytes(), err
}

type chunkWriter struct {
	w    io.Writer
	buf  []byte
	n    int
	size int
}

// Write writes to the Encoder. Writes are not necessarily written to the
// underlying Writer until Flush is called.
func (w *chunkWriter) Write(p []byte) (n int, err error) {
	var m int
	for n < len(p) {
		m = copy(w.buf[w.n:], p[n:])
		w.n += m
		n += m
		if w.n == w.size {
			err = w.writeChunk()
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// Write writes a string to the Encoder. Writes are not necessarily written to
// the underlying Writer until Flush is called.
func (w *chunkWriter) WriteString(s string) (n int, err error) {
	var m int
	for n < len(s) {
		m = copy(w.buf[w.n:], s[n:])
		w.n += m
		n += m
		if w.n == w.size {
			err = w.writeChunk()
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// Flush writes the existing data to the underlying writer and then ends
// the stream.
func (w *chunkWriter) Flush() error {
	err := w.writeChunk()
	if err != nil {
		return err
	}
	_, err = w.w.Write(EndMessage)
	return err
}

func (w *chunkWriter) writeMarker(marker uint8) error {
	w.buf[w.n] = marker
	w.n++
	if w.n == w.size {
		return w.writeChunk()
	}
	return nil
}

func (w *chunkWriter) writeChunk() error {
	if w.n == 0 {
		return nil
	}
	err := binary.Write(w.w, binary.BigEndian, uint16(w.n))
	if err != nil {
		return err
	}
	_, err = w.w.Write(w.buf[:w.n])
	w.n = 0
	return err
}

func (e *Encoder) write(v interface{}) error {
	return binary.Write(e.w, binary.BigEndian, v)
}

// Encode encodes an object to the stream
func (e *Encoder) Encode(val interface{}) error {
	err := e.encode(val)
	if err != nil {
		return err
	}
	return e.w.Flush()
}

// Encode encodes an object to the stream
func (e *Encoder) encode(val interface{}) error {
	switch val := val.(type) {
	case nil:
		return e.w.writeMarker(NilMarker)
	case bool:
		if val {
			return e.w.writeMarker(TrueMarker)
		}
		return e.w.writeMarker(FalseMarker)
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
		if ^uint(0) > math.MaxUint64 && val > math.MaxInt64 {
			return fmt.Errorf("integer too big: %d. Max integer supported: %d", val, math.MaxInt64)
		}
		return e.encodeInt(int64(val))
	case uint8:
		return e.encodeInt(int64(val))
	case uint16:
		return e.encodeInt(int64(val))
	case uint32:
		return e.encodeInt(int64(val))
	case uint64:
		if val > math.MaxInt64 {
			return fmt.Errorf("integer too big: %d. Max integer supported: %d", val, math.MaxInt64)
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
		return fmt.Errorf("unrecognized type when encoding data for Bolt transport: %T %+v", val, val)
	}
}

func (e *Encoder) encodeInt(val int64) (err error) {
	switch {
	case val < math.MinInt32:
		// Write as INT_64
		if err = e.w.writeMarker(Int64Marker); err != nil {
			return err
		}
		return e.write(val)
	case val < math.MinInt16:
		// Write as INT_32
		if err = e.w.writeMarker(Int32Marker); err != nil {
			return err
		}
		return e.write(int32(val))
	case val < math.MinInt8:
		// Write as INT_16
		if err = e.w.writeMarker(Int16Marker); err != nil {
			return err
		}
		return e.write(int16(val))
	case val < -16:
		// Write as INT_8
		if err = e.w.writeMarker(Int8Marker); err != nil {
			return err
		}
		return e.write(int8(val))
	case val < math.MaxInt8:
		// Write as TINY_INT
		return e.write(int8(val))
	case val < math.MaxInt16:
		// Write as INT_16
		if err = e.w.writeMarker(Int16Marker); err != nil {
			return err
		}
		return e.write(int16(val))
	case val < math.MaxInt32:
		// Write as INT_32
		if err = e.w.writeMarker(Int32Marker); err != nil {
			return err
		}
		return e.write(int32(val))
	case val <= math.MaxInt64:
		// Write as INT_64
		if err = e.w.writeMarker(Int64Marker); err != nil {
			return err
		}
		return e.write(val)
	default:
		return fmt.Errorf("Int too long to write: %d", val)
	}
}

func (e *Encoder) encodeFloat(val float64) error {
	if err := e.w.writeMarker(FloatMarker); err != nil {
		return err
	}
	return e.write(val)
}

func (e *Encoder) encodeString(str string) (err error) {
	length := len(str)
	switch {
	case length <= 15:
		err = e.w.writeMarker(TinyStringMarker + uint8(length))
		if err != nil {
			return err
		}
		_, err = e.w.WriteString(str)
		return err
	case length <= math.MaxUint8:
		if err = e.w.writeMarker(String8Marker); err != nil {
			return err
		}
		if err = e.write(uint8(length)); err != nil {
			return err
		}
		_, err = e.w.WriteString(str)
		return err
	case length <= math.MaxUint16:
		if err = e.w.writeMarker(String16Marker); err != nil {
			return err
		}
		if err = e.write(uint16(length)); err != nil {
			return err
		}
		_, err = e.w.WriteString(str)
		return err
	case length > math.MaxUint16 && length <= math.MaxUint32:
		if err = e.w.writeMarker(String32Marker); err != nil {
			return err
		}
		if err = e.write(uint32(length)); err != nil {
			return err
		}
		_, err = e.w.WriteString(str)
		return err
	default:
		return errors.New("string too long to write")
	}
}

func (e *Encoder) encodeSlice(val []interface{}) (err error) {
	length := len(val)
	switch {
	case length <= 15:
		err = e.w.writeMarker(TinySliceMarker + uint8(length))
		if err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if err = e.w.writeMarker(Slice8Marker); err != nil {
			return err
		}
		if err = e.write(uint8(length)); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if err = e.w.writeMarker(Slice16Marker); err != nil {
			return err
		}
		if err = e.write(uint16(length)); err != nil {
			return err
		}
	case length <= math.MaxUint32:
		if err := e.w.writeMarker(Slice32Marker); err != nil {
			return err
		}
		if err = e.write(uint32(length)); err != nil {
			return err
		}
	default:
		return errors.New("slice too long to write")
	}

	// Encode Slice values
	for _, item := range val {
		err = e.encode(item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) encodeMap(val map[string]interface{}) (err error) {
	length := len(val)
	switch {
	case length <= 15:
		err = e.w.writeMarker(TinyMapMarker + uint8(length))
		if err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if err = e.w.writeMarker(Map8Marker); err != nil {
			return err
		}
		if err = e.write(uint8(length)); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if err = e.w.writeMarker(Map16Marker); err != nil {
			return err
		}
		if err = e.write(uint16(length)); err != nil {
			return err
		}
	case length <= math.MaxUint32:
		if err = e.w.writeMarker(Map32Marker); err != nil {
			return err
		}
		if err = e.write(uint32(length)); err != nil {
			return err
		}
	default:
		return errors.New("map too long to write")
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

func (e *Encoder) encodeStructure(val structures.Structure) (err error) {
	fields := val.AllFields()
	length := len(fields)
	switch {
	case length <= 15:
		err = e.w.writeMarker(TinyStructMarker + uint8(length))
		if err != nil {
			return err
		}
	case length <= math.MaxUint8:
		if err = e.w.writeMarker(Struct8Marker); err != nil {
			return err
		}
		if err = e.write(uint8(length)); err != nil {
			return err
		}
	case length <= math.MaxUint16:
		if err = e.w.writeMarker(Struct16Marker); err != nil {
			return err
		}
		if err = e.write(uint16(length)); err != nil {
			return err
		}
	default:
		return errors.New("structure too large to write")
	}

	err = e.w.writeMarker(uint8(val.Signature()))
	if err != nil {
		return err
	}

	for _, field := range fields {
		err = e.encode(field)
		if err != nil {
			return err
		}
	}
	return nil
}
