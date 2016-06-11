package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Decoder decodes a message from the bolt protocol stream
// Attempts to support all builtin golang types, when it can be confidently
// mapped to a data type from: http://alpha.neohq.net/docs/server-manual/bolt-serialization.html#bolt-packstream-structures
// (version v3.1.0-M02 at the time of writing this.
//
// Maps and Slices are a special case, where only
// map[string]interface{} and []interface{} are supported.
// The interface for maps and slices may be more permissive in the future.
type Decoder struct {
	reader io.Reader
	buf    *bytes.Buffer
}

// NewDecoder Creates a new Decoder object
func NewDecoder(r io.Reader) Decoder {
	return Decoder{
		reader: r,
		buf:    &bytes.Buffer{},
	}
}

// Read out the object bytes to decode
func (d Decoder) read(p []byte) (int, error) {
	// TODO: Reset on Error? Close on error?

	// TODO: This implementation currently reads all the chunks
	// right away.  Could make this so that it starts
	// processing the first chunk, then re-enters this
	// function to get the next chunk until the end is reached
	output := &bytes.Buffer{}
	for {
		// First read enough to get the chunk header
		if d.buf.Len() < 2 {
			numRead, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				// TODO: Should probably not downcast
				return int(numRead), err
			} else if d.buf.Len() < 2 {
				continue
			}
		}

		// Chunk header contains length of current message
		messageLen := uint16(binary.BigEndian.Uint64(d.buf.Next(2)))
		if messageLen == 0 {
			if d.buf.Len() > 0 {
				return 0, fmt.Errorf("Data left in read buffer!")
			}
			// If the length is 0, the chunk is done.
			return output.Read(p)

		}

		// Read from the chunk until we get the desired message length
		for d.buf.Len() < int(messageLen) {
			_, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				return 0, err
			}
		}

		// Store message part into buffer
		output.Write(d.buf.Next(int(messageLen)))
	}
}

// Decode decodes the stream to an object
func (d Decoder) Decode() (interface{}, error) {
	var data []byte
	if _, err := d.read(data); err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(data)

	return d.decode(buffer)
}

func (d Decoder) decode(buffer *bytes.Buffer) (interface{}, error) {

	marker, err := buffer.ReadByte()
	if err != nil {
		return nil, err
	}

	switch {

	// NIL
	case marker == NilMarker:
		return nil, nil

	// BOOL
	case marker == TrueMarker:
		return true, nil
	case marker == FalseMarker:
		return false, nil

	// INT
	case int(marker) >= -16 && int(marker) <= 127:
		return int64(marker), nil
	case marker == Int8Marker:
		var out int8
		err := binary.Read(buffer, binary.BigEndian, &out)
		return out, err
	case marker == Int16Marker:
		var out int16
		err := binary.Read(buffer, binary.BigEndian, &out)
		return out, err
	case marker == Int32Marker:
		var out int32
		err := binary.Read(buffer, binary.BigEndian, &out)
		return out, err
	case marker == Int64Marker:
		var out int64
		err := binary.Read(buffer, binary.BigEndian, &out)
		return out, err

	// FLOAT
	case marker == FloatMarker:
		var out float64
		err := binary.Read(buffer, binary.BigEndian, &out)
		return out, err

	// STRING
	case marker >= TinyStringMarker && marker <= TinyStringMarker+0x0F:
		size := int(marker) - int(TinyStringMarker)
		if size == 0 {
			return "", nil
		}
		return string(buffer.Next(size)), nil
	case marker == String8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return string(buffer.Next(int(size))), nil
	case marker == String16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return string(buffer.Next(int(size))), nil
	case marker == String32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return string(buffer.Next(int(size))), nil

	// SLICE
	case marker >= TinySliceMarker && marker <= TinySliceMarker+0x0F:
		size := int(marker) - int(TinyStringMarker)
		return d.decodeSlice(buffer, size)
	case marker == Slice8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buffer, int(size))
	case marker == Slice16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buffer, int(size))
	case marker == Slice32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeSlice(buffer, int(size))

	// MAP
	case marker >= TinyMapMarker && marker <= TinyMapMarker+0x0F:
		size := int(marker) - int(TinyStringMarker)
		return d.decodeMap(buffer, size)
	case marker == Map8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buffer, int(size))
	case marker == Map16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buffer, int(size))
	case marker == Map32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeMap(buffer, int(size))

	default:
		return nil, fmt.Errorf("Unrecognized marker byte!: %x", marker)
	}

}

func (d Decoder) decodeSlice(buffer *bytes.Buffer, size int) ([]interface{}, error) {
	// TODO: support other data types?
	slice := make([]interface{}, size)
	for i := 0; i < size; i++ {
		item, err := d.decode(buffer)
		if err != nil {
			return nil, err
		}
		slice[i] = item
	}

	return slice, nil
}

func (d Decoder) decodeMap(buffer *bytes.Buffer, size int) (map[string]interface{}, error) {
	// TODO: support other data types? or map[interface{}]interface{}?
	mapp := make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		keyInt, err := d.decode(buffer)
		if err != nil {
			return nil, err
		}
		val, err := d.decode(buffer)
		if err != nil {
			return nil, err
		}

		key, ok := keyInt.(string)
		if !ok {
			return nil, fmt.Errorf("Unexpected key type: %T with value %+v", keyInt, keyInt)
		}
		mapp[key] = val
	}

	return mapp, nil
}
