package messages

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
//
// Implements io.ReadCloser, and chunks the encoded payload to
// the underlying writer
type Decoder struct {
	reader io.Reader
	buf    *bytes.Buffer
	closed bool
}

// NewDecoder Creates a new Decoder object
func NewDecoder(r io.Reader) Decoder {
	return Decoder{
		reader: r,
		buf:    *bytes.Buffer{},
	}
}

// Read out the object bytes to decode
func (d Decoder) Read(p []byte) (int, error) {
	if d.closed {
		return 0, fmt.Errorf("Decoder already closed")
	}

	// TODO: Reset on Error? Close on error?
	output := *bytes.Buffer{}
	for {
		// First read enough to get the chunk header
		if d.buf.Len() < 2 {
			numRead, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				return numRead, err
			} else if d.buf.Len() < 2 {
				continue
			}
		}

		// Chunk header contains length of current message
		messageLen := uint16(binary.BigEndian.Uint64([]byte{d.buf.ReadByte(), d.buf.ReadByte()}))
		if messageLen == 0 {
			if d.buf.Len() > 0 {
				return 0, fmt.Errorf("Data left in read buffer!")
			}
			// If the length is 0, the chunk is done.
			return output.Read(p)

		}

		// Read from the chunk until we get the desired message length
		for d.buf.Len() < messageLen {
			_, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				return 0, err
			}
		}

		// Store message part into buffer
		output.Write(d.buf.Next(messageLen))
	}
}

// Close closes and flushes the decoder. This is important to call as it will
// flush the remaining data in the chunk.
func (d Decoder) Close() error {
	// TODO: Reset on Error? Close on error?
	d.closed = true
	return nil
}

// Decode decodes the stream to an object
func (d Decoder) Decode(iVal interface{}) error {
	return nil
}
