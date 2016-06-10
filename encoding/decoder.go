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

// Read out the object bytes to decode
func (d Decoder) Read(p []byte) (int, error) {
	// TODO: Reset on Error? Close on error?
	output := *bytes.Buffer{}
	for {
		if d.buf.Len() < 2 {
			numRead, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				return numRead, err
			} else if d.buf.Len() < 2 {
				continue
			}
		}

		messageLen := uint16(binary.BigEndian.Uint64([]byte{d.buf.ReadByte(), d.buf.ReadByte()}))
		if messageLen == 0 {
			return output.Read(p)

		}

		for d.buf.Len() < messageLen {
			_, err := d.buf.ReadFrom(d.reader)
			if err != nil {
				return 0, err
			}
		}

		output.Write(d.buf.Next(messageLen))
	}
}

// Close closes and flushes the decoder. This is important to call as it will
// flush the remaining data in the chunk.
func (d Decoder) Close() error {
}

// Decode decodes the stream to an object
func (d Decoder) Decode(iVal interface{}) error {

}
