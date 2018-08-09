package encoding

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
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
	r   io.Reader
	buf *bytes.Buffer
}

// NewDecoder Creates a new Decoder object
func NewDecoder(r io.Reader) Decoder {
	return Decoder{
		r:   r,
		buf: &bytes.Buffer{},
	}
}

// Unmarshal is used to marshal an object to the bolt interface encoded bytes
func Unmarshal(b []byte) (interface{}, error) {
	return NewDecoder(bytes.NewBuffer(b)).Decode()
}

// Read out the object bytes to decode
func (d Decoder) read() (*bytes.Buffer, error) {
	output := &bytes.Buffer{}
	for {
		lengthBytes := make([]byte, 2)
		if numRead, err := io.ReadFull(d.r, lengthBytes); numRead != 2 {
			return nil, errors.Wrap(err, "Couldn't read expected bytes for message length. Read: %d Expected: 2.", numRead)
		}

		// Chunk header contains length of current message
		messageLen := binary.BigEndian.Uint16(lengthBytes)
		if messageLen == 0 {
			// If the length is 0, the chunk is done.
			return output, nil
		}

		data, err := d.readData(messageLen)
		if err != nil {
			return output, errors.Wrap(err, "An error occurred reading message data")
		}

		numWritten, err := output.Write(data)
		if numWritten < len(data) {
			return output, errors.New("Didn't write full data on output. Expected: %d Wrote: %d", len(data), numWritten)
		} else if err != nil {
			return output, errors.Wrap(err, "Error writing data to output")
		}
	}
}

func (d Decoder) readData(messageLen uint16) ([]byte, error) {
	output := make([]byte, messageLen)
	var totalRead uint16
	for totalRead < messageLen {
		data := make([]byte, messageLen-totalRead)
		numRead, err := d.r.Read(data)
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred reading from stream")
		} else if numRead == 0 {
			return nil, errors.Wrap(err, "Couldn't read expected bytes for message. Read: %d Expected: %d.", totalRead, messageLen)
		}

		for idx, b := range data {
			output[uint16(idx)+totalRead] = b
		}

		totalRead += uint16(numRead)
	}

	return output, nil
}

// Decode decodes the stream to an object
func (d Decoder) Decode() (interface{}, error) {
	data, err := d.read()
	if err != nil {
		return nil, err
	}

	return d.decode(data)
}

func (d Decoder) decode(buffer *bytes.Buffer) (interface{}, error) {

	marker, err := buffer.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "Error reading marker")
	}

	// Here we have to get the marker as an int to check and see
	// if it's a TINYINT
	var markerInt int8
	err = binary.Read(bytes.NewBuffer([]byte{marker}), binary.BigEndian, &markerInt)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading marker as int8 from bolt message")
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
	case markerInt >= -16 && markerInt <= 127:
		return int64(int8(marker)), nil
	case marker == Int8Marker:
		var out int8
		err := binary.Read(buffer, binary.BigEndian, &out)
		return int64(out), err
	case marker == Int16Marker:
		var out int16
		err := binary.Read(buffer, binary.BigEndian, &out)
		return int64(out), err
	case marker == Int32Marker:
		var out int32
		err := binary.Read(buffer, binary.BigEndian, &out)
		return int64(out), err
	case marker == Int64Marker:
		var out int64
		err := binary.Read(buffer, binary.BigEndian, &out)
		return int64(out), err

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
			return nil, errors.Wrap(err, "An error occurred reading string size")
		}
		return string(buffer.Next(int(size))), nil
	case marker == String16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading string size")
		}
		return string(buffer.Next(int(size))), nil
	case marker == String32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading string size")
		}
		return string(buffer.Next(int(size))), nil

	// SLICE
	case marker >= TinySliceMarker && marker <= TinySliceMarker+0x0F:
		size := int(marker) - int(TinySliceMarker)
		return d.decodeSlice(buffer, size)
	case marker == Slice8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading slice size")
		}
		return d.decodeSlice(buffer, int(size))
	case marker == Slice16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading slice size")
		}
		return d.decodeSlice(buffer, int(size))
	case marker == Slice32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading slice size")
		}
		return d.decodeSlice(buffer, int(size))

	// MAP
	case marker >= TinyMapMarker && marker <= TinyMapMarker+0x0F:
		size := int(marker) - int(TinyMapMarker)
		return d.decodeMap(buffer, size)
	case marker == Map8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading map size")
		}
		return d.decodeMap(buffer, int(size))
	case marker == Map16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading map size")
		}
		return d.decodeMap(buffer, int(size))
	case marker == Map32Marker:
		var size int32
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading map size")
		}
		return d.decodeMap(buffer, int(size))

	// STRUCTURES
	case marker >= TinyStructMarker && marker <= TinyStructMarker+0x0F:
		size := int(marker) - int(TinyStructMarker)
		return d.decodeStruct(buffer, size)
	case marker == Struct8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading struct size")
		}
		return d.decodeStruct(buffer, int(size))
	case marker == Struct16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, errors.Wrap(err, "An error occurred reading struct size")
		}
		return d.decodeStruct(buffer, int(size))

	default:
		return nil, errors.New("Unrecognized marker byte!: %x", marker)
	}

}

func (d Decoder) decodeSlice(buffer *bytes.Buffer, size int) ([]interface{}, error) {
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
			return nil, errors.New("Unexpected key type: %T with value %+v", keyInt, keyInt)
		}
		mapp[key] = val
	}

	return mapp, nil
}

func (d Decoder) decodeStruct(buffer *bytes.Buffer, size int) (interface{}, error) {

	signature, err := buffer.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred reading struct signature byte")
	}

	switch signature {
	case graph.NodeSignature:
		return d.decodeNode(buffer)
	case graph.RelationshipSignature:
		return d.decodeRelationship(buffer)
	case graph.PathSignature:
		return d.decodePath(buffer)
	case graph.UnboundRelationshipSignature:
		return d.decodeUnboundRelationship(buffer)
	case messages.RecordMessageSignature:
		return d.decodeRecordMessage(buffer)
	case messages.FailureMessageSignature:
		return d.decodeFailureMessage(buffer)
	case messages.IgnoredMessageSignature:
		return d.decodeIgnoredMessage(buffer)
	case messages.SuccessMessageSignature:
		return d.decodeSuccessMessage(buffer)
	case messages.AckFailureMessageSignature:
		return d.decodeAckFailureMessage(buffer)
	case messages.DiscardAllMessageSignature:
		return d.decodeDiscardAllMessage(buffer)
	case messages.PullAllMessageSignature:
		return d.decodePullAllMessage(buffer)
	case messages.ResetMessageSignature:
		return d.decodeResetMessage(buffer)
	default:
		return nil, errors.New("Unrecognized type decoding struct with signature %x", signature)
	}
}

func (d Decoder) decodeNode(buffer *bytes.Buffer) (graph.Node, error) {
	node := graph.Node{}

	nodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return node, err
	}
	node.NodeIdentity = nodeIdentityInt.(int64)

	labelInt, err := d.decode(buffer)
	if err != nil {
		return node, err
	}
	labelIntSlice, ok := labelInt.([]interface{})
	if !ok {
		return node, errors.New("Expected: Labels []string, but got %T %+v", labelInt, labelInt)
	}
	node.Labels, err = sliceInterfaceToString(labelIntSlice)
	if err != nil {
		return node, err
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return node, err
	}
	node.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return node, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return node, nil

}

func (d Decoder) decodeRelationship(buffer *bytes.Buffer) (graph.Relationship, error) {
	rel := graph.Relationship{}

	relIdentityInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.RelIdentity = relIdentityInt.(int64)

	startNodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.StartNodeIdentity = startNodeIdentityInt.(int64)

	endNodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.EndNodeIdentity = endNodeIdentityInt.(int64)

	var ok bool
	typeInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.Type, ok = typeInt.(string)
	if !ok {
		return rel, errors.New("Expected: Type string, but got %T %+v", typeInt, typeInt)
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return rel, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return rel, nil
}

func (d Decoder) decodePath(buffer *bytes.Buffer) (graph.Path, error) {
	path := graph.Path{}

	nodesInt, err := d.decode(buffer)
	if err != nil {
		return path, err
	}
	nodesIntSlice, ok := nodesInt.([]interface{})
	if !ok {
		return path, errors.New("Expected: Nodes []Node, but got %T %+v", nodesInt, nodesInt)
	}
	path.Nodes, err = sliceInterfaceToNode(nodesIntSlice)
	if err != nil {
		return path, err
	}

	relsInt, err := d.decode(buffer)
	if err != nil {
		return path, err
	}
	relsIntSlice, ok := relsInt.([]interface{})
	if !ok {
		return path, errors.New("Expected: Relationships []Relationship, but got %T %+v", relsInt, relsInt)
	}
	path.Relationships, err = sliceInterfaceToUnboundRelationship(relsIntSlice)
	if err != nil {
		return path, err
	}

	seqInt, err := d.decode(buffer)
	if err != nil {
		return path, err
	}
	seqIntSlice, ok := seqInt.([]interface{})
	if !ok {
		return path, errors.New("Expected: Sequence []int, but got %T %+v", seqInt, seqInt)
	}
	path.Sequence, err = sliceInterfaceToInt(seqIntSlice)

	return path, err
}

func (d Decoder) decodeUnboundRelationship(buffer *bytes.Buffer) (graph.UnboundRelationship, error) {
	rel := graph.UnboundRelationship{}

	relIdentityInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.RelIdentity = relIdentityInt.(int64)

	var ok bool
	typeInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.Type, ok = typeInt.(string)
	if !ok {
		return rel, errors.New("Expected: Type string, but got %T %+v", typeInt, typeInt)
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return rel, err
	}
	rel.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return rel, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return rel, nil
}

func (d Decoder) decodeRecordMessage(buffer *bytes.Buffer) (messages.RecordMessage, error) {
	fieldsInt, err := d.decode(buffer)
	if err != nil {
		return messages.RecordMessage{}, err
	}
	fields, ok := fieldsInt.([]interface{})
	if !ok {
		return messages.RecordMessage{}, errors.New("Expected: Fields []interface{}, but got %T %+v", fieldsInt, fieldsInt)
	}

	return messages.NewRecordMessage(fields), nil
}

func (d Decoder) decodeFailureMessage(buffer *bytes.Buffer) (messages.FailureMessage, error) {
	metadataInt, err := d.decode(buffer)
	if err != nil {
		return messages.FailureMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.FailureMessage{}, errors.New("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewFailureMessage(metadata), nil
}

func (d Decoder) decodeIgnoredMessage(buffer *bytes.Buffer) (messages.IgnoredMessage, error) {
	return messages.NewIgnoredMessage(), nil
}

func (d Decoder) decodeSuccessMessage(buffer *bytes.Buffer) (messages.SuccessMessage, error) {
	metadataInt, err := d.decode(buffer)
	if err != nil {
		return messages.SuccessMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.SuccessMessage{}, errors.New("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewSuccessMessage(metadata), nil
}

func (d Decoder) decodeAckFailureMessage(buffer *bytes.Buffer) (messages.AckFailureMessage, error) {
	return messages.NewAckFailureMessage(), nil
}

func (d Decoder) decodeDiscardAllMessage(buffer *bytes.Buffer) (messages.DiscardAllMessage, error) {
	return messages.NewDiscardAllMessage(), nil
}

func (d Decoder) decodePullAllMessage(buffer *bytes.Buffer) (messages.PullAllMessage, error) {
	return messages.NewPullAllMessage(), nil
}

func (d Decoder) decodeResetMessage(buffer *bytes.Buffer) (messages.ResetMessage, error) {
	return messages.NewResetMessage(), nil
}
