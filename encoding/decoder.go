package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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
			numRead, err := d.buf.ReadFrom(d.r)
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
			_, err := d.buf.ReadFrom(d.r)
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
	// TODO: Keep data types or cast to int/int64?
	case int(marker) >= -16 && int(marker) <= 127:
		return int8(marker), nil
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
		size := int(marker) - int(TinySliceMarker)
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
		size := int(marker) - int(TinyMapMarker)
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

	// STRUCTURES
	case marker >= TinyStructMarker && marker <= TinyStructMarker+0x0F:
		size := int(marker) - int(TinyStructMarker)
		return d.decodeStruct(buffer, size)
	case marker == Map8Marker:
		var size int8
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeStruct(buffer, int(size))
	case marker == Map16Marker:
		var size int16
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil {
			return nil, err
		}
		return d.decodeStruct(buffer, int(size))

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

func (d Decoder) decodeStruct(buffer *bytes.Buffer, size int) (interface{}, error) {

	// TODO: How to handle size?
	signature, err := buffer.ReadByte()
	if err != nil {
		return nil, err
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
	default:
		return nil, fmt.Errorf("Unrecognized type decoding struct with signature %x", signature)
	}
}

func (d Decoder) decodeNode(buffer *bytes.Buffer) (graph.Node, error) {
	nodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return graph.Node{}, err
	}
	nodeIdentity, ok := nodeIdentityInt.(int)
	if !ok {
		return graph.Node{}, fmt.Errorf("Expected: Node Identity int, but got %T %+v", nodeIdentityInt, nodeIdentityInt)
	}

	labelInt, err := d.decode(buffer)
	if err != nil {
		return graph.Node{}, err
	}
	labelIntSlice, ok := labelInt.([]interface{})
	if !ok {
		return graph.Node{}, fmt.Errorf("Expected: Labels []string, but got %T %+v", labelInt, labelInt)
	}
	labels, err := sliceInterfaceToString(labelIntSlice)
	if err != nil {
		return graph.Node{}, err
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return graph.Node{}, err
	}
	properties, ok := propertiesInt.(map[string]interface{})
	if !ok {
		return graph.Node{}, fmt.Errorf("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return graph.Node{
		NodeIdentity: nodeIdentity,
		Labels:       labels,
		Properties:   properties,
	}, nil

}

func (d Decoder) decodeRelationship(buffer *bytes.Buffer) (graph.Relationship, error) {
	relIdentityInt, err := d.decode(buffer)
	if err != nil {
		return graph.Relationship{}, err
	}
	relIdentity, ok := relIdentityInt.(int)
	if !ok {
		return graph.Relationship{}, fmt.Errorf("Expected: Rel Identity int, but got %T %+v", relIdentityInt, relIdentityInt)
	}

	startNodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return graph.Relationship{}, err
	}
	startNodeIdentity, ok := startNodeIdentityInt.(int)
	if !ok {
		return graph.Relationship{}, fmt.Errorf("Expected: Start Node Identity int, but got %T %+v", startNodeIdentityInt, startNodeIdentityInt)
	}

	endNodeIdentityInt, err := d.decode(buffer)
	if err != nil {
		return graph.Relationship{}, err
	}
	endNodeIdentity, ok := endNodeIdentityInt.(int)
	if !ok {
		return graph.Relationship{}, fmt.Errorf("Expected: End Node Identity int, but got %T %+v", endNodeIdentityInt, endNodeIdentityInt)
	}

	typeInt, err := d.decode(buffer)
	if err != nil {
		return graph.Relationship{}, err
	}
	typee, ok := typeInt.(string)
	if !ok {
		return graph.Relationship{}, fmt.Errorf("Expected: Type string, but got %T %+v", typee, typee)
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return graph.Relationship{}, err
	}
	properties, ok := propertiesInt.(map[string]interface{})
	if !ok {
		return graph.Relationship{}, fmt.Errorf("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return graph.Relationship{
		RelIdentity:       relIdentity,
		StartNodeIdentity: startNodeIdentity,
		EndNodeIdentity:   endNodeIdentity,
		Type:              typee,
		Properties:        properties,
	}, nil
}

func (d Decoder) decodePath(buffer *bytes.Buffer) (graph.Path, error) {
	nodesInt, err := d.decode(buffer)
	if err != nil {
		return graph.Path{}, err
	}
	nodesIntSlice, ok := nodesInt.([]interface{})
	if !ok {
		return graph.Path{}, fmt.Errorf("Expected: Nodes []Node, but got %T %+v", nodesInt, nodesInt)
	}
	nodes, err := sliceInterfaceToNode(nodesIntSlice)
	if err != nil {
		return graph.Path{}, err
	}

	relsInt, err := d.decode(buffer)
	if err != nil {
		return graph.Path{}, err
	}
	relsIntSlice, ok := relsInt.([]interface{})
	if !ok {
		return graph.Path{}, fmt.Errorf("Expected: Relationships []Relationship, but got %T %+v", relsInt, relsInt)
	}
	rels, err := sliceInterfaceToRelationship(relsIntSlice)
	if err != nil {
		return graph.Path{}, err
	}

	seqInt, err := d.decode(buffer)
	if err != nil {
		return graph.Path{}, err
	}
	seqIntSlice, ok := seqInt.([]interface{})
	if !ok {
		return graph.Path{}, fmt.Errorf("Expected: Sequence []int, but got %T %+v", seqInt, seqInt)
	}
	seq, err := sliceInterfaceToInt(seqIntSlice)
	if err != nil {
		return graph.Path{}, err
	}

	return graph.Path{
		Nodes:         nodes,
		Relationships: rels,
		Sequence:      seq,
	}, nil
}

func (d Decoder) decodeUnboundRelationship(buffer *bytes.Buffer) (graph.UnboundRelationship, error) {
	relIdentityInt, err := d.decode(buffer)
	if err != nil {
		return graph.UnboundRelationship{}, err
	}
	relIdentity, ok := relIdentityInt.(int)
	if !ok {
		return graph.UnboundRelationship{}, fmt.Errorf("Expected: Rel Identity int, but got %T %+v", relIdentityInt, relIdentityInt)
	}

	typeInt, err := d.decode(buffer)
	if err != nil {
		return graph.UnboundRelationship{}, err
	}
	typee, ok := typeInt.(string)
	if !ok {
		return graph.UnboundRelationship{}, fmt.Errorf("Expected: Type string, but got %T %+v", typee, typee)
	}

	propertiesInt, err := d.decode(buffer)
	if err != nil {
		return graph.UnboundRelationship{}, err
	}
	properties, ok := propertiesInt.(map[string]interface{})
	if !ok {
		return graph.UnboundRelationship{}, fmt.Errorf("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return graph.UnboundRelationship{
		RelIdentity: relIdentity,
		Type:        typee,
		Properties:  properties,
	}, nil
}

func (d Decoder) decodeRecordMessage(buffer *bytes.Buffer) (messages.RecordMessage, error) {
	fieldsInt, err := d.decode(buffer)
	if err != nil {
		return messages.RecordMessage{}, err
	}
	fields, ok := fieldsInt.([]interface{})
	if !ok {
		return messages.RecordMessage{}, fmt.Errorf("Expected: Fields []interface{}, but got %T %+v", fieldsInt, fieldsInt)
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
		return messages.FailureMessage{}, fmt.Errorf("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewFailureMessage(metadata), nil
}

func (d Decoder) decodeIgnoredMessage(buffer *bytes.Buffer) (messages.IgnoredMessage, error) {
	metadataInt, err := d.decode(buffer)
	if err != nil {
		return messages.IgnoredMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.IgnoredMessage{}, fmt.Errorf("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewIgnoredMessage(metadata), nil
}

func (d Decoder) decodeSuccessMessage(buffer *bytes.Buffer) (messages.SuccessMessage, error) {
	metadataInt, err := d.decode(buffer)
	if err != nil {
		return messages.SuccessMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.SuccessMessage{}, fmt.Errorf("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewSuccessMessage(metadata), nil
}
