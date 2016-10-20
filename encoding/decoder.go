package encoding

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/graph"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

type reader interface {
	io.Reader
	io.ByteReader
}

type byteReader struct {
	io.Reader
	b []byte
}

func (b *byteReader) ReadByte() (byte, error) {
	_, err := io.ReadFull(b.Reader, b.b[0:1])
	return b.b[0], err
}

type boltReader struct {
	r      reader
	length uint16
}

func (b *boltReader) next() error {
	err := binary.Read(b.r, binary.BigEndian, &b.length)
	if err != nil {
		return err
	}
	if b.length == 0 {
		return io.EOF
	}
	return nil
}

func (b *boltReader) Read(p []byte) (n int, err error) {
	if b.length <= 0 {
		err = b.next()
		if err != nil {
			return 0, err
		}
	}
	if len(p) > int(b.length) {
		p = p[0:b.length]
	}
	n, err = b.r.Read(p)
	b.length -= uint16(n)
	return n, err
}

func (b *boltReader) ReadByte() (c byte, err error) {
	if b.length <= 0 {
		err = b.next()
		if err != nil {
			return 0, err
		}
	}
	c, err = b.r.ReadByte()
	if err != nil {
		return 0, err
	}
	b.length--
	return c, err
}

// Decoder decodes a message from the bolt protocol stream. It attempts to
// support all builtin golang types, when it can be confidently mapped to a data
// type from:
// http://alpha.neohq.net/docs/server-manual/bolt-serialization.html#bolt-packstream-structures
// (version v3.1.0-M02 at the time of writing this.
//
// Maps and Slices are a special case, where only map[string]interface{} and
// []interface{} are supported. The interface for maps and slices may be more
// permissive in the future.
type Decoder struct {
	r   *boltReader
	buf []byte
}

// NewDecoder creates a new Decoder object
func NewDecoder(r io.Reader) *Decoder {
	rr, ok := r.(reader)
	if ok {
		return &Decoder{r: &boltReader{r: rr}}
	}
	return &Decoder{r: &boltReader{r: &byteReader{Reader: r, b: []byte{0}}}}
}

// Unmarshal is used to marshal an object to the bolt interface encoded bytes
func Unmarshal(b []byte) (interface{}, error) {
	return NewDecoder(bytes.NewReader(b)).Decode()
}

// Decode decodes the stream to an object.
func (d *Decoder) Decode() (interface{}, error) {
	v, err := d.decode()
	if err != nil {
		return nil, err
	}
	var eof uint16
	err = d.read(&eof)
	if eof != 0 {
		return nil, errors.New("invalid eof")
	}
	return v, nil
}

func (d *Decoder) read(v interface{}) error {
	return binary.Read(d.r, binary.BigEndian, v)
}

func (d *Decoder) decode() (interface{}, error) {
	marker, err := d.r.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "Error reading marker")
	}

	// Basic nil, true, and false.
	switch marker {
	case NilMarker:
		return nil, nil
	case TrueMarker:
		return true, nil
	case FalseMarker:
		return false, nil
	}

	// Sized numbers.
	switch marker {
	default:
		if int8(marker) >= -16 && int8(marker) <= 127 {
			return int64(int8(marker)), nil
		}
	case Int8Marker:
		var out int8
		err := d.read(&out)
		return int64(out), err
	case Int16Marker:
		var out int16
		err := d.read(&out)
		return int64(out), err
	case Int32Marker:
		var out int32
		err := d.read(&out)
		return int64(out), err
	case Int64Marker:
		var out int64
		err := d.read(&out)
		return int64(out), err
	case FloatMarker:
		var out float64
		err := d.read(&out)
		return out, err
	}

	// Strings
	switch marker {
	default:
		if marker >= TinyStringMarker && marker <= TinyStringMarker+0x0F {
			return d.decodeString(int(marker) - TinyStringMarker)
		}
	case String8Marker:
		var out int8
		err := d.read(&out)
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(out))
	case String16Marker:
		var out int16
		err := d.read(&out)
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(out))
	case String32Marker:
		var out int32
		err := d.read(&out)
		if err != nil {
			return nil, err
		}
		return d.decodeString(int(out))
	}

	// Slices
	switch marker {
	default:
		if marker >= TinySliceMarker && marker <= TinySliceMarker+0x0F {
			return d.decodeSlice(int(marker) - TinySliceMarker)
		}
	case Slice8Marker:
		var size int8
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeSlice(int(size))
	case Slice16Marker:
		var size int16
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeSlice(int(size))
	case Slice32Marker:
		var size int32
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeSlice(int(size))
	}

	// Maps
	switch marker {
	default:
		if marker >= TinyMapMarker && marker <= TinyMapMarker+0x0F {
			return d.decodeMap(int(marker) - TinyMapMarker)
		}
	case Map8Marker:
		var size int8
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeMap(int(size))
	case Map16Marker:
		var size int16
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeMap(int(size))
	case Map32Marker:
		var size int32
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeMap(int(size))
	}

	// Structures
	switch marker {
	default:
		if marker >= TinyStructMarker && marker <= TinyStructMarker+0x0F {
			return d.decodeStruct()
		}
	case Struct8Marker:
		var size int8
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeStruct()
	case Struct16Marker:
		var size int16
		err := d.read(&size)
		if err != nil {
			return nil, err
		}
		return d.decodeStruct()
	}

	return nil, errors.New("Unrecognized marker byte: %x", marker)
}

func (d *Decoder) decodeString(size int) (string, error) {
	if size == 0 {
		return "", nil
	}
	if size <= cap(d.buf) {
		d.buf = d.buf[0:size]
	} else {
		d.buf = make([]byte, size)
	}
	_, err := io.ReadFull(d.r, d.buf)
	if err != nil {
		return "", err
	}
	return string(d.buf), nil
}

func (d *Decoder) decodeSlice(size int) ([]interface{}, error) {
	slice := make([]interface{}, size)
	for i := 0; i < size; i++ {
		item, err := d.decode()
		if err != nil {
			return nil, err
		}
		slice[i] = item
	}
	return slice, nil
}

func (d *Decoder) decodeMap(size int) (map[string]interface{}, error) {
	mapp := make(map[string]interface{}, size)
	for i := 0; i < size; i++ {
		keyInt, err := d.decode()
		if err != nil {
			return nil, err
		}

		val, err := d.decode()
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

func (d *Decoder) decodeStruct() (interface{}, error) {
	signature, err := d.r.ReadByte()
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred reading struct signature byte")
	}

	switch signature {
	case graph.NodeSignature:
		return d.decodeNode()
	case graph.RelationshipSignature:
		return d.decodeRelationship()
	case graph.PathSignature:
		return d.decodePath()
	case graph.UnboundRelationshipSignature:
		return d.decodeUnboundRelationship()
	case messages.RecordMessageSignature:
		return d.decodeRecordMessage()
	case messages.FailureMessageSignature:
		return d.decodeFailureMessage()
	case messages.IgnoredMessageSignature:
		return d.decodeIgnoredMessage()
	case messages.SuccessMessageSignature:
		return d.decodeSuccessMessage()
	case messages.AckFailureMessageSignature:
		return d.decodeAckFailureMessage()
	case messages.DiscardAllMessageSignature:
		return d.decodeDiscardAllMessage()
	case messages.PullAllMessageSignature:
		return d.decodePullAllMessage()
	case messages.ResetMessageSignature:
		return d.decodeResetMessage()
	default:
		return nil, errors.New("Unrecognized type decoding struct with signature %x", signature)
	}
}

func (d *Decoder) decodeNode() (graph.Node, error) {
	var node graph.Node
	nodeIdentityInt, err := d.decode()
	if err != nil {
		return node, err
	}
	var ok bool
	node.NodeIdentity, ok = nodeIdentityInt.(int64)
	if !ok {
		return node, errors.New("unexpected type")
	}

	labelInt, err := d.decode()
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

	propertiesInt, err := d.decode()
	if err != nil {
		return node, err
	}
	node.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return node, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}
	return node, nil
}

func (d *Decoder) decodeRelationship() (graph.Relationship, error) {
	var rel graph.Relationship

	relIdentityInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.RelIdentity = relIdentityInt.(int64)

	startNodeIdentityInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.StartNodeIdentity = startNodeIdentityInt.(int64)

	endNodeIdentityInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.EndNodeIdentity = endNodeIdentityInt.(int64)

	var ok bool
	typeInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.Type, ok = typeInt.(string)
	if !ok {
		return rel, errors.New("Expected: Type string, but got %T %+v", typeInt, typeInt)
	}

	propertiesInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return rel, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}
	return rel, nil
}

func (d *Decoder) decodePath() (graph.Path, error) {
	var path graph.Path

	nodesInt, err := d.decode()
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

	relsInt, err := d.decode()
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

	seqInt, err := d.decode()
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

func (d *Decoder) decodeUnboundRelationship() (graph.UnboundRelationship, error) {
	var rel graph.UnboundRelationship

	relIdentityInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	var ok bool
	rel.RelIdentity, ok = relIdentityInt.(int64)
	if !ok {
		return rel, errors.New("expected int64, got %T", relIdentityInt)
	}

	typeInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.Type, ok = typeInt.(string)
	if !ok {
		return rel, errors.New("Expected: Type string, but got %T %+v", typeInt, typeInt)
	}

	propertiesInt, err := d.decode()
	if err != nil {
		return rel, err
	}
	rel.Properties, ok = propertiesInt.(map[string]interface{})
	if !ok {
		return rel, errors.New("Expected: Properties map[string]interface{}, but got %T %+v", propertiesInt, propertiesInt)
	}

	return rel, nil
}

func (d *Decoder) decodeRecordMessage() (messages.RecordMessage, error) {
	fieldsInt, err := d.decode()
	if err != nil {
		return messages.RecordMessage{}, err
	}
	fields, ok := fieldsInt.([]interface{})
	if !ok {
		return messages.RecordMessage{}, errors.New("Expected: Fields []interface{}, but got %T %+v", fieldsInt, fieldsInt)
	}

	return messages.NewRecordMessage(fields), nil
}

func (d *Decoder) decodeFailureMessage() (messages.FailureMessage, error) {
	metadataInt, err := d.decode()
	if err != nil {
		return messages.FailureMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.FailureMessage{}, errors.New("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewFailureMessage(metadata), nil
}

func (d *Decoder) decodeIgnoredMessage() (messages.IgnoredMessage, error) {
	return messages.NewIgnoredMessage(), nil
}

func (d *Decoder) decodeSuccessMessage() (messages.SuccessMessage, error) {
	metadataInt, err := d.decode()
	if err != nil {
		return messages.SuccessMessage{}, err
	}
	metadata, ok := metadataInt.(map[string]interface{})
	if !ok {
		return messages.SuccessMessage{}, errors.New("Expected: Metadata map[string]interface{}, but got %T %+v", metadataInt, metadataInt)
	}

	return messages.NewSuccessMessage(metadata), nil
}

func (d *Decoder) decodeAckFailureMessage() (messages.AckFailureMessage, error) {
	return messages.NewAckFailureMessage(), nil
}

func (d *Decoder) decodeDiscardAllMessage() (messages.DiscardAllMessage, error) {
	return messages.NewDiscardAllMessage(), nil
}

func (d *Decoder) decodePullAllMessage() (messages.PullAllMessage, error) {
	return messages.NewPullAllMessage(), nil
}

func (d *Decoder) decodeResetMessage() (messages.ResetMessage, error) {
	return messages.NewResetMessage(), nil
}
