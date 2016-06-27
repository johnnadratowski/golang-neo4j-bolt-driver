package graph

const (
	// UnboundRelationshipSignature is the signature byte for a UnboundRelationship object
	UnboundRelationshipSignature = 0x72
)

// UnboundRelationship Represents a UnboundRelationship structure
type UnboundRelationship struct {
	RelIdentity int64
	Type        string
	Properties  map[string]interface{}
}

// Signature gets the signature byte for the struct
func (r UnboundRelationship) Signature() int {
	return UnboundRelationshipSignature
}

// AllFields gets the fields to encode for the struct
func (r UnboundRelationship) AllFields() []interface{} {
	return []interface{}{r.RelIdentity, r.Type, r.Properties}
}
