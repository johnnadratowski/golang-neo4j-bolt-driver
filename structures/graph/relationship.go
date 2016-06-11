package graph

const (
	// RelationshipSignature is the signature byte for a Relationship object
	RelationshipSignature = 0x52
)

// Relationship Represents a Relationship structure
type Relationship struct {
	RelIdentity       int
	StartNodeIdentity int
	EndNodeIdentity   int
	Type              string
	Properties        map[string]interface{}
}

// Signature gets the signature byte for the struct
func (r Relationship) Signature() int {
	return RelationshipSignature
}
