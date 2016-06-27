package graph

const (
	// PathSignature is the signature byte for a Path object
	PathSignature = 0x50
)

// Path Represents a Path structure
type Path struct {
	Nodes         []Node
	Relationships []UnboundRelationship
	Sequence      []int
}

// Signature gets the signature byte for the struct
func (p Path) Signature() int {
	return PathSignature
}

// AllFields gets the fields to encode for the struct
func (p Path) AllFields() []interface{} {
	nodes := make([]interface{}, len(p.Nodes))
	for i, node := range p.Nodes {
		nodes[i] = node
	}
	relationships := make([]interface{}, len(p.Relationships))
	for i, relationship := range p.Relationships {
		relationships[i] = relationship
	}
	sequences := make([]interface{}, len(p.Sequence))
	for i, sequence := range p.Sequence {
		sequences[i] = sequence
	}
	return []interface{}{nodes, relationships, sequences}
}
