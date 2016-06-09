package messages

const (
	// PathSignature is the signature byte for a Path object
	PathSignature = 0x50
)

// Path Represents a Path structure
type Path struct {
	Nodes         []Node
	Relationships []Relationship
	Sequence      int
}

// Signature gets the signature byte for the struct
func (n Path) Signature() int {
	return PathSignature
}
