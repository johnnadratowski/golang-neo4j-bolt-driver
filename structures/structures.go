package structures

// Structure represents a Neo4J structure
type Structure interface {
	Signature() int
}

// MessageStructure represents a Neo4J message structure
type MessageStructure interface {
	Structure
	Fields() []interface{}
}
