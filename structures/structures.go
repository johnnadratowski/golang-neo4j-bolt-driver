package structures

// Structure represents a Neo4J structure
type Structure interface {
	Signature() int
	AllFields() []interface{}
}
