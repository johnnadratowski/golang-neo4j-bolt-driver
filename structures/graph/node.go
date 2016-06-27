package graph

const (
	// NodeSignature is the signature byte for a Node object
	NodeSignature = 0x4E
)

// Node Represents a Node structure
type Node struct {
	NodeIdentity int64
	Labels       []string
	Properties   map[string]interface{}
}

// Signature gets the signature byte for the struct
func (n Node) Signature() int {
	return NodeSignature
}

// AllFields gets the fields to encode for the struct
func (n Node) AllFields() []interface{} {
	labels := make([]interface{}, len(n.Labels))
	for i, label := range n.Labels {
		labels[i] = label
	}
	return []interface{}{n.NodeIdentity, labels, n.Properties}
}
