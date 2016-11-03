package encoding

import (
	"fmt"

	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/graph"
)

func errtype(want string, got interface{}) error {
	return fmt.Errorf("expected type %s, got %T", want, got)
}

func sliceInterfaceToString(from []interface{}) ([]string, error) {
	to := make([]string, len(from))
	for i, item := range from {
		str, ok := item.(string)
		if !ok {
			return nil, errtype("string", item)
		}
		to[i] = str
	}
	return to, nil
}

func sliceInterfaceToInt(from []interface{}) ([]int, error) {
	to := make([]int, len(from))
	for i, item := range from {
		x, ok := item.(int64)
		if !ok {
			return nil, errtype("int", item)
		}
		to[i] = int(x)
	}
	return to, nil
}

func sliceInterfaceToNode(from []interface{}) ([]graph.Node, error) {
	to := make([]graph.Node, len(from))
	for i, item := range from {
		node, ok := item.(graph.Node)
		if !ok {
			return nil, errtype("graph.Node", item)
		}
		to[i] = node
	}
	return to, nil
}

func sliceInterfaceToRelationship(from []interface{}) ([]graph.Relationship, error) {
	to := make([]graph.Relationship, len(from))
	for i, item := range from {
		rel, ok := item.(graph.Relationship)
		if !ok {
			return nil, errtype("graph.Relationship", item)
		}
		to[i] = rel
	}
	return to, nil
}

func sliceInterfaceToUnboundRelationship(from []interface{}) ([]graph.UnboundRelationship, error) {
	to := make([]graph.UnboundRelationship, len(from))
	for i, item := range from {
		ubr, ok := item.(graph.UnboundRelationship)
		if !ok {
			return nil, errtype("graph.UnboundedRelationship", item)
		}
		to[i] = ubr
	}
	return to, nil
}
