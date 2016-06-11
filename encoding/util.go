package encoding

import (
	"fmt"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
)

func sliceInterfaceToString(from []interface{}) ([]string, error) {
	to := make([]string, len(from))
	for idx, item := range from {
		toItem, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("Expected string value. Got %T %+v", toItem, toItem)
		}
		to[idx] = toItem
	}
	return to, nil
}

func sliceInterfaceToInt(from []interface{}) ([]int, error) {
	to := make([]int, len(from))
	for idx, item := range from {
		toItem, ok := item.(int)
		if !ok {
			return nil, fmt.Errorf("Expected Node value. Got %T %+v", toItem, toItem)
		}
		to[idx] = toItem
	}
	return to, nil
}

func sliceInterfaceToNode(from []interface{}) ([]graph.Node, error) {
	to := make([]graph.Node, len(from))
	for idx, item := range from {
		toItem, ok := item.(graph.Node)
		if !ok {
			return nil, fmt.Errorf("Expected Node value. Got %T %+v", toItem, toItem)
		}
		to[idx] = toItem
	}
	return to, nil
}

func sliceInterfaceToRelationship(from []interface{}) ([]graph.Relationship, error) {
	to := make([]graph.Relationship, len(from))
	for idx, item := range from {
		toItem, ok := item.(graph.Relationship)
		if !ok {
			return nil, fmt.Errorf("Expected Relationship value. Got %T %+v", toItem, toItem)
		}
		to[idx] = toItem
	}
	return to, nil
}
