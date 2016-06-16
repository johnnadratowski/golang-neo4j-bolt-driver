package golangNeo4jBoltDriver

import (
	"fmt"
	"reflect"
)

// Result represents a result from a query that returns no data
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
	Metadata() (map[string]interface{})
}

// TODO: Would sql/driver.RowsAffected render this useless?
type boltResult struct {
	metadata map[string]interface{}
}

func newResult(metadata map[string]interface{}) boltResult {
	return boltResult{metadata: metadata}
}

// Returns the response metadata from the bolt success message
func (r boltResult) Metadata() (map[string]interface{}) {
	return r.metadata
}

// LastInsertId gets the last inserted id. This will always return -1.
func (r boltResult) LastInsertId() (int64, error) {
	// TODO: Is this possible? -
	// 	I think we would need to parse the query to get the number of parameters
	return -1, nil
}

// RowsAffected returns the number of rows affected.
func (r boltResult) RowsAffected() (int64, error) {
	stats, ok := r.metadata["stats"].(map[string]interface{})
	if !ok {
		return -1, fmt.Errorf("Unrecognized type for stats metadata: %#v", r.metadata)
	}

	var rowsAffected int64
	nodesCreated, ok := stats["nodes-created"]
	if ok && reflect.ValueOf(nodesCreated).Kind() == reflect.Int {
		rowsAffected += reflect.ValueOf(nodesCreated).Int()
	}

	relsCreated, ok := stats["rel-created"]
	if ok && reflect.ValueOf(relsCreated).Kind() == reflect.Int {
		rowsAffected += reflect.ValueOf(relsCreated).Int()
	}

	nodesDeleted, ok := stats["nodes-deleted"]
	if ok && reflect.ValueOf(nodesDeleted).Kind() == reflect.Int {
		rowsAffected += reflect.ValueOf(nodesDeleted).Int()
	}

	relsDeleted, ok := stats["rel-deleted"]
	if ok && reflect.ValueOf(relsDeleted).Kind() == reflect.Int {
		rowsAffected += reflect.ValueOf(relsDeleted).Int()
	}

	return rowsAffected, nil
}
