package golangNeo4jBoltDriver

import "github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"

// Result represents a result from a query that returns no data
type Result interface {
	// LastInsertId Always returns -1. This is necessary
	// to meet the sql.driver interface
	LastInsertId() (int64, error)
	// RowsAffected returns the number of rows affected
	// This doesn't currently support updates, only
	// inserts/deletions
	RowsAffected() (int64, error)
	// Metadata returns the metadata response from neo4j
	Metadata() map[string]interface{}
}

type boltResult struct {
	metadata map[string]interface{}
}

func newResult(metadata map[string]interface{}) boltResult {
	return boltResult{metadata: metadata}
}

// Returns the response metadata from the bolt success message
func (r boltResult) Metadata() map[string]interface{} {
	return r.metadata
}

// LastInsertId gets the last inserted id. This will always return -1.
func (r boltResult) LastInsertId() (int64, error) {
	// TODO: Is this possible?
	return -1, nil
}

// RowsAffected returns the number of nodes+rels created/deleted.  For reasons of limitations
// on the API, we cannot tell how many nodes+rels were updated, only how many properties were
// updated.  If this changes in the future, number updated will be added to the output of this
// interface.
func (r boltResult) RowsAffected() (int64, error) {
	stats, ok := r.metadata["stats"].(map[string]interface{})
	if !ok {
		return -1, errors.New("Unrecognized type for stats metadata: %#v", r.metadata)
	}

	var rowsAffected int64
	nodesCreated, ok := stats["nodes-created"]
	if ok {
		rowsAffected += nodesCreated.(int64)
	}

	relsCreated, ok := stats["relationships-created"]
	if ok {
		rowsAffected += relsCreated.(int64)
	}

	nodesDeleted, ok := stats["nodes-deleted"]
	if ok {
		rowsAffected += nodesDeleted.(int64)
	}

	relsDeleted, ok := stats["relationships-deleted"]
	if ok {
		rowsAffected += relsDeleted.(int64)
	}

	return rowsAffected, nil
}
