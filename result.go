package golangNeo4jBoltDriver

// Result represents a result from a query that returns no data
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// TODO: Would sql/driver.RowsAffected render this useless?
type boltResult struct {
	rowsAffected int64
}

func newResult(rowsAffected int64) boltResult {
	return boltResult{rowsAffected: rowsAffected}
}

// LastInsertId gets the last inserted id. This will always return -1.
func (r boltResult) LastInsertId() (int64, error) {
	// TODO: Is this possible?
	return -1, nil
}

// RowsAffected returns the number of rows affected.
func (r boltResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
