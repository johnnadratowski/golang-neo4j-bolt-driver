package golangNeo4jBoltDriver

// Tx represents a transaction
type Tx interface {
	Commit() error
	Rollback() error
}

type boltTx struct{}

func (t *boltTx) Commit() error {
	// TODO: Implement
	return nil
}

func (t *boltTx) Rollback() error {
	// TODO: Implement
	return nil
}
