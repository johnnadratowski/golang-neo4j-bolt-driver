package golangNeo4jBoltDriver

import (
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Tx represents a transaction
type Tx interface {
	// Commit commits the transaction
	Commit() error
	// Rollback rolls back the transaction
	Rollback() error
}

type boltTx struct {
	conn   *boltConn
	closed bool
}

func newTx(conn *boltConn) *boltTx {
	return &boltTx{
		conn: conn,
	}
}

// Commit commits and closes the transaction
func (t *boltTx) Commit() error {
	if t.closed {
		return errors.New("Transaction already closed")
	}
	if t.conn.statement != nil {
		if err := t.conn.statement.Close(); err != nil {
			return errors.Wrap(err, "An error occurred closing open rows in transaction Commit")
		}
	}

	successInt, pullInt, err := t.conn.sendRunPullAllConsumeSingle("COMMIT", nil)
	if err != nil {
		return errors.Wrap(err, "An error occurred committing transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return errors.New("Unrecognized response type committing transaction: %#v", success)
	}

	log.Infof("Got success message committing transaction: %#v", success)

	pull, ok := pullInt.(messages.SuccessMessage)
	if !ok {
		return errors.New("Unrecognized response type pulling transaction:  %#v", pull)
	}

	log.Infof("Got success message pulling transaction: %#v", pull)

	t.conn.transaction = nil
	t.closed = true
	return err
}

// Rollback rolls back and closes the transaction
func (t *boltTx) Rollback() error {
	if t.closed {
		return errors.New("Transaction already closed")
	}
	if t.conn.statement != nil {
		if err := t.conn.statement.Close(); err != nil {
			return errors.Wrap(err, "An error occurred closing open rows in transaction Rollback")
		}
	}

	successInt, pullInt, err := t.conn.sendRunPullAllConsumeSingle("ROLLBACK", nil)
	if err != nil {
		return errors.Wrap(err, "An error occurred rolling back transaction")
	}

	success, ok := successInt.(messages.SuccessMessage)
	if !ok {
		return errors.New("Unrecognized response type rolling back transaction: %#v", success)
	}

	log.Infof("Got success message rolling back transaction: %#v", success)

	pull, ok := pullInt.(messages.SuccessMessage)
	if !ok {
		return errors.New("Unrecognized response type pulling transaction: %#v", pull)
	}

	log.Infof("Got success message pulling transaction: %#v", pull)

	t.conn.transaction = nil
	t.closed = true
	return err
}
