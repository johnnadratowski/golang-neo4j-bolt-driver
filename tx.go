package bolt

import (
	"github.com/SermoDigital/golang-neo4j-bolt-driver/errors"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/log"
	"github.com/SermoDigital/golang-neo4j-bolt-driver/structures/messages"
)

type boltTx struct {
	conn   *conn
	closed bool
}

func newTx(con *conn) *boltTx {
	return &boltTx{conn: con}
}

// Commit commits and closes the transaction
func (t *boltTx) Commit() error {
	if t.closed {
		return errors.New("Transaction already closed")
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

	t.closed = true
	return err
}

// Rollback rolls back and closes the transaction
func (t *boltTx) Rollback() error {
	if t.closed {
		return errors.New("Transaction already closed")
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

	t.closed = true
	return err
}
