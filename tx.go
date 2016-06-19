package golangNeo4jBoltDriver

import (
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Tx represents a transaction
type Tx interface {
	Commit() error
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

	respInt, err := t.conn.sendRun("COMMIT", nil)
	if err != nil {
		return errors.Wrap(err, "An error occurred committing transaction")
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully committed transaction: %#v", resp)
	default:
		err = errors.New("Unrecognized response type committing transaction: %T Value: %#v", resp, resp)
	}

	t.conn.transaction = nil
	t.closed = true
	return err
}

// Rollback rolls back and closes the transaction
func (t *boltTx) Rollback() error {
	if t.closed {
		return errors.New("Transaction already closed")
	}

	respInt, err := t.conn.sendRun("ROLLBACK", nil)
	if err != nil {
		return errors.Wrap(err, "An error occurred rolling back transaction")
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Successfully rollback transaction: %#v", resp)
	default:
		err = errors.New("Unrecognized response type rollback transaction: %T Value: %#v", resp, resp)
	}

	t.conn.transaction = nil
	t.closed = true
	return err
}
