package golangNeo4jBoltDriver

import (
	"fmt"

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
		return fmt.Errorf("Transaction already closed")
	}

	respInt, err := t.conn.sendRun("COMMIT", nil)
	if err != nil {
		return fmt.Errorf("An error occurred committing transaction: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Successfully committed transaction: %#v", resp)
	case messages.FailureMessage:
		Logger.Printf("Got failure message committing transaction: %#v", resp)
		err = t.conn.ackFailure(resp)
		if err != nil {
			t.conn.Close()
			err = fmt.Errorf("Unrecoverable failure committing transaction. Closing connection. Error: %s \nGot Failure Message: %#v.", err, resp)
		} else {
			err = fmt.Errorf("Got failure message committing transaction: %#v", resp)
		}
	default:
		err = fmt.Errorf("Unrecognized response type committing transaction: %T Value: %#v", resp, resp)
	}

	t.conn.transaction = nil
	t.closed = true
	return err
}

// Rollback rolls back and closes the transaction
func (t *boltTx) Rollback() error {
	if t.closed {
		return fmt.Errorf("Transaction already closed")
	}

	respInt, err := t.conn.sendRun("ROLLBACK", nil)
	if err != nil {
		return fmt.Errorf("An error occurred rolling back transaction: %s", err)
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		Logger.Printf("Successfully rollback transaction: %#v", resp)
	case messages.FailureMessage:
		Logger.Printf("Got failure message rollback transaction: %#v", resp)
		err = t.conn.ackFailure(resp)
		if err != nil {
			t.conn.Close()
			err = fmt.Errorf("Unrecoverable failure rollback transaction. Closing connection. Error: %s \nGot Failure Message: %#v.", err, resp)
		} else {
			err = fmt.Errorf("Got failure message rollback transaction: %#v", resp)
		}
	default:
		err = fmt.Errorf("Unrecognized response type rollback transaction: %T Value: %#v", resp, resp)
	}

	t.conn.transaction = nil
	t.closed = true
	return err
}
