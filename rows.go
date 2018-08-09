package golangNeo4jBoltDriver

import (
	"database/sql/driver"
	"io"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/encoding"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Rows represents results of rows from the DB
//
// Row objects ARE NOT THREAD SAFE.
// If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Rows interface {
	// Columns Gets the names of the columns in the returned dataset
	Columns() []string
	// Metadata Gets all of the metadata returned from Neo on query start
	Metadata() map[string]interface{}
	// Close the rows, flushing any existing datastream
	Close() error
	// NextNeo gets the next row result
	// When the rows are completed, returns the success metadata
	// and io.EOF
	NextNeo() ([]interface{}, map[string]interface{}, error)
	// All gets all of the results from the row set. It's recommended to use NextNeo when
	// there are a lot of rows
	All() ([][]interface{}, map[string]interface{}, error)
}

// PipelineRows represents results of a set of rows from the DB
// when running a pipeline statement.
//
// Row objects ARE NOT THREAD SAFE.
// If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type PipelineRows interface {
	// Columns Gets the names of the columns in the returned dataset
	Columns() []string
	// Metadata Gets all of the metadata returned from Neo on query start
	Metadata() map[string]interface{}
	// Close the rows, flushing any existing datastream
	Close() error
	// NextPipeline gets the next row result
	// When the rows are completed, returns the success metadata and the next
	// set of rows.
	// When all rows are completed, returns io.EOF
	NextPipeline() ([]interface{}, map[string]interface{}, PipelineRows, error)
}

type boltRows struct {
	metadata        map[string]interface{}
	statement       *boltStmt
	closed          bool
	consumed        bool
	finishedConsume bool
	pipelineIndex   int
	closeStatement  bool
}

func newRows(statement *boltStmt, metadata map[string]interface{}) *boltRows {
	return &boltRows{
		statement: statement,
		metadata:  metadata,
	}
}

func newQueryRows(statement *boltStmt, metadata map[string]interface{}) *boltRows {
	rows := newRows(statement, metadata)
	rows.consumed = true       // Already consumed from pipeline with PULL_ALL
	rows.closeStatement = true // Query rows don't expose a statement, so they need to close the statement when they close
	return rows
}

func newPipelineRows(statement *boltStmt, metadata map[string]interface{}, pipelineIndex int) *boltRows {
	rows := newRows(statement, metadata)
	rows.consumed = true // Already consumed from pipeline with PULL_ALL
	rows.pipelineIndex = pipelineIndex
	return rows
}

func newQueryPipelineRows(statement *boltStmt, metadata map[string]interface{}, pipelineIndex int) *boltRows {
	rows := newPipelineRows(statement, metadata, pipelineIndex)
	rows.closeStatement = true // Query rows don't expose a statement, so they need to close the statement when they close
	return rows
}

// Columns returns the columns from the result
func (r *boltRows) Columns() []string {
	fieldsInt, ok := r.metadata["fields"]
	if !ok {
		return []string{}
	}

	fields, ok := fieldsInt.([]interface{})
	if !ok {
		log.Errorf("Unrecognized fields from success message: %#v", fieldsInt)
		return []string{}
	}

	fieldsStr := make([]string, len(fields))
	for i, f := range fields {
		if fieldsStr[i], ok = f.(string); !ok {
			log.Errorf("Unrecognized fields from success message: %#v", fieldsInt)
			return []string{}
		}
	}
	return fieldsStr
}

// Metadata Gets all of the metadata returned from Neo on query start
func (r *boltRows) Metadata() map[string]interface{} {
	return r.metadata
}

// Close closes the rows
func (r *boltRows) Close() error {
	if r.closed {
		return nil
	}

	if !r.consumed {
		// Discard all messages if not consumed
		respInt, err := r.statement.conn.sendDiscardAllConsume()
		if err != nil {
			return errors.Wrap(err, "An error occurred discarding messages on row close")
		}

		switch resp := respInt.(type) {
		case messages.SuccessMessage:
			log.Infof("Got success message: %#v", resp)
		default:
			return errors.New("Unrecognized response type discarding all rows: Value: %#v", resp)
		}

	} else if !r.finishedConsume {
		// If this is a pipeline statement, we need to "consume all" multiple times
		numConsume := 1
		if r.statement.queries != nil {
			numQueries := len(r.statement.queries)
			if numQueries > 0 {
				// So, every pipeline statement has two successes
				// but by the time you get to the row object, one has
				// been consumed. Hence we need to clear out the
				// rest of the messages on close by taking the current
				// index * 2 but removing the first success
				numConsume = ((numQueries - r.pipelineIndex) * 2) - 1
			}
		}

		// Clear out all unconsumed messages if we
		// never finished consuming them.
		_, _, err := r.statement.conn.consumeAllMultiple(numConsume)
		if err != nil {
			return errors.Wrap(err, "An error occurred clearing out unconsumed stream")
		}
	}

	r.closed = true
	r.statement.rows = nil

	if r.closeStatement {
		return r.statement.Close()
	}
	return nil
}

// Next gets the next row result
func (r *boltRows) Next(dest []driver.Value) error {
	data, _, err := r.NextNeo()
	if err != nil {
		return err
	}

	for i, item := range data {
		switch item := item.(type) {
		case []interface{}, map[string]interface{}, graph.Node, graph.Path, graph.Relationship, graph.UnboundRelationship:
			dest[i], err = encoding.Marshal(item)
			if err != nil {
				return err
			}
		default:
			dest[i], err = driver.DefaultParameterConverter.ConvertValue(item)
			if err != nil {
				return err
			}
		}
	}

	return nil

}

// NextNeo gets the next row result
// When the rows are completed, returns the success metadata
// and io.EOF
func (r *boltRows) NextNeo() ([]interface{}, map[string]interface{}, error) {
	if r.closed {
		return nil, nil, errors.New("Rows are already closed")
	}

	if !r.consumed {
		r.consumed = true
		if err := r.statement.conn.sendPullAll(); err != nil {
			r.finishedConsume = true
			return nil, nil, err
		}
	}

	respInt, err := r.statement.conn.consume()
	if err != nil {
		return nil, nil, err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Got success message: %#v", resp)
		r.finishedConsume = true
		return nil, resp.Metadata, io.EOF
	case messages.RecordMessage:
		log.Infof("Got record message: %#v", resp)
		return resp.Fields, nil, nil
	default:
		return nil, nil, errors.New("Unrecognized response type getting next query row: %#v", resp)
	}
}

func (r *boltRows) All() ([][]interface{}, map[string]interface{}, error) {
	output := [][]interface{}{}
	for {
		row, metadata, err := r.NextNeo()
		if err != nil || row == nil {
			if err == io.EOF {
				return output, metadata, nil
			}
			return output, metadata, err
		}
		output = append(output, row)
	}
}

// NextPipeline gets the next row result
// When the rows are completed, returns the success metadata and the next
// set of rows.
// When all rows are completed, returns io.EOF
func (r *boltRows) NextPipeline() ([]interface{}, map[string]interface{}, PipelineRows, error) {
	if r.closed {
		return nil, nil, nil, errors.New("Rows are already closed")
	}

	respInt, err := r.statement.conn.consume()
	if err != nil {
		return nil, nil, nil, err
	}

	switch resp := respInt.(type) {
	case messages.SuccessMessage:
		log.Infof("Got success message: %#v", resp)

		if r.pipelineIndex == len(r.statement.queries)-1 {
			r.finishedConsume = true
			return nil, nil, nil, err
		}

		successResp, err := r.statement.conn.consume()
		if err != nil && err != io.EOF {
			return nil, nil, nil, errors.Wrap(err, "An error occurred getting next set of rows from pipeline command: %#v", successResp)
		}

		success, ok := successResp.(messages.SuccessMessage)
		if !ok {
			return nil, nil, nil, errors.New("Unexpected response getting next set of rows from pipeline command: %#v", successResp)
		}

		r.statement.rows = newPipelineRows(r.statement, success.Metadata, r.pipelineIndex+1)
		r.statement.rows.closeStatement = r.closeStatement
		return nil, success.Metadata, r.statement.rows, nil

	case messages.RecordMessage:
		log.Infof("Got record message: %#v", resp)
		return resp.Fields, nil, nil, nil
	default:
		return nil, nil, nil, errors.New("Unrecognized response type getting next pipeline row: %#v", resp)
	}
}
