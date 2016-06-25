package golangNeo4jBoltDriver

import (
	"database/sql/driver"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/log"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
)

// Stmt represents a statement to run against the database
//
// Stmt objects, and any rows prepared within ARE NOT
// THREAD SAFE.  If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type Stmt interface {
	// Close Closes the statement. See sql/driver.Stmt.
	Close() error
	// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
	ExecNeo(params map[string]interface{}) (Result, error)
	// QueryNeo executes a query that returns data. Implements a Neo-friendly alternative to sql/driver.
	QueryNeo(params map[string]interface{}) (Rows, error)
}

// PipelineStmt represents a set of statements to run against the database
//
// PipelineStmt objects, and any rows prepared within ARE NOT
// THREAD SAFE.  If you want to use multiple go routines with these objects,
// you should use a driver to create a new conn for each routine.
type PipelineStmt interface {
	// Close Closes the statement. See sql/driver.Stmt.
	Close() error
	// ExecPipeline executes a set of queries that returns no rows.
	ExecPipeline(params ...map[string]interface{}) ([]Result, error)
	// QueryPipeline executes a set of queries that return data.
	// Implements a Neo-friendly alternative to sql/driver.
	QueryPipeline(params ...map[string]interface{}) (PipelineRows, error)
}

type boltStmt struct {
	queries []string
	query   string
	conn    *boltConn
	closed  bool
	rows    *boltRows
}

func newStmt(query string, conn *boltConn) *boltStmt {
	return &boltStmt{query: query, conn: conn}
}

func newPipelineStmt(queries []string, conn *boltConn) *boltStmt {
	return &boltStmt{queries: queries, conn: conn}
}

// Close Closes the statement. See sql/driver.Stmt.
func (s *boltStmt) Close() error {
	if s.closed {
		return nil
	}

	if s.rows != nil {
		if err := s.rows.Close(); err != nil {
			return err
		}
	}

	s.closed = true
	s.conn.statement = nil
	s.conn = nil
	return nil
}

// NumInput returns the number of placeholder parameters. See sql/driver.Stmt.
// Currently will always return -1
func (s *boltStmt) NumInput() int {
	return -1 // TODO: Not sure if we should disable this
}

// args turns a driver value list into neo4j query args
func (s *boltStmt) args(args []driver.Value) (map[string]interface{}, error) {
	if len(args)%2 != 0 {
		return nil, errors.New("Must pass an even numer of arguments - key then value")
	}

	output := map[string]interface{}{}
	for i := 0; i < len(args)-1; i++ {
		k, ok := args[i].(string)
		if !ok {
			return nil, errors.New("Only support strings for keys. Argument %d was not a string. Got: %#v", i, args[i])
		}
		output[k] = args[i+1].(interface{})
	}

	return output, nil
}

// Exec executes a query that returns no rows. See sql/driver.Stmt.
//
// This implementation does not support positional arguments,  only named arguments.
// To meet the sql.Driver interface, this translates the args to a map[string]interface{}
// by taking the even index numbers as keys and the odd index numbers as values. Example:
//
// []driver.Value{"key1", "value1", "key2", "value2"}.
//
// It is illegal to pass an odd number of arguments.
func (s *boltStmt) Exec(args []driver.Value) (driver.Result, error) {
	params, err := s.args(args)
	if err != nil {
		return nil, err
	}
	return s.ExecNeo(params)
}

// ExecNeo executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) ExecNeo(params map[string]interface{}) (Result, error) {
	if s.closed {
		return nil, errors.New("Neo4j Bolt statement already closed")
	}
	if s.rows != nil {
		return nil, errors.New("Another query is already open")
	}

	runResp, pullResp, _, err := s.conn.sendRunPullAllConsumeAll(s.query, params)
	if err != nil {
		return nil, err
	}

	success, ok := runResp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type when running exec query: %#v", success)

	}

	log.Infof("Got run success message: %#v", success)

	success, ok = pullResp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response when discarding exec rows: %#v", success)
	}

	log.Infof("Got discard all success message: %#v", success)

	return newResult(success.Metadata), nil
}

func (s *boltStmt) ExecPipeline(params ...map[string]interface{}) ([]Result, error) {
	if s.closed {
		return nil, errors.New("Neo4j Bolt statement already closed")
	}
	if s.rows != nil {
		return nil, errors.New("Another query is already open")
	}

	if len(params) != len(s.queries) {
		return nil, errors.New("Must pass same number of params as there are queries")
	}

	for i, query := range s.queries {
		err := s.conn.sendRunPullAll(query, params[i])
		if err != nil {
			return nil, errors.Wrap(err, "Error running exec query:\n\n%s\n\nWith Params:\n%#v", query, params[i])
		}
	}

	log.Info("Successfully ran all pipeline queries")

	results := make([]Result, len(s.queries))
	for i := range s.queries {
		runResp, err := s.conn.consume()
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred getting result of exec command: %#v", runResp)
		}

		success, ok := runResp.(messages.SuccessMessage)
		if !ok {
			return nil, errors.New("Unexpected response when getting exec query result: %#v", runResp)
		}

		_, pullResp, err := s.conn.consumeAll()
		if err != nil {
			return nil, errors.Wrap(err, "An error occurred getting result of exec discard command: %#v", pullResp)
		}

		success, ok = pullResp.(messages.SuccessMessage)
		if !ok {
			return nil, errors.New("Unexpected response when getting exec query discard result: %#v", pullResp)
		}

		results[i] = newResult(success.Metadata)

	}

	return results, nil
}

// Query executes a query that returns data. See sql/driver.Stmt.
//
// This implementation does not support positional arguments,  only named arguments.
// To meet the sql.Driver interface, this translates the args to a map[string]interface{}
// by taking the even index numbers as keys and the odd index numbers as values. Example:
//
// []driver.Value{"key1", "value1", "key2", "value2"}.
//
// It is illegal to pass an odd number of arguments.
func (s *boltStmt) Query(args []driver.Value) (driver.Rows, error) {
	params, err := s.args(args)
	if err != nil {
		return nil, err
	}
	return s.queryNeo(params)
}

// QueryNeo executes a query that returns data. Implements a Neo-friendly alternative to sql/driver.
func (s *boltStmt) QueryNeo(params map[string]interface{}) (Rows, error) {
	return s.queryNeo(params)
}

func (s *boltStmt) queryNeo(params map[string]interface{}) (*boltRows, error) {
	if s.closed {
		return nil, errors.New("Neo4j Bolt statement already closed")
	}
	if s.rows != nil {
		return nil, errors.New("Another query is already open")
	}

	respInt, err := s.conn.sendRunConsume(s.query, params)
	if err != nil {
		return nil, err
	}

	resp, ok := respInt.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Unrecognized response type running query: %#v", resp)
	}

	log.Infof("Got success message on run query: %#v", resp)
	s.rows = newRows(s, resp.Metadata)
	return s.rows, nil
}

func (s *boltStmt) QueryPipeline(params ...map[string]interface{}) (PipelineRows, error) {
	if s.closed {
		return nil, errors.New("Neo4j Bolt statement already closed")
	}
	if s.rows != nil {
		return nil, errors.New("Another query is already open")
	}

	if len(params) != len(s.queries) {
		return nil, errors.New("Must pass same number of params as there are queries")
	}

	for i, query := range s.queries {
		err := s.conn.sendRunPullAll(query, params[i])
		if err != nil {
			return nil, errors.Wrap(err, "Error running query:\n\n%s\n\nWith Params:\n%#v", query, params[i])
		}
	}

	log.Info("Successfully ran all pipeline queries")

	resp, err := s.conn.consume()
	if err != nil {
		return nil, errors.Wrap(err, "An error occurred consuming initial pipeline command")
	}

	success, ok := resp.(messages.SuccessMessage)
	if !ok {
		return nil, errors.New("Got unexpected return message when consuming initial pipeline command: %#v", resp)
	}

	s.rows = newPipelineRows(s, success.Metadata, 0)
	return s.rows, nil
}
