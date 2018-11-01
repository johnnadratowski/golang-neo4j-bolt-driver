# Golang Neo4J Bolt Driver
[![Build Status](https://travis-ci.org/johnnadratowski/golang-neo4j-bolt-driver.svg?branch=master)](https://travis-ci.org/johnnadratowski/golang-neo4j-bolt-driver)
[![GoDoc](https://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver?status.svg)](https://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver)


**ANNOUNCEMENT: I must apologize to the community for not being more responsive.  Because of personal life events I am really not able to properly maintain this codebase.  Certain other events lead me to believe an official Neo4J Golang driver was to be released soon, but it seems like that may not necessarily be the case.  Since I am unable to properly maintain this codebase at this juncture, at this point I think it makes sense to open up this repo to collaborators who are interested in helping with maintenance.  Please feel free to email me directly if you're interested.**

Implements the Neo4J Bolt Protocol specification:
As of the time of writing this, the current version is v3.1.0-M02

```
go get github.com/johnnadratowski/golang-neo4j-bolt-driver
```

## Features

* Neo4j Bolt low-level binary protocol support
* Message Pipelining for high concurrency
* Connection Pooling
* TLS support
* Compatible with sql.driver

## Usage

*_Please see [the statement tests](./stmt_test.go) or [the conn tests](./conn_test.go) for A LOT of examples of usage_*

### Examples

#### Quick n’ Dirty

```go
func quickNDirty() {
	driver := bolt.NewDriver()
	conn, _ := driver.OpenNeo("bolt://username:password@localhost:7687")
	defer conn.Close()

	// Start by creating a node
	result, _ := conn.ExecNeo("CREATE (n:NODE {foo: {foo}, bar: {bar}})", map[string]interface{}{"foo": 1, "bar": 2.2})
	numResult, _ := result.RowsAffected()
	fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 1

	// Lets get the node
	data, rowsMetadata, _, _ := conn.QueryNeoAll("MATCH (n:NODE) RETURN n.foo, n.bar", nil)
	fmt.Printf("COLUMNS: %#v\n", rowsMetadata["fields"].([]interface{}))  // COLUMNS: n.foo,n.bar
	fmt.Printf("FIELDS: %d %f\n", data[0][0].(int64), data[0][1].(float64)) // FIELDS: 1 2.2

	// oh cool, that worked. lets blast this baby and tell it to run a bunch of statements
	// in neo concurrently with a pipeline
	results, _ := conn.ExecPipeline([]string{
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
	}, nil, nil, nil, nil, nil, nil)
	for _, result := range results {
		numResult, _ := result.RowsAffected()
		fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 2 (per each iteration)
	}

	data, _, _, _ = conn.QueryNeoAll("MATCH (n:NODE)-[:REL]->(m) RETURN m", nil)
	for _, row := range data {
		fmt.Printf("NODE: %#v\n", row[0].(graph.Node)) // Prints all nodes
	}

	result, _ = conn.ExecNeo(`MATCH (n) DETACH DELETE n`, nil)
	numResult, _ = result.RowsAffected()
	fmt.Printf("Rows Deleted: %d", numResult) // Rows Deleted: 13
}
```

#### Slow n' Clean

```go

// Constants to be used throughout the example
const (
	URI          = "bolt://username:password@localhost:7687"
	CreateNode   = "CREATE (n:NODE {foo: {foo}, bar: {bar}})"
	GetNode      = "MATCH (n:NODE) RETURN n.foo, n.bar"
	RelationNode = "MATCH path=(n:NODE)-[:REL]->(m) RETURN path"
	DeleteNodes  = "MATCH (n) DETACH DELETE n"
)

func main() {
	con := createConnection()
	defer con.Close()

	st := prepareSatement(CreateNode, con)
	executeStatement(st)

	st = prepareSatement(GetNode, con)
	rows := queryStatement(st)
	consumeRows(rows, st)

	pipe := preparePipeline(con)
	executePipeline(pipe)

	st = prepareSatement(RelationNode, con)
	rows = queryStatement(st)
	consumeMetadata(rows, st)

	cleanUp(DeleteNodes, con)
}

func createConnection() bolt.Conn {
	driver := bolt.NewDriver()
	con, err := driver.OpenNeo(URI)
	handleError(err)
	return con
}

// Here we prepare a new statement. This gives us the flexibility to
// cancel that statement without any request sent to Neo
func prepareSatement(query string, con bolt.Conn) bolt.Stmt {
	st, err := con.PrepareNeo(query)
	handleError(err)
	return st
}

// Here we prepare a new pipeline statement for running multiple
// queries concurrently
func preparePipeline(con bolt.Conn) bolt.PipelineStmt {
	pipeline, err := con.PreparePipeline(
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
	)
	handleError(err)
	return pipeline
}

func executePipeline(pipeline bolt.PipelineStmt) {
	pipelineResults, err := pipeline.ExecPipeline(nil, nil, nil, nil, nil, nil)
	handleError(err)

	for _, result := range pipelineResults {
		numResult, _ := result.RowsAffected()
		fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 2 (per each iteration)
	}

	err = pipeline.Close()
	handleError(err)
}

func queryStatement(st bolt.Stmt) bolt.Rows {
	// Even once I get the rows, if I do not consume them and close the
	// rows, Neo will discard and not send the data
	rows, err := st.QueryNeo(nil)
	handleError(err)
	return rows
}

func consumeMetadata(rows bolt.Rows, st bolt.Stmt) {
	// Here we loop through the rows until we get the metadata object
	// back, meaning the row stream has been fully consumed

	var err error
	err = nil

	for err == nil {
		var row []interface{}
		row, _, err = rows.NextNeo()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err != io.EOF {
			fmt.Printf("PATH: %#v\n", row[0].(graph.Path)) // Prints all paths
		}
	}
	st.Close()
}

func consumeRows(rows bolt.Rows, st bolt.Stmt) {
	// This interface allows you to consume rows one-by-one, as they
	// come off the bolt stream. This is more efficient especially
	// if you're only looking for a particular row/set of rows, as
	// you don't need to load up the entire dataset into memory
	data, _, err := rows.NextNeo()
	handleError(err)

	// This query only returns 1 row, so once it's done, it will return
	// the metadata associated with the query completion, along with
	// io.EOF as the error
	_, _, err = rows.NextNeo()
	handleError(err)
	fmt.Printf("COLUMNS: %#v\n", rows.Metadata()["fields"].([]interface{})) // COLUMNS: n.foo,n.bar
	fmt.Printf("FIELDS: %d %f\n", data[0].(int64), data[1].(float64))       // FIELDS: 1 2.2

	st.Close()
}

// Executing a statement just returns summary information
func executeStatement(st bolt.Stmt) {
	result, err := st.ExecNeo(map[string]interface{}{"foo": 1, "bar": 2.2})
	handleError(err)
	numResult, err := result.RowsAffected()
	handleError(err)
	fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 1

	// Closing the statment will also close the rows
	st.Close()
}

func cleanUp(query string, con bolt.Conn) {
	result, _ := con.ExecNeo(query, nil)
	fmt.Println(result)
	numResult, _ := result.RowsAffected()
	fmt.Printf("Rows Deleted: %d", numResult) // Rows Deleted: 13
}

// Here we create a simple function that will take care of errors, helping with some code clean up
func handleError(err error) {
	if err != nil {
		panic(err)
	}
}
```
## API

*_There is much more detailed information in [the godoc](http://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver)_*

This implementation attempts to follow the best practices as per the Bolt specification, but also implements compatibility with Golang's `sql.driver` interface.

As such, these interfaces closely match the `sql.driver` interfaces, but they also provide Neo4j Bolt specific functionality in addition to the `sql.driver` interface.

It is recommended that you use the Neo4j Bolt-specific interfaces if possible.  The implementation is more efficient and can more closely support the Neo4j Bolt feature set.

The URL format is: `bolt://(user):(password)@(host):(port)`
Schema must be `bolt`. User and password is only necessary if you are authenticating.

Connection pooling is provided out of the box with the `NewDriverPool` method.  You can give it the maximum number of
connections to have at a time.

You can get logs from the driver by setting the log level using the `log` packages `SetLevel`.


## Dev Quickstart

```
# Put in git hooks
ln -s ../../scripts/pre-commit .git/hooks/pre-commit
ln -s ../../scripts/pre-push .git/hooks/pre-push

# No special build steps necessary
go build

# Testing with log info and a local bolt DB, getting coverage output
BOLT_DRIVER_LOG=info NEO4J_BOLT=bolt://localhost:7687 go test -coverprofile=./tmp/cover.out -coverpkg=./... -v -race && go tool cover -html=./tmp/cover.out

# Testing with trace output for debugging
BOLT_DRIVER_LOG=trace NEO4J_BOLT=bolt://localhost:7687 go test -v -race

# Testing with running recorder to record tests for CI
BOLT_DRIVER_LOG=trace NEO4J_BOLT=bolt://localhost:7687 RECORD_OUTPUT=1 go test -v -race
```

The tests are written in an integration testing style.  Most of them are in the statement tests, but should be made more granular in the future.

In order to get CI, I made a recorder mechanism so you don't need to run neo4j alongside the tests in the CI server.  You run the tests locally against a neo4j instance with the RECORD_OUTPUT=1 environment variable, it generates the recordings in the ./recordings folder.  This is necessary if the tests have changed, or if the internals have significantly changed.  Installing the git hooks will run the tests automatically on push.  If there are updated tests, you will need to re-run the recorder to add them and push them as well.

You need access to a running Neo4J database to develop for this project, so that you can run the tests to generate the recordings. For the recordings to be generated correctly you also need to make sure authorization is turned off on the Neo4J instance. For more information on Neo4J installation and configuration see the official Neo4j docs: https://neo4j.com/docs/operations-manual/current/installation/

## Supported Builds
* Linux (1.4.x, 1.5.x, 1.6.x, 1.7.x, 1.8.x, 1.9.x and tip)
* MacOs (1.7.x, 1.8.x, 1.9.x and tip)
*_according to https://github.com/golang/go/issues/17824, go 1.6.4 (and anything prior does not support osx 10.12) which results in unpredicable behavior (sometimes it is okay, sometimes build hangs, and sometimes build fail due to segfault)_*



## TODO

* Cypher Parser to implement NumInput and pre-flight checking
* More Tests
* Benchmark Tests
