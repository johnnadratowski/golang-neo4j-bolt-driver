# Golang Neo4J Bolt Driver
[![Build Status](https://travis-ci.org/johnnadratowski/golang-neo4j-bolt-driver.svg?branch=master)](https://travis-ci.org/johnnadratowski/golang-neo4j-bolt-driver)
[![GoDoc](https://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver?status.svg)](https://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver)


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
	conn, _ := driver.OpenNeo("bolt://localhost:7687")
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
func slowNClean() {
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo("bolt://localhost:7687")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Here we prepare a new statement. This gives us the flexibility to
	// cancel that statement without any request sent to Neo
	stmt, err := conn.PrepareNeo("CREATE (n:NODE {foo: {foo}, bar: {bar}})")
	if err != nil {
		panic(err)
	}

	// Executing a statement just returns summary information
	result, err := stmt.ExecNeo(map[string]interface{}{"foo": 1, "bar": 2.2})
	if err != nil {
		panic(err)
	}
	numResult, err := result.RowsAffected()
	if err != nil {
		panic(err)
	}
	fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 1

	// Closing the statment will also close the rows
	stmt.Close()

	// Lets get the node. Once again I can cancel this with no penalty
	stmt, err = conn.PrepareNeo("MATCH (n:NODE) RETURN n.foo, n.bar")
	if err != nil {
		panic(err)
	}

	// Even once I get the rows, if I do not consume them and close the
	// rows, Neo will discard and not send the data
	rows, err := stmt.QueryNeo(nil)
	if err != nil {
		panic(err)
	}

	// This interface allows you to consume rows one-by-one, as they
	// come off the bolt stream. This is more efficient especially
	// if you're only looking for a particular row/set of rows, as
	// you don't need to load up the entire dataset into memory
	data, _, err := rows.NextNeo()
	if err != nil {
		panic(err)
	}

	// This query only returns 1 row, so once it's done, it will return
	// the metadata associated with the query completion, along with
	// io.EOF as the error
	_, _, err = rows.NextNeo()
	if err != io.EOF {
		panic(err)
	}
	fmt.Printf("COLUMNS: %#v\n", rows.Metadata()["fields"].([]interface{})) // COLUMNS: n.foo,n.bar
	fmt.Printf("FIELDS: %d %f\n", data[0].(int64), data[1].(float64))       // FIELDS: 1 2.2

	stmt.Close()

	// Here we prepare a new pipeline statement for running multiple
	// queries concurrently
	pipeline, err := conn.PreparePipeline(
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(f:FOO)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(b:BAR)",
		"MATCH (n:NODE) CREATE (n)-[:REL]->(z:BAZ)",
	)
	if err != nil {
		panic(err)
	}

	pipelineResults, err := pipeline.ExecPipeline(nil, nil, nil, nil, nil, nil)
	if err != nil {
		panic(err)
	}

	for _, result := range pipelineResults {
		numResult, _ := result.RowsAffected()
		fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 2 (per each iteration)
	}

	err = pipeline.Close()
	if err != nil {
		panic(err)
	}

	stmt, err = conn.PrepareNeo("MATCH path=(n:NODE)-[:REL]->(m) RETURN path")
	if err != nil {
		panic(err)
	}

	rows, err = stmt.QueryNeo(nil)
	if err != nil {
		panic(err)
	}

	// Here we loop through the rows until we get the metadata object
	// back, meaning the row stream has been fully consumed
	for err == nil {
		var row []interface{}
		row, _, err = rows.NextNeo()
		if err != nil && err != io.EOF {
			panic(err)
		} else if err != io.EOF {
			fmt.Printf("PATH: %#v\n", row[0].(graph.Path)) // Prints all paths
		}
	}

	stmt.Close()

	result, _ = conn.ExecNeo(`MATCH (n) DETACH DELETE n`, nil)
	fmt.Println(result)
	numResult, _ = result.RowsAffected()
	fmt.Printf("Rows Deleted: %d", numResult) // Rows Deleted: 13
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

You need access to a running Neo4J database to develop for this project, so that you can run the tests to generate the recordings.

## TODO

* Cypher Parser to implement NumInput and pre-flight checking
* More Tests
* Benchmark Tests
