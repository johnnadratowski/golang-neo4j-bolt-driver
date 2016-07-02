# Golang Neo4J Bolt Driver

Implements the Neo4J Bolt Protocol specification:
As of the time of writing this, the current version is v3.1.0-M02

```
go get github.com/johnnadratowski/golang-neo4j-bolt-driver
```

## Usage

*_Please see [the statement tests](./stmt_test.go) or [the conn tests](./conn_test.go) for example usage_*

*_There is more detailed information in [the godoc](http://godoc.org/github.com/johnnadratowski/golang-neo4j-bolt-driver)_*

This implementation attempts to follow the best practices as per the Bolt specification, but also implements compatibility with Golang's `sql.driver` interface.

As such, these interfaces closely match the `sql.driver` interfaces, but they also provide Neo4j Bolt specific functionality in addition to the `sql.driver` interface.

It is recommended that you use the Neo4j Bolt-specific interfaces if possible.  The implementation is more efficient and can more closely support the Neo4j Bolt feature set.

You can get logs from the driver by setting the log level using the `log` packages `SetLevel`.


## Pipelining

This interface supports message pipelining, which allows for sending a bunch of requests to neo4j to be processed at once.
You can see [examples of this in the tests](./stmt_test.go#L1090).

## Dev Quickstart

```
# Put in git hooks
ln -s ../../scripts/pre-commit .git/hooks/pre-commit
ln -s ../../scripts/pre-push .git/hooks/pre-push

# No special build steps necessary
go build

# Testing with log info and a local bolt DB, getting coverage output
BOLT_DRIVER_LOG=info NEO4J_BOLT=bolt://localhost:7687 go test -coverprofile=./tmp/cover.out -coverpkg=./... -v && go tool cover -html=./tmp/cover.out
```

The tests are written in an integration testing style.  Most of them are in the statement tests, but should be made more granular in the future.

## TODO

* Connection pooling for driver when not using SQL interface
* Cypher Parser to implement NumInput and pre-flight checking
* More Tests
* Benchmark Tests
* Examples
* Github CI Integration
