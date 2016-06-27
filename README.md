# Golang Neo4J Bolt Driver

Implements the Neo4J Bolt Protocol specification:
As of the time of writing this, the current version is v3.1.0-M02


```
go get github.com/johnnadratowski/golang-neo4j-bolt-driver
```

## Usage

*_Please see [the statement tests](./stmt_test.go) for example usage_*

This implementation attempts to follow the best practices as per the Bolt specification, but also implements compatibility with Golang's `sql.driver` interface.

As such, these interfaces closely match the `sql.driver` interfaces, but they also provide Neo4j Bolt specific functionality in addition to the `sql.driver` interface.

It is recommended that you use the Neo4j Bolt-specific interfaces if possible.  The implementation is more efficient and can more closely support the Neo4j Bolt feature set.

Some compromises had to be made in order to support the `sql.driver` interface.  For example, in order to pass values using the proper driver.Value interface, any data types that don't support that interface must be marshalled/unmarshalled using the provided encoder/decoder in the `encoding` package.

You can get logs from the driver by setting the log level using the `log` packages `SetLevel`.


## Dev Quickstart

```
# No special build steps necessary
go build

# Testing with log info and a local bolt DB, getting coverage output
BOLT_DRIVER_LOG=info NEO4J_BOLT=bolt://localhost:7687 go test -coverprofile=./tmp/cover.out -coverpkg=./... -v && go tool cover -html=./tmp/cover.out
```

The tests are written in an integration testing style.  Most of them are in the statement tests, but should be made more granular in the future.

## TODO

* More convenient Query/Exec methods on conn
* Connection pooling for driver when not using SQL interface
* Cypher Parser to implement NumInput and pre-flight checking
* More Tests
