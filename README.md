# Golang Neo4J Bolt Driver

Implements the Neo4J Bolt Protocol specification: http://alpha.neohq.net/docs/server-manual/bolt-overview.html
As of the time of writing this, the current version is v3.1.0-M02

The driver is compatible with golang's sql.Driver interface, and is registered as an SQL driver at "neo4j-bolt".

## Dev Quickstart

```
# Testing with log info and a local bolt DB, getting coverage output
BOLT_DRIVER_LOG=info NEO4J_BOLT=bolt://localhost:7687 go test -coverprofile=./tmp/cover.out -coverpkg=./... -v && go tool cover -html=./tmp/cover.out
```

// TODO: QuickStart - Install, Example
