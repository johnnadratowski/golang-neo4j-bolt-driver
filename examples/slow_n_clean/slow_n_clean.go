package main

import (
	"fmt"

	bolt "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"io"
)

func main() {
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
