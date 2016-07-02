/*Package golangNeo4jBoltDriver implements a driver for the Neo4J Bolt Protocol.

The driver is compatible with Golang's sql.driver interface, but
aims to implement a more complete featureset in line with what
Neo4J and Bolt provides.

As such, there are multiple interfaces the user can choose from.
It's highly recommended that the user use the Neo4J-specific
interfaces as they are more flexible and efficient then the
provided sql.driver compatible methods.

The interface tries to be consistent throughout. The sql.driver
interfaces are standard, but the Neo4J-specific ones contain a
naming convention of either "Neo" or "Pipeline".

The "Neo" ones are the basic interfaces for making queries to
Neo4j and it's expected that these would be used the most.

The "Pipeline" ones are to support Bolt's pipelining features.
Pipelines allow the user to send Neo4j many queries at once and
have them executed by the database asynchronously.  This is useful
if you have a bunch of queries that aren't necessarily dependant
on one another, and you want to get better performance.  The
internal APIs will also pipeline statements where it is able to
reliably do so, but by manually using the pipelining feature
you can maximize your throughput.

The sql driver is registered as "neo4j-bolt". The sql.driver interface is much more limited than what bolt and neo4j supports.  In some cases, concessions were made in order to make that interface work with the neo4j way of doing things.  The main instance of this is the marshalling of objects to/from the sql.driver.Value interface.  In order to support object types that aren't supported by this interface, the internal encoding package is used to marshal these objects to byte strings. This ultimately makes for a less efficient and more 'clunky' implementation.  A glaring instance of this is passing parameters.  Neo4j expects named parameters but the driver interface can only really support positional parameters. To get around this, the user must create a map[string]interface{} of their parameters and marshal it to a driver.Value using the encoding.Marshal function. Similarly, the user must unmarshal data returned from the queries using the encoding.Unmarshal function, then use type assertions to retrieve the proper type.

In most cases the driver will return the data from neo as the proper go-specific types.  These types are very specific and will be returned with the minimum amount of bytes necessary, as this is how they are encoded in the driver.  For example, if you sent a number that's an integer but it has a value of '1', it will come back as an int8, as that's the lowest number of bytes that need to be sent over the line.  The user is expected to cast them as necessary.

There are also cases where no go-specific type matches the returned values, such as when you query for a node, relationship, or path.  The driver exposes specific structs which represent this data in the 'structures.graph' package. There are 4 types - Node, Relationship, UnboundRelationship, and Path.  The driver returns interface{} objects which must have their types properly asserted to get the data out.

There are some limitations to the types of collections the driver
supports.  Specifically, maps should always be of type map[string]interface{} and lists should always be of type []interface{}.  It doesn't seem that the Bolt protocol supports
uint64 either, so the biggest number it can send right now is
the int64 max.
*/
package golangNeo4jBoltDriver
