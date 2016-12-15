/*Package golangNeo4jBoltDriver implements a driver for the Neo4J Bolt Protocol.

The driver is compatible with Golang's sql.driver interface, but
aims to implement a more complete featureset in line with what
Neo4J and Bolt provides.

As such, there are multiple interfaces the user can choose from.
It's highly recommended that the user use the Neo4J-specific
interfaces as they are more flexible and efficient than the
provided sql.driver compatible methods.

The interface tries to be consistent throughout. The sql.driver
interfaces are standard, but the Neo4J-specific ones contain a
naming convention of either "Neo" or "Pipeline".

The "Neo" ones are the basic interfaces for making queries to
Neo4j and it's expected that these would be used the most.

The "Pipeline" ones are to support Bolt's pipelining features.
Pipelines allow the user to send Neo4j many queries at once and
have them executed by the database concurrently.  This is useful
if you have a bunch of queries that aren't necessarily dependant
on one another, and you want to get better performance.  The
internal APIs will also pipeline statements where it is able to
reliably do so, but by manually using the pipelining feature
you can maximize your throughput.

The API provides connection pooling using the `NewDriverPool` method.
This allows you to pass it the maximum number of open connections
to be used in the pool.  Once this limit is hit, any new clients will
have to wait for a connection to become available again.

The sql driver is registered as "neo4j-bolt". The sql.driver interface
is much more limited than what bolt and neo4j supports.  In some cases,
concessions were made in order to make that interface work with the
neo4j way of doing things.  The main instance of this is the marshalling
of objects to/from the sql.driver.Value interface.  In order to support
object types that aren't supported by this interface, the internal encoding
package is used to marshal these objects to byte strings. This ultimately
makes for a less efficient and more 'clunky' implementation.  A glaring
instance of this is passing parameters.  Neo4j expects named parameters
but the driver interface can only really support positional parameters.
To get around this, the user must create a map[string]interface{} of their
parameters and marshal it to a driver.Value using the encoding.Marshal
function. Similarly, the user must unmarshal data returned from the queries
using the encoding.Unmarshal function, then use type assertions to retrieve
the proper type.

In most cases the driver will return the data from neo as the proper
go-specific types.  For integers they always come back
as int64 and floats always come back as float64.  This is for the
convenience of the user and acts similarly to go's JSON interface.
This prevents the user from having to use reflection to get
these values.  Internally, the types are always transmitted over
the wire with as few bytes as possible.

There are also cases where no go-specific type matches the returned values,
such as when you query for a node, relationship, or path.  The driver
exposes specific structs which represent this data in the 'structures.graph'
package. There are 4 types - Node, Relationship, UnboundRelationship, and
Path.  The driver returns interface{} objects which must have their types
properly asserted to get the data out.

There are some limitations to the types of collections the driver
supports.  Specifically, maps should always be of type map[string]interface{}
and lists should always be of type []interface{}.  It doesn't seem that
the Bolt protocol supports uint64 either, so the biggest number it can send
right now is the int64 max.

The URL format is: `bolt://(user):(password)@(host):(port)`
Schema must be `bolt`. User and password is only necessary if you are authenticating.
TLS is supported by using query parameters on the connection string, like so:
`bolt://host:port?tls=true&tls_no_verify=false`

The supported query params are:

* timeout - the number of seconds to set the connection timeout to. Defaults to 60 seconds.
* tls - Set to 'true' or '1' if you want to use TLS encryption
* tls_no_verify - Set to 'true' or '1' if you want to accept any server certificate (for testing, not secure)
* tls_ca_cert_file - path to a custom ca cert for a self-signed TLS cert
* tls_cert_file - path to a cert file for this client (need to verify this is processed by Neo4j)
* tls_key_file - path to a key file for this client (need to verify this is processed by Neo4j)

Errors returned from the API support wrapping, so if you receive an error
from the library, it might be wrapping other errors.  You can get the innermost
error by using the `InnerMost` method.  Failure messages from Neo4J are reported,
along with their metadata, as an error.  In order to get the failure message metadata
from a wrapped error, you can do so by calling
`err.(*errors.Error).InnerMost().(messages.FailureMessage).Metadata`

If there is an error with the database connection, you should get a sql/driver ErrBadConn
as per the best practice recommendations of the Golang SQL Driver. However, this error
may be wrapped, so you might have to call `InnerMost` to get it, as specified above.
*/
package golangNeo4jBoltDriver
