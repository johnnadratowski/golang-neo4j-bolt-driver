package bolt

func (c *boltConn) Query(query string, params map[string]interface{}) (rows, error) {
	return c.conn.query(query, params)
}

// Exec executes a query that returns no rows. Implements a Neo-friendly alternative to sql/driver.
func (c *boltConn) Exec(query string, args map[string]interface{}) (Result, error) {
	if c.bad {
		return nil, ErrClosed
	}
	stmt := &boltStmt{conn: c.conn, query: query}
	defer stmt.Close()
	return stmt.Exec(args)
}
