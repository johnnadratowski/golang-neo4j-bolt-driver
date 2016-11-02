package bolt

import "database/sql/driver"

type sqlConn struct {
	*conn
}

// Prepare
func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return nil, nil
	//return c.prepare(query)
}

func (c *sqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.conn.query(query, params)
}

// Exec executes a query that returns no rows.
func (c *sqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	stmt := &sqlStmt{&boltStmt{query: query, conn: c.conn}}
	defer stmt.Close()
	return stmt.Exec(args)
}
