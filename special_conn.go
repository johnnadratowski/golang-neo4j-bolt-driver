package bolt

import "database/sql/driver"

type boltConn struct {
	*conn
}

func (c *boltConn) Query(query string, params map[string]interface{}) (rows, error) {
	return c.conn.query(query, params)
}

func (c *boltConn) Exec(query string, args map[string]interface{}) (Result, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	stmt := &boltStmt{conn: c.conn, query: query}
	defer stmt.Close()
	return stmt.Exec(args)
}

type sqlConn struct {
	*conn
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.prepare(query)
	if err != nil {
		return nil, err
	}
	return &sqlStmt{boltStmt: stmt}, nil
}

func (c *sqlConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	params, err := driverArgsToMap(args)
	if err != nil {
		return nil, err
	}
	return c.conn.query(query, params)
}

func (c *sqlConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	stmt := &sqlStmt{boltStmt: &boltStmt{query: query, conn: c.conn}}
	defer stmt.Close()
	return stmt.Exec(args)
}
