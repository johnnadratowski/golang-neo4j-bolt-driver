package golangNeo4jBoltDriver

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	magicPreamble     = []byte{0x60, 0x60, 0xb0, 0x17}
	supportedVersions = []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	handShake          = append(magicPreamble, supportedVersions...)
	noVersionSupported = [4]byte{0x00, 0x00, 0x00, 0x00}
)

const (
	// Version is the current version of this driver
	Version = "1.0"

	// ClientID is the id of this client
	ClientID = "GolangNeo4jBolt/" + Version
)

// Open calls DialOpen with the default dialer.
func Open(name string) (driver.Conn, error) {
	if os.Getenv(TLSEnv) == "1" {
		drv, err := TLSDialer("", "", "", false)
		if err != nil {
			return nil, err
		}
		return DialOpen(drv, name)
	}
	return DialOpen(&dialer{}, name)
}

// OpenNeo calls DialOpenNeo with the default dialer.
func OpenNeo(name string) (Conn, error) {
	if os.Getenv(TLSEnv) == "1" {
		drv, err := TLSDialer("", "", "", false)
		if err != nil {
			return nil, err
		}
		return DialOpenNeo(drv, name)
	}
	return DialOpenNeo(&dialer{}, name)
}

const (
	// HostEnv is the environment variable read to gather the host information.
	HostEnv = "BOLT_DRIVER_HOST"

	// PortEnv is the environment variable read to gather the port information.
	PortEnv = "BOLT_DRIVER_PORT"

	// UserEnv is the environment variable read to gather the username.
	UserEnv = "BOLT_DRIVER_USER"

	// PassEnv is the environment variable read to gather the password.
	PassEnv = "BOLT_DRIVER_PASS"

	// TLSEnv is the environment variable read to determine whether the Open
	// and OpenNeo methods should attempt to connect with TLS.
	TLSEnv = "BOLT_DRIVER_TLS"

	// TLSNoVerifyEnv is the environment variable read to determine whether
	// the TLS certificate's verification should be skipped.
	TLSNoVerifyEnv = "BOLT_DRIVER_NO_VERIFY"

	// TLSCACertFileEnv is the environment variable read that should contain
	// the CA certificate's path.
	TLSCACertFileEnv = "BOLT_DRIVER_TLS_CA_CERT_FILE"

	// TLSCertFileEnv is the environment variable read that should contain the
	// public key path.
	TLSCertFileEnv = "BOLT_TLS_CERT_FILE"

	// TLSKeyFileEnv is the environment variable read that should contain the
	// private key path.
	TLSKeyFileEnv = "BOLT_TLS_KEY_FILE"
)

func parseEnv() values {
	v := make(values)
	for _, val := range os.Environ() {
		parts := strings.SplitN(val, "=", 2)
		switch p := parts[1]; parts[0] {
		case HostEnv:
			v.set("host", p)
		case PortEnv:
			v.set("port", p)
		case UserEnv:
			v.set("user", p)
		case PassEnv:
			v.set("password", p)
		}
	}
	return v
}

func parseURL(v values, name string) error {
	url, err := url.Parse(name)
	if err != nil {
		return err
	}
	if url.User != nil {
		pw, ok := url.User.Password()
		if !ok {
			return errors.New("if username is provided a password is required")
		}
		v.set("password", pw)
		v.set("username", url.User.Username())
	}
	m := url.Query()
	set := func(key string) {
		v.set(key, m.Get(key))
	}
	set("timeout")
	set("tls")
	set("tls_ca_cert_file")
	set("tls_cert_file")
	set("tls_key_file")
	set("tls_no_verify")
	return nil
}

// TLSDialer returns a Dialer that is compatible with Neo4j. It can be passed
// to DialOpen and DialOpenNeo. It reads configuration information from
// environment variables, although the function parameters take precedence.
// noVerify will only be read from an environment variable if noVerify is false.
func TLSDialer(caFile, certFile, keyFile string, noVerify bool) (Dialer, error) {
	if caFile == "" {
		caFile = os.Getenv(TLSCACertFileEnv)
	}
	if certFile == "" {
		certFile = os.Getenv(TLSCertFileEnv)
	}
	if keyFile == "" {
		keyFile = os.Getenv(TLSKeyFileEnv)
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS12}

	if caFile != "" {
		cert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cert) {
			return nil, errors.New("could not append CA certificate")
		}
		cfg.RootCAs = pool
	}

	if certFile != "" {
		if keyFile == "" {
			return nil, errors.New("cert file requires a key file")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	if !noVerify {
		noVerify = os.Getenv(TLSNoVerifyEnv) == "1"
	}
	cfg.InsecureSkipVerify = noVerify
	return &dialer{cfg: cfg}, nil
}

// DialOpen opens a driver.Conn with the given Dialer and network configuration.
func DialOpen(d Dialer, name string) (driver.Conn, error) {
	return open(d, name)
}

func open(d Dialer, name string) (*boltConn, error) {
	// Default Neo4j configuration information.
	v := values{"host": "localhost", "port": "7474"}

	// Parse environment variables if applicable. These will be overwritten by
	// the URI configuration if it exists.
	v.merge(parseEnv())

	// Parse our values from the URL if applicable.
	if strings.HasPrefix(name, "bolt://") {
		err := parseURL(v, name)
		if err != nil {
			return nil, err
		}
	}

	conn, err := dial(d, v)
	if err != nil {
		return nil, err
	}

	var timeout int64
	if tos := v.get("timeout"); tos != "" && tos != "0" {
		timeout, err = strconv.ParseInt(v.get("timeout"), 0, 0)
		if err != nil {
			return nil, err
		}
	}
	return newBoltConn(
		conn, time.Duration(timeout)*time.Second,
		v.get("username"), v.get("password"),
	)
}

func dial(d Dialer, v values) (net.Conn, error) {
	addr := net.JoinHostPort(v.get("host"), v.get("port"))
	dto := v.get("dial_timeout")
	if dto != "" && dto != "0" {
		sec, err := strconv.ParseInt(dto, 0, 0)
		if err != nil {
			return nil, nil
		}
		return d.DialTimeout("tcp", addr, time.Duration(sec)*time.Second)
		if err != nil {
			return nil, err
		}
	}
	return d.Dial("tcp", addr)
}

// DialOpenNeo opens an Conn with the given Dialer and network configuration.
func DialOpenNeo(d Dialer, name string) (Conn, error) {
	return open(d, name)
}

// Dialer is a generic interface for types that can dial network addresses.
type Dialer interface {
	// Dial connects to the address on the named network.
	Dial(network, address string) (net.Conn, error)

	// DialTimeout acts like Dial but takes a timeout. The timeout should
	// included name resolution, if required.
	DialTimeout(network, address string, timeout time.Duration) (net.Conn, error)
}

// dialer is the default Dialer. It'll use TLS if its cfg member is set,
// typically through calling TLSDialer.
type dialer struct {
	cfg *tls.Config
}

// Dial implements Dialer.
func (d *dialer) Dial(network, addr string) (net.Conn, error) {
	if d.cfg != nil {
		return tls.Dial(network, addr, d.cfg)
	}
	return net.Dial(network, addr)
}

// Dial implements Dialer.
func (d *dialer) DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error) {
	if d.cfg != nil {
		return tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, addr, d.cfg)
	}
	return net.DialTimeout(network, addr, timeout)
}

type boltDriver struct{}

// Open opens a new Bolt connection to the Neo4J database
func (d *boltDriver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

// DriverPool is a driver allowing connection to Neo4j with support for
// connection pooling. The driver allows you to open a new connection to Neo4j.
//
// Driver objects should be THREAD SAFE, so you can use them
// to open connections in multiple threads. The connection objects
// themselves, and any prepared statements/transactions within ARE NOT
// THREAD SAFE.
type DriverPool interface {
	// OpenPool opens a Neo-specific connection.
	OpenPool() (Conn, error)

	reclaim(*boltConn)
}

type boltDriverPool struct {
	connStr  string
	maxConns int
	pool     chan *boltConn
}

// NewDriverPool creates a new Driver object with connection pooling
func NewDriverPool(connStr string, max int) (DriverPool, error) {
	d := &boltDriverPool{
		connStr:  connStr,
		maxConns: max,
		pool:     make(chan *boltConn, max),
	}
	for i := 0; i < max; i++ {
		d.pool <- &boltConn{pool: d}
	}
	return d, nil
}

// OpenNeo opens a new Bolt connection to the Neo4J database.
func (d *boltDriverPool) OpenPool() (Conn, error) {
	conn := <-d.pool
	return conn, nil
}

func (d *boltDriverPool) reclaim(conn *boltConn) {
	// sneakily swap out connection so a reference to
	// it isn't held on to
	newConn := &boltConn{}
	*newConn = *conn
	d.pool <- newConn
	conn = nil
}

type values map[string]string

func (v values) set(k, vv string) {
	v[k] = vv
}

func (v values) get(k string) string {
	return v[k]
}

func (v values) has(k string) bool {
	_, ok := v[k]
	return ok
}

// merge adds v2 to v, overwriting any new entries.
func (v values) merge(v2 values) {
	for k, vv := range v2 {
		v.set(k, vv)
	}
}

func init() {
	sql.Register("neo4j-bolt", &boltDriver{})
	sql.Register("neo4j-bolt-recorder", &recorder{})
}
