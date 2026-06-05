package adapterhost

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// RemoteIdentity is the adapter identity sent during the remote handshake.
type RemoteIdentity struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Digest  string `json:"digest"`
}

// ServeRemoteOptions configures a phone-home adapter connection.
type ServeRemoteOptions struct {
	// Host is the criteria host address to dial (e.g. "host.example.com:7778").
	// Unix socket paths are also supported for local testing.
	Host string

	// TLSConfig is optional; when nil the connection is plain TCP or Unix.
	TLSConfig *tls.Config

	// Identity is sent in the v2 handshake line before gRPC frames flow.
	Identity RemoteIdentity

	// AcceptToken is an optional pre-shared token the host may require.
	AcceptToken string
}

// ServeRemote dials the criteria host, sends the v2 identity handshake, and
// serves the adapter gRPC contract on the held connection. It blocks until the
// connection closes or a fatal error occurs.
//
// This is the remote counterpart to [Serve]; callers should invoke exactly one
// of Serve or ServeRemote from main().
func ServeRemote(impl Service, opts *ServeRemoteOptions) error {
	if opts == nil {
		return errors.New("ServeRemote: opts is required")
	}
	if opts.Host == "" {
		return errors.New("ServeRemote: Host is required")
	}

	conn, err := dialRemote(opts.Host, opts.TLSConfig)
	if err != nil {
		return fmt.Errorf("ServeRemote: dial %s: %w", opts.Host, err)
	}

	if err := sendHandshake(conn, opts.Identity, opts.AcceptToken); err != nil {
		_ = conn.Close()
		return fmt.Errorf("ServeRemote: handshake: %w", err)
	}

	server := grpc.NewServer()
	v2.RegisterAdapterServiceServer(server, &grpcAdapterServer{impl: impl})

	wrapped := &closeSignalConn{Conn: conn, doneCh: make(chan struct{})}
	lis := newSingleConnListener(wrapped)

	// When the underlying connection is closed (by the peer or by the gRPC
	// transport), close the listener so grpc.Server.Serve unblocks and returns.
	go func() {
		<-wrapped.doneCh
		_ = lis.Close()
	}()

	return server.Serve(lis)
}

// dialRemote opens a TCP or Unix connection to the host. TLS is applied only
// for TCP addresses when a config is provided.
func dialRemote(host string, tlsConfig *tls.Config) (net.Conn, error) {
	network := "tcp"
	if filepath.IsAbs(host) || (host != "" && host[0] == '/') {
		network = "unix"
	}

	if tlsConfig != nil && network == "tcp" {
		return tls.Dial("tcp", host, tlsConfig)
	}
	return net.Dial(network, host)
}

// remoteHandshake is the JSON line sent immediately after the transport
// connection is established. The host shim (WS20) reads it before allowing
// gRPC frames to flow.
type remoteHandshake struct {
	Name               string `json:"name"`
	Version            string `json:"version"`
	Digest             string `json:"digest"`
	Token              string `json:"token,omitempty"`
	SDKProtocolVersion int    `json:"sdk_protocol_version"`
}

// sendHandshake writes the identity handshake line to conn.
func sendHandshake(conn net.Conn, identity RemoteIdentity, token string) error {
	h := remoteHandshake{
		Name:               identity.Name,
		Version:            identity.Version,
		Digest:             identity.Digest,
		Token:              token,
		SDKProtocolVersion: 2,
	}
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

// closeSignalConn wraps a net.Conn and signals via doneCh the first time
// Close() is called. This lets ServeRemote detect when the peer has closed
// the connection so it can unblock grpc.Server.Serve.
type closeSignalConn struct {
	net.Conn
	once   sync.Once
	doneCh chan struct{}
}

func (c *closeSignalConn) Close() error {
	c.once.Do(func() { close(c.doneCh) })
	return c.Conn.Close()
}

// singleConnListener is a net.Listener that returns a pre-opened connection
// on its first Accept() and then blocks until Close() is called. It lets a
// grpc.Server serve on a connection that was dialed outbound (the phone-home
// model).
type singleConnListener struct {
	conn   net.Conn
	mu     sync.Mutex
	used   bool
	closed chan struct{}
}

func newSingleConnListener(conn net.Conn) *singleConnListener {
	return &singleConnListener{
		conn:   conn,
		closed: make(chan struct{}),
	}
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if !l.used {
		l.used = true
		l.mu.Unlock()
		return l.conn, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, errors.New("listener closed")
}

func (l *singleConnListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.closed:
		// already closed
	default:
		close(l.closed)
	}
	return nil
}

func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }
