package remote

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

var (
	errAddressNotSpecified  = errors.New("remote: address of connection pool is not specified")
	errFreeConnNotAvailable = errors.New("remote: no more free conn available")
	errGetConnTimeout       = errors.New("remote: get connection timeout, no more connection available in connection pool")
)

type poolOptions struct {
	addr        string
	timeout     time.Duration
	maxPoolSize uint32
	minPoolSize uint32
	maxIdleTime time.Duration
}

func defaultPoolOptions() poolOptions {
	return poolOptions{
		addr:        "",
		timeout:     1 * time.Second,
		minPoolSize: 0,
		maxPoolSize: 20,
		maxIdleTime: 5 * time.Minute,
	}
}

type availableConnResponse struct {
	conn *ConnWrapper
	err  error
}

type ConnWrapper struct {
	net.Conn
	inUse         bool
	createAt      time.Time
	idleStartTime time.Time
}

type ConnPoolBuilder interface {
	SetAddress(addr string) ConnPoolBuilder
	SetMaxPoolSize(size uint32) ConnPoolBuilder
	SetMinPoolSize(size uint32) ConnPoolBuilder
	SetMaxIdleTime(d time.Duration) ConnPoolBuilder
	SetTimeout(timeout time.Duration) ConnPoolBuilder
	Build() (ConnPool, error)
}

type connPoolBuilder struct {
	options poolOptions
}

func (b *connPoolBuilder) SetAddress(addr string) ConnPoolBuilder {
	b.options.addr = addr
	return b
}

func (b *connPoolBuilder) SetTimeout(d time.Duration) ConnPoolBuilder {
	b.options.maxIdleTime = d
	return b
}

func (b *connPoolBuilder) SetMaxPoolSize(size uint32) ConnPoolBuilder {
	b.options.maxPoolSize = size
	return b
}

func (b *connPoolBuilder) SetMinPoolSize(size uint32) ConnPoolBuilder {
	b.options.minPoolSize = size
	return b
}

func (b *connPoolBuilder) SetMaxIdleTime(d time.Duration) ConnPoolBuilder {
	b.options.maxIdleTime = d
	return b
}

func (b *connPoolBuilder) Build() (ConnPool, error) {
	// TODO: move validation into a new method
	if b.options.addr == "" {
		return nil, errAddressNotSpecified
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &connPool{
		options:    b.options,
		connLocker: sync.Mutex{},
		freeConn:   make([]*ConnWrapper, 0),
		numOpen:    0,
		stop:       cancel,

		openCh:                  make(chan struct{}, b.options.maxPoolSize),
		availableConnRequestCh:  make(chan struct{}, b.options.maxPoolSize),
		availableConnResponseCh: make(chan *availableConnResponse),
	}

	go p.connOpener(ctx)
	go p.idleConnCleaner(ctx)
	return p, nil
}

func NewConnPoolBuilder() ConnPoolBuilder {
	return &connPoolBuilder{
		options: defaultPoolOptions(),
	}
}

type ConnPool interface {
	Get(ctx context.Context) (*ConnWrapper, error)
	Return(conn *ConnWrapper, err error)
	Close()
}

type connPool struct {
	options poolOptions

	connLocker sync.Mutex
	freeConn   []*ConnWrapper // FILO
	numOpen    uint32

	openCh                  chan struct{}
	availableConnRequestCh  chan struct{}
	availableConnResponseCh chan *availableConnResponse

	stop context.CancelFunc
}

func (p *connPool) Get(ctx context.Context) (*ConnWrapper, error) {
	p.connLocker.Lock()
	conn, err := p.popFreeConn()
	if err == nil {
		return conn, nil
	}
	p.connLocker.Unlock()

	if conn != nil {
		return conn, nil
	}
	p.requestAvailableConn()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(p.options.timeout):
		return nil, errGetConnTimeout
	case availableRsp := <-p.availableConnResponseCh:
		return availableRsp.conn, availableRsp.err
	}
}

// Return a connection to ConnPool.
// err is optionally the last error that occurred on this connection.
func (p *connPool) Return(conn *ConnWrapper, err error) {
	if conn == nil {
		return
	}

	p.connLocker.Lock()
	if err != nil {
		p.numOpen--
		p.connLocker.Unlock()
		return
	}

	select {
	case <-p.availableConnRequestCh:
		p.connLocker.Unlock()
		p.availableConnResponseCh <- &availableConnResponse{conn: conn, err: nil}
	default:
		p.pushFreeConn(conn)
		p.connLocker.Unlock()
	}
}

func (p *connPool) Close() {
	// All connections are associated to p.stop by the context in p.Build
	p.stop()
}

// Removes the last connection from the freeConn and returns it.
func (p *connPool) popFreeConn() (*ConnWrapper, error) {
	numFree := len(p.freeConn)
	if numFree == 0 {
		return nil, errFreeConnNotAvailable
	}
	conn := p.freeConn[numFree-1]
	p.freeConn[numFree-1] = nil
	conn.inUse = true
	return conn, nil
}

// Appends a new connection to the end of the freeConn.
func (p *connPool) pushFreeConn(conn *ConnWrapper) {
	conn.inUse = false
	conn.idleStartTime = time.Now()
	p.freeConn = append(p.freeConn, conn)
}

func (p *connPool) requestAvailableConn() {
	p.connLocker.Lock()
	numCanOpen := p.options.maxPoolSize - p.numOpen
	if numCanOpen > 0 {
		p.numOpen++
		p.connLocker.Unlock()
		p.openCh <- struct{}{}
	} else {
		p.connLocker.Unlock()
		p.availableConnRequestCh <- struct{}{}
		return
	}

}

func (p *connPool) connOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.openCh:
			var d net.Dialer
			var connWrapper *ConnWrapper
			conn, err := d.DialContext(ctx, "tcp", p.options.addr)
			if err != nil {
				p.connLocker.Lock()
				p.numOpen--
				p.connLocker.Unlock()
			} else {
				connWrapper = &ConnWrapper{
					Conn:          conn,
					createAt:      time.Now(),
					idleStartTime: time.Now(),
					inUse:         true,
				}
			}
			p.availableConnResponseCh <- &availableConnResponse{conn: connWrapper, err: err}
		}
	}
}

func (p *connPool) idleConnCleaner(ctx context.Context) {
	ticker := time.NewTicker(p.options.maxIdleTime / 10)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.connLocker.Lock()
			numFree := len(p.freeConn)
			if numFree == 0 {
				p.connLocker.Unlock()
				continue
			}
			now := time.Now()
			lastExpiredIdx := -1
			for i, conn := range p.freeConn {
				expired := now.Sub(conn.idleStartTime) > p.options.maxIdleTime
				if !expired {
					break
				}
				lastExpiredIdx = i
			}
			if lastExpiredIdx < 0 {
				continue
			}

			p.freeConn = p.freeConn[lastExpiredIdx+1:]
		}
	}
}
