package remote

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

type poolOption struct {
	addr        string
	timeout     time.Duration
	maxPoolSize int
	minPoolSize int
	maxIdleTime time.Duration
}

func defaultPoolOption() poolOption {
	return poolOption{
		addr:        "",
		timeout:     1 * time.Second,
		minPoolSize: 0,
		maxPoolSize: 20,
		maxIdleTime: 5 * time.Minute,
	}
}

type availableConn struct {
	conn *ConnWrapper
	err  error
}

type ConnWrapper struct {
	net.Conn
	inUse         bool
	createAt      time.Time
	idleStartTime time.Time
}

type ConnPool interface {
	Get(ctx context.Context) (*ConnWrapper, error)
	Return(conn *ConnWrapper, err error)
	Close()
	SetAddress(addr string) ConnPool
	SetMaxPoolSize(size int) ConnPool
	SetMinPoolSize(size int) ConnPool
	SetMaxIdleTime(d time.Duration) ConnPool
	SetTimeout(timeout time.Duration) ConnPool
	Build() ConnPool
}

type connPool struct {
	option     poolOption
	connLocker sync.Mutex
	freeConn   []*ConnWrapper // FILO
	numOpen    int
	stop       context.CancelFunc

	openCh             chan struct{}
	availableRequestCh chan struct{}
	availableCh        chan *availableConn
}

func NewConnPool() ConnPool {
	return &connPool{
		option:     defaultPoolOption(),
		connLocker: sync.Mutex{},
		freeConn:   make([]*ConnWrapper, 0), // TODO: initial size should be minPoolSize?
		numOpen:    0,
	}
}

func (p *connPool) SetAddress(addr string) ConnPool {
	p.option.addr = addr
	return p
}

func (p *connPool) SetTimeout(d time.Duration) ConnPool {
	p.option.maxIdleTime = d
	return p
}

func (p *connPool) SetMaxPoolSize(size int) ConnPool {
	p.option.maxPoolSize = size
	return p
}

func (p *connPool) SetMinPoolSize(size int) ConnPool {
	p.option.minPoolSize = size
	return p
}

func (p *connPool) SetMaxIdleTime(d time.Duration) ConnPool {
	p.option.maxIdleTime = d
	return p
}

func (p *connPool) Build() ConnPool {
	ctx, cancel := context.WithCancel(context.Background())
	p.stop = cancel
	if p.option.addr == "" {
		log.Panicln("build conn pool: address is not specified")
	}

	p.openCh = make(chan struct{}, p.option.maxPoolSize)
	p.availableRequestCh = make(chan struct{}, p.option.maxPoolSize)
	p.availableCh = make(chan *availableConn)

	go p.connOpener(ctx)
	go p.idleConnCleaner(ctx)
	return p
}

func (p *connPool) Get(ctx context.Context) (*ConnWrapper, error) {
	p.connLocker.Lock()
	conn := p.popFreeConn()
	p.connLocker.Unlock()

	if conn != nil {
		return conn, nil
	}
	p.requestAvailableConn()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(p.option.timeout):
		return nil, errors.New("remote: get connection timeout, no more connection available in connection pool")
	case available := <-p.availableCh:
		return available.conn, available.err
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
	case <-p.availableRequestCh:
		p.connLocker.Unlock()
		p.availableCh <- &availableConn{conn: conn, err: nil}
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
func (p *connPool) popFreeConn() *ConnWrapper {
	numFree := len(p.freeConn)
	if numFree == 0 {
		return nil
	}
	conn := p.freeConn[numFree-1]
	p.freeConn[numFree-1] = nil
	conn.inUse = true
	return conn
}

// Appends a new connection to the end of the freeConn.
func (p *connPool) pushFreeConn(conn *ConnWrapper) {
	conn.inUse = false
	conn.idleStartTime = time.Now()
	p.freeConn = append(p.freeConn, conn)
}

func (p *connPool) requestAvailableConn() {
	p.connLocker.Lock()
	numCanOpen := p.option.maxPoolSize - p.numOpen
	if numCanOpen > 0 {
		p.numOpen++
		p.connLocker.Unlock()
		p.openCh <- struct{}{}
	} else {
		p.connLocker.Unlock()
		p.availableRequestCh <- struct{}{}
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
			conn, err := d.DialContext(ctx, "tcp", p.option.addr)
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
			p.availableCh <- &availableConn{conn: connWrapper, err: err}
		}
	}
}

func (p *connPool) idleConnCleaner(ctx context.Context) {
	ticker := time.NewTicker(p.option.maxIdleTime / 10)
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
				expired := now.Sub(conn.idleStartTime) > p.option.maxIdleTime
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
