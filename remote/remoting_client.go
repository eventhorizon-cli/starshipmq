package remote

import (
	"context"
	"starshipmq/remote/protocol"
	"sync"
)

type RemotingClient interface {
	Invoke(ctx context.Context, addr string, command *protocol.RemotingCommand) error
	Close()
}

type remotingClient struct {
	connPoolLocker sync.RWMutex
	connPools      map[string]ConnPool
}

func (c *remotingClient) Invoke(ctx context.Context, addr string, command *protocol.RemotingCommand) error {
	connPool := c.getOrCreateConnPool(addr)

	conn, err := connPool.Get(ctx)
	defer connPool.Return(conn, err)

	if err != nil {
		return err
	}
	_, err = conn.Write(protocol.EncodeHeader(command))
	if err != nil {
		return err
	}
	_, err = conn.Write(command.Body)
	if err != nil {
		return err
	}
	return nil
}

func (c *remotingClient) Close() {
	for _, p := range c.connPools {
		p.Close()
	}
}

func (c *remotingClient) getOrCreateConnPool(addr string) ConnPool {
	c.connPoolLocker.RLock()
	pool, ok := c.connPools[addr]
	c.connPoolLocker.RUnlock()
	if ok {
		return pool
	}

	c.connPoolLocker.Lock()
	defer c.connPoolLocker.Unlock()
	// double check
	pool, ok = c.connPools[addr]
	if ok {
		return pool
	}

	pool = c.createConnPool(addr)
	c.connPools[addr] = pool

	return pool
}

func (c *remotingClient) createConnPool(addr string) ConnPool {
	return NewConnPool().
		SetAddress(addr).
		Build()
}

func NewRemotingClient() RemotingClient {
	return &remotingClient{
		connPools: make(map[string]ConnPool),
	}
}
