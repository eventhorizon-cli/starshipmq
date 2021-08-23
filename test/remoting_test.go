package test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"starshipmq/remote"
	"starshipmq/remote/protocol"
	"sync"
	"testing"
)

func TestRemotingServer(t *testing.T) {
	addr := "127.0.0.1:5001"
	assert := assert.New(t)
	wg := &sync.WaitGroup{}
	wg.Add(3)

	server := newRemotingServer(addr)
	server.Run()

	server.Handle(func(command *protocol.RemotingCommand) error {
		body := string(command.Body)
		assert.Equal("Hello World", body)
		wg.Done()
		return nil
	})

	client := newClientForServer()
	for i := 0; i < 3; i++ {
		go func() {
			err := client.Invoke(context.Background(), addr, &protocol.RemotingCommand{
				Body: []byte("Hello World"),
			})
			assert.Nil(err)
		}()

	}


	wg.Wait()
	client.Close()
	server.Shutdown()
}

func newClientForServer() remote.RemotingClient {
	return remote.NewRemotingClient()
}

func newRemotingServer(addr string) remote.RemotingServer {
	return remote.NewRemotingServer().SetAddress(addr)
}
