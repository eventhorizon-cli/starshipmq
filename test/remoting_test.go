package test

import (
	"github.com/stretchr/testify/assert"
	"starshipmq/remoting"
	"starshipmq/remoting/protocol"
	"sync"
	"testing"
)

func TestRemotingServer(t *testing.T) {
	assert := assert.New(t)

	server := newRemotingServer()
	server.Run()
	defer server.Shutdown()

	wg := &sync.WaitGroup{}
	server.Handle(func(command *protocol.RemotingCommand) error {
		body := string(command.Body)
		assert.Equal(body, "Hello World")
		wg.Done()
		return nil
	})

	client := newClientForServer()
	defer client.Close()

	for i := 0; i < 3; i++ {
		err := client.SendCommand(&protocol.RemotingCommand{
			Body: []byte("Hello World"),
		})
		wg.Add(1)
		assert.Nil(err)
	}
	wg.Wait()
}

func newClientForServer() *remoting.RemotingClient {
	return remoting.NewRemotingClient("127.0.0.1:5000")
}

func newRemotingServer() *remoting.RemotingServer {
	server := remoting.NewRemotingServer(&remoting.RemotingServerConfig{Address: "127.0.0.1:5000"})

	return server
}
