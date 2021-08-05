package remoting

import (
	"log"
	"net"
	"starshipmq/remoting/protocol"
)

type RemotingServer struct {
	config   *RemotingServerConfig
	handler  func(*protocol.RemotingCommand) error
	listener net.Listener
}

type RemotingServerConfig struct {
	Address string
}

func (server *RemotingServer) Handle(handler func(*protocol.RemotingCommand) error) {
	server.handler = handler
}

func (server *RemotingServer) Run() {
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		log.Fatal(err)
	}
	server.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println(err)
				continue
			}
			cmd := protocol.Decode(conn)
			err = server.handler(cmd)
			if err != nil {
				log.Println(err)
			}
		}
	}()
}

func (server *RemotingServer) Shutdown() {
	err := server.listener.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func NewRemotingServer(config *RemotingServerConfig) *RemotingServer {
	return &RemotingServer{config: config}
}
