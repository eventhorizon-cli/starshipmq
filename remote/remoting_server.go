package remote

import (
	"context"
	"log"
	"net"
	"starshipmq/remote/protocol"
)

type RemotingServer interface {
	Handle(handler func(*protocol.RemotingCommand) error)
	Run()
	Shutdown()
	SetAddress(addr string) RemotingServer
}

type serverOption struct {
	address string
}

type remotingServer struct {
	option   serverOption
	handler  func(*protocol.RemotingCommand) error
	listener net.Listener
	stop     context.CancelFunc
}

func (s *remotingServer) SetAddress(addr string) RemotingServer {
	s.option.address = addr
	return s
}

func (s *remotingServer) Handle(handler func(*protocol.RemotingCommand) error) {
	s.handler = handler
}

func (s *remotingServer) Run() {
	ctx, cancel := context.WithCancel(context.Background())
	s.stop = cancel

	listener, err := net.Listen("tcp", s.option.address)
	if err != nil {
		log.Fatal(err)
	}
	s.listener = listener

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				s.listen()
			}
		}
	}()
}

func (s *remotingServer) Shutdown() {
	s.stop()
	err := s.listener.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func (s *remotingServer) listen() {
	conn, err := s.listener.Accept()
	if err != nil {
		log.Println(err)
		return
	}

	s.dispatch(conn)
}

func (s *remotingServer) dispatch(conn net.Conn) {
	cmd, err := protocol.Decode(conn)
	if err != nil {
		log.Println(err)
	}

	err = s.handler(cmd)
	if err != nil {
		log.Println(err)
	}
}

func NewRemotingServer() RemotingServer {
	return &remotingServer{}
}
