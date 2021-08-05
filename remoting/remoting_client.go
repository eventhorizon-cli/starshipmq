package remoting

import (
	"net"
	"starshipmq/remoting/protocol"
)

type RemotingClient struct {
	address string
	conn    net.Conn
}

func (client *RemotingClient) SendCommand(command *protocol.RemotingCommand) error {
	conn, err := net.Dial("tcp", client.address)
	client.conn = conn

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

func (client *RemotingClient) Close() {
	err := client.conn.Close()
	if err != nil {
		return
	}
}

func NewRemotingClient(address string) *RemotingClient {
	return &RemotingClient{
		address: address,
	}
}
