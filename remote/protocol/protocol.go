package protocol

import (
	"encoding/binary"
	"net"
)

const (
	headerMark = 0xAA
)

type RemotingCommand struct {
	Body []byte
}

func EncodeHeader(command *RemotingCommand) []byte {
	bodyLen := len(command.Body)
	return []byte{headerMark, byte(bodyLen >> 8), byte(bodyLen)}
}

func Decode(conn net.Conn) (*RemotingCommand, error) {
	header := make([]byte, 3)
	_, err := conn.Read(header)
	if err != nil {
		return nil, err
	}
	bodyLen := binary.BigEndian.Uint16(header[1:3])
	body := make([]byte, bodyLen)
	_, err = conn.Read(body)
	if err != nil {
		return nil, err
	}
	return &RemotingCommand{
		Body: body,
	}, nil
}
