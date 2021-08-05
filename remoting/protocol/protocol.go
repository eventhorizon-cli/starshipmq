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

func Decode(conn net.Conn) *RemotingCommand {
	header := make([]byte, 3)
	conn.Read(header)
	bodyLen := binary.BigEndian.Uint16(header[1:3])
	body := make([]byte, bodyLen)
	conn.Read(body)
	return &RemotingCommand{
		Body: body,
	}
}
