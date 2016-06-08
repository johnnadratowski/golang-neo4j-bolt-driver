package messages

import "io"

// InitMessage Represents an INIT message
type InitMessage struct {
	clientName string
	authToken  map[string]interface{}
}

// InitMessage Gets a new InitMessage struct
func InitMessage(clientName string, authToken map[string]interface{}) InitMessage {
	return InitMessage{
		clientName: clientName,
		authToken:  authToken,
	}
}

func (i InitMessage) Signature() rune {
	return 0x01
}

func (i InitMessage) Marker() rune {
	return 0xB1
}

func (i InitMessage) Write(writer io.Writer) (int64, error) {
	return writer.Write(append([]byte{i.Marker(), i.Signature()}, EncodeMap(i.authToken)...))
}
