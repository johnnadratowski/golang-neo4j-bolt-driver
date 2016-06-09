package structures

// InitMessage Represents an INIT message
type InitMessage struct {
	clientName string
	authToken  map[string]interface{}
}

// NewInitMessage Gets a new InitMessage struct
func NewInitMessage(clientName string, authToken map[string]interface{}) InitMessage {
	return InitMessage{
		clientName: clientName,
		authToken:  authToken,
	}
}

func (i InitMessage) Signature() int {
	return 0x01
}

func (i InitMessage) Fields() []interface{} {
	return []interface{}{i.clientName, i.authToken}
}
