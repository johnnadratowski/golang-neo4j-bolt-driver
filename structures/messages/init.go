package messages

const (
	// InitMessageSignature is the signature byte for the INIT message
	InitMessageSignature = 0x01
)

// InitMessage Represents an INIT message
type InitMessage struct {
	clientName string
	authToken  map[string]interface{}
}

// NewInitMessage Gets a new InitMessage struct
func NewInitMessage(clientName string, user string, password string) InitMessage {
	msg := InitMessage{clientName: clientName}
	if user == "" {
		msg.authToken = map[string]interface{}{"scheme": "none"}
	} else {
		msg.authToken = map[string]interface{}{
			"scheme":      "basic",
			"principal":   user,
			"credentials": password,
		}
	}
	return msg
}

// Signature gets the signature byte for the struct
func (i InitMessage) Signature() int {
	return InitMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i InitMessage) AllFields() []interface{} {
	return []interface{}{i.clientName, i.authToken}
}
