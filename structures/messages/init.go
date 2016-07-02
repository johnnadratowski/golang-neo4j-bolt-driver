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
	var authToken map[string]interface{}
	if user == "" {
		authToken = map[string]interface{}{
			"scheme": "none",
		}
	} else {
		authToken = map[string]interface{}{
			"scheme":      "basic",
			"principal":   user,
			"credentials": password,
		}
	}

	return InitMessage{
		clientName: clientName,
		authToken:  authToken,
	}
}

// Signature gets the signature byte for the struct
func (i InitMessage) Signature() int {
	return InitMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i InitMessage) AllFields() []interface{} {
	return []interface{}{i.clientName, i.authToken}
}
