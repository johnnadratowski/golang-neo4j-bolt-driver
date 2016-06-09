package structures

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
func NewInitMessage(clientName string, authToken map[string]interface{}) InitMessage {
	return InitMessage{
		clientName: clientName,
		authToken:  authToken,
	}
}

// Signature gets the signature byte for the struct
func (i InitMessage) Signature() int {
	return InitMessageSignature
}

// Fields gets the fields to encode for the struct
func (i InitMessage) Fields() []interface{} {
	return []interface{}{i.clientName, i.authToken}
}

