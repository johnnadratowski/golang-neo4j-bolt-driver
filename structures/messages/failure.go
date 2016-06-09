package messages

const (
	// FailureMessageSignature is the signature byte for the FAILURE message
	FailureMessageSignature = 0x7F
)

// FailureMessage Represents an FAILURE message
type FailureMessage struct {
	metadata map[string]interface{}
}

// NewFailureMessage Gets a new FailureMessage struct
func NewFailureMessage(metadata map[string]interface{}) FailureMessage {
	return FailureMessage{
		metadata: metadata,
	}
}

// Signature gets the signature byte for the struct
func (i FailureMessage) Signature() int {
	return FailureMessageSignature
}

// Fields gets the fields to encode for the struct
func (i FailureMessage) Fields() []interface{} {
	return []interface{}{i.metadata}
}
