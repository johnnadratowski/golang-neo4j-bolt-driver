package messages

const (
	// FailureMessageSignature is the signature byte for the FAILURE message
	FailureMessageSignature = 0x7F
)

// FailureMessage Represents an FAILURE message
type FailureMessage struct {
	Metadata map[string]interface{}
}

// NewFailureMessage Gets a new FailureMessage struct
func NewFailureMessage(metadata map[string]interface{}) FailureMessage {
	return FailureMessage{
		Metadata: metadata,
	}
}

// Signature gets the signature byte for the struct
func (i FailureMessage) Signature() int {
	return FailureMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i FailureMessage) AllFields() []interface{} {
	return []interface{}{i.Metadata}
}
