package messages

const (
	// IgnoredMessageSignature is the signature byte for the IGNORED message
	IgnoredMessageSignature = 0x7E
)

// IgnoredMessage Represents an IGNORED message
type IgnoredMessage struct {
	metadata map[string]interface{}
}

// NewIgnoredMessage Gets a new IgnoredMessage struct
func NewIgnoredMessage(metadata map[string]interface{}) IgnoredMessage {
	return IgnoredMessage{
		metadata: metadata,
	}
}

// Signature gets the signature byte for the struct
func (i IgnoredMessage) Signature() int {
	return IgnoredMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i IgnoredMessage) AllFields() []interface{} {
	return []interface{}{i.metadata}
}
