package messages

const (
	// IgnoredMessageSignature is the signature byte for the IGNORED message
	IgnoredMessageSignature = 0x7E
)

// IgnoredMessage Represents an IGNORED message
type IgnoredMessage struct{}

// NewIgnoredMessage Gets a new IgnoredMessage struct
func NewIgnoredMessage() IgnoredMessage {
	return IgnoredMessage{}
}

// Signature gets the signature byte for the struct
func (i IgnoredMessage) Signature() int {
	return IgnoredMessageSignature
}

// AllFields gets the fields to encode for the struct
func (i IgnoredMessage) AllFields() []interface{} {
	return []interface{}{}
}
